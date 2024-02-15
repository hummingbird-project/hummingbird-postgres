import Foundation
import HummingbirdJobs
@_spi(ConnectionPool) import HummingbirdPostgres
import Logging
import NIOConcurrencyHelpers
@_spi(ConnectionPool) import PostgresNIO

@_spi(ConnectionPool)
public final class HBPostgresJobQueue: HBJobQueue {
    public typealias JobID = UUID

    /// what to do with failed/processing jobs from last time queue was handled
    public enum JobInitialization: Sendable {
        case doNothing
        case rerun
        case remove
    }

    public enum PostgresQueueError: Error, CustomStringConvertible {
        case failedToAdd

        public var description: String {
            switch self {
            case .failedToAdd:
                return "Failed to add job to queue"
            }
        }
    }

    enum Status: Int16, PostgresCodable {
        case pending = 0
        case processing = 1
        case failed = 2
    }

    /// Queue configuration
    public struct Configuration: Sendable {
        let jobTable: String
        let jobQueueTable: String
        let pendingJobsInitialization: JobInitialization
        let failedJobsInitialization: JobInitialization
        let processingJobsInitialization: JobInitialization

        public init(
            jobTable: String = "_hb_jobs",
            jobQueueTable: String = "_hb_job_queue",
            pendingJobsInitialization: HBPostgresJobQueue.JobInitialization = .doNothing,
            failedJobsInitialization: HBPostgresJobQueue.JobInitialization = .rerun,
            processingJobsInitialization: HBPostgresJobQueue.JobInitialization = .rerun
        ) {
            self.jobTable = jobTable
            self.jobQueueTable = jobQueueTable
            self.pendingJobsInitialization = pendingJobsInitialization
            self.failedJobsInitialization = failedJobsInitialization
            self.processingJobsInitialization = processingJobsInitialization
        }
    }

    let client: PostgresClient
    let configuration: Configuration
    let logger: Logger
    let isStopped: NIOLockedValueBox<Bool>

    /// Initialize a HBPostgresJobQueue
    public init(client: PostgresClient, configuration: Configuration = .init(), logger: Logger) {
        self.client = client
        self.configuration = configuration
        self.logger = logger
        self.isStopped = .init(false)
    }

    /// Run on initialization of the job queue
    public func onInit() async throws {
        do {
            _ = try await self.client.withConnection { connection in
                try await connection.query(
                    """
                    CREATE TABLE IF NOT EXISTS \(unescaped: self.configuration.jobTable) (
                        id uuid PRIMARY KEY,
                        job json,
                        status smallint
                    )     
                    """,
                    logger: self.logger
                )
                try await connection.query(
                    """
                    CREATE TABLE IF NOT EXISTS \(unescaped: self.configuration.jobQueueTable) (
                        job_id uuid PRIMARY KEY,
                        createdAt timestamp with time zone
                    )
                    """,
                    logger: self.logger
                )
                try await connection.query(
                    """
                    CREATE INDEX IF NOT EXISTS \(unescaped: self.configuration.jobQueueTable)idx 
                    ON \(unescaped: self.configuration.jobQueueTable) (createdAt ASC)
                    """,
                    logger: self.logger
                )
                try await self.updateJobsOnInit(withStatus: .pending, onInit: self.configuration.pendingJobsInitialization, connection: connection)
                try await self.updateJobsOnInit(withStatus: .processing, onInit: self.configuration.processingJobsInitialization, connection: connection)
                try await self.updateJobsOnInit(withStatus: .failed, onInit: self.configuration.failedJobsInitialization, connection: connection)
            }
        } catch let error as PSQLError {
            print("\(String(reflecting: error))")
            throw error
        }
    }

    /// Push Job onto queue
    /// - Returns: Identifier of queued job
    @discardableResult public func push(_ job: HBJob) async throws -> JobID {
        try await self.client.withConnection { connection in
            let queuedJob = HBQueuedJob<JobID>(id: .init(), job: job)
            try await add(queuedJob, connection: connection)
            try await addToQueue(jobId: queuedJob.id, connection: connection)
            return queuedJob.id
        }
    }

    /// This is called to say job has finished processing and it can be deleted
    public func finished(jobId: JobID) async throws {
        _ = try await self.client.withConnection { connection in
            try await self.delete(jobId: jobId, connection: connection)
        }
    }

    /// This is called to say job has failed to run and should be put aside
    public func failed(jobId: JobID, error: Error) async throws {
        _ = try await self.client.withConnection { connection in
            try await self.setStatus(jobId: jobId, status: .failed, connection: connection)
        }
    }

    /// stop serving jobs
    public func stop() async {
        self.isStopped.withLockedValue { $0 = true }
    }

    /// shutdown queue once all active jobs have been processed
    public func shutdownGracefully() async {}

    func popFirst() async throws -> HBQueuedJob<JobID>? {
        do {
            return try await self.client.withConnection { connection -> HBQueuedJob? in
                try Task.checkCancellation()
                let stream = try await connection.query(
                    """
                    DELETE
                    FROM \(unescaped: self.configuration.jobQueueTable) pse
                    WHERE pse.job_id =
                          (SELECT pse_inner.job_id
                           FROM \(unescaped: self.configuration.jobQueueTable) pse_inner
                           ORDER BY pse_inner.createdAt ASC
                               FOR UPDATE SKIP LOCKED
                           LIMIT 1)
                    RETURNING pse.job_id
                    """,
                    logger: self.logger
                )
                guard let jobId = try await stream.decode(UUID.self, context: .default).first(where: { _ in true }) else {
                    return nil
                }
                let stream2 = try await connection.query(
                    "SELECT job FROM \(unescaped: self.configuration.jobTable) WHERE id = \(jobId)",
                    logger: self.logger
                )

                do {
                    try await self.setStatus(jobId: jobId, status: .processing, connection: connection)
                    guard let job = try await stream2.decode(HBAnyCodableJob.self, context: .default).first(where: { _ in true }) else {
                        return nil
                    }
                    return HBQueuedJob(id: jobId, job: job.job)
                } catch {
                    try await self.setStatus(jobId: jobId, status: .failed, connection: connection)
                    throw JobQueueError.decodeJobFailed
                }
            }
        } catch let error as PSQLError {
            print("\(String(reflecting: error))")
            throw error
        }
    }

    func add(_ job: HBQueuedJob<JobID>, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            INSERT INTO \(unescaped: self.configuration.jobTable) (id, job, status)
            VALUES (\(job.id), \(job.anyCodableJob), \(Status.pending))
            """,
            logger: self.logger
        )
    }

    func delete(jobId: JobID, connection: PostgresConnection) async throws {
        try await connection.query(
            "DELETE FROM \(unescaped: self.configuration.jobTable) WHERE id = \(jobId)",
            logger: self.logger
        )
    }

    func addToQueue(jobId: JobID, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            INSERT INTO \(unescaped: self.configuration.jobQueueTable) (job_id, createdAt) VALUES (\(jobId), \(Date.now))
            """,
            logger: self.logger
        )
    }

    func setStatus(jobId: JobID, status: Status, connection: PostgresConnection) async throws {
        try await connection.query(
            "UPDATE \(unescaped: self.configuration.jobTable) SET status = \(status) WHERE id = \(jobId)",
            logger: self.logger
        )
    }

    func getJobs(withStatus status: Status) async throws -> [JobID] {
        return try await self.client.withConnection { connection in
            let stream = try await connection.query(
                "SELECT id FROM \(unescaped: self.configuration.jobTable) WHERE status = \(status)",
                logger: self.logger
            )
            var jobs: [JobID] = []
            for try await id in stream.decode(JobID.self, context: .default) {
                jobs.append(id)
            }
            return jobs
        }
    }

    func updateJobsOnInit(withStatus status: Status, onInit: JobInitialization, connection: PostgresConnection) async throws {
        switch onInit {
        case .remove:
            try await connection.query(
                "DELETE FROM \(unescaped: self.configuration.jobTable) WHERE status = \(status)",
                logger: self.logger
            )
        case .rerun:
            guard status != .pending else { return }
            let jobs = try await getJobs(withStatus: status)
            for jobId in jobs {
                try await self.addToQueue(jobId: jobId, connection: connection)
            }
        case .doNothing:
            break
        }
    }
}

/// extend HBPostgresJobQueue to conform to AsyncSequence
extension HBPostgresJobQueue {
    public struct AsyncIterator: AsyncIteratorProtocol {
        let queue: HBPostgresJobQueue

        public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.withLockedValue({ $0 }) {
                    return nil
                }
                if let job = try await queue.popFirst() {
                    return job
                }
                // we only sleep if we didn't receive a job
                try await Task.sleep(for: .seconds(1))
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(queue: self)
    }
}

extension HBAnyCodableJob: PostgresCodable {}
