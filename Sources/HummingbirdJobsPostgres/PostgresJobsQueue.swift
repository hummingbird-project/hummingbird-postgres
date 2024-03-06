import Foundation
import HummingbirdJobs
@_spi(ConnectionPool) import HummingbirdPostgres
import Logging
import NIOConcurrencyHelpers
import NIOCore
@_spi(ConnectionPool) import PostgresNIO

@_spi(ConnectionPool)
public final class HBPostgresQueue: HBJobQueueDriver {
    public typealias JobID = UUID

    /// what to do with failed/processing jobs from last time queue was handled
    public enum JobInitialization: Sendable {
        case doNothing
        case rerun
        case remove
    }

    /// Errors thrown by HBPostgresJobQueue
    public enum PostgresQueueError: Error, CustomStringConvertible {
        case failedToAdd

        public var description: String {
            switch self {
            case .failedToAdd:
                return "Failed to add job to queue"
            }
        }
    }

    /// Job Status
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
        let pollTime: Duration

        public init(
            jobTable: String = "_hb_jobs",
            jobQueueTable: String = "_hb_job_queue",
            pendingJobsInitialization: HBPostgresQueue.JobInitialization = .doNothing,
            failedJobsInitialization: HBPostgresQueue.JobInitialization = .rerun,
            processingJobsInitialization: HBPostgresQueue.JobInitialization = .rerun,
            pollTime: Duration = .milliseconds(100)
        ) {
            self.jobTable = jobTable
            self.jobQueueTable = jobQueueTable
            self.pendingJobsInitialization = pendingJobsInitialization
            self.failedJobsInitialization = failedJobsInitialization
            self.processingJobsInitialization = processingJobsInitialization
            self.pollTime = pollTime
        }
    }

    /// Postgres client used by Job queue
    public let client: PostgresClient
    /// Job queue configuration
    public let configuration: Configuration
    /// Logger used by queue
    public let logger: Logger
    let isStopped: NIOLockedValueBox<Bool>

    /// Initialize a HBPostgresJobQueue
    /// - Parameters:
    ///   - client: Postgres client
    ///   - configuration: Queue configuration
    ///   - logger: Logger used by queue
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
                        job bytea,
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
    @discardableResult public func push(_ buffer: ByteBuffer) async throws -> JobID {
        try await self.client.withConnection { connection in
            let queuedJob = HBQueuedJob<JobID>(id: .init(), jobBuffer: buffer)
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
                while true {
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
                    // return nil if nothing in queue
                    guard let jobId = try await stream.decode(UUID.self, context: .default).first(where: { _ in true }) else {
                        return nil
                    }
                    // select job from job table
                    let stream2 = try await connection.query(
                        "SELECT job FROM \(unescaped: self.configuration.jobTable) WHERE id = \(jobId)",
                        logger: self.logger
                    )

                    do {
                        try await self.setStatus(jobId: jobId, status: .processing, connection: connection)
                        // if failed to find a job in the job table try getting another index
                        guard let buffer = try await stream2.decode(ByteBuffer.self, context: .default).first(where: { _ in true }) else {
                            continue
                        }
                        return HBQueuedJob(id: jobId, jobBuffer: buffer)
                    } catch {
                        try await self.setStatus(jobId: jobId, status: .failed, connection: connection)
                        throw JobQueueError.decodeJobFailed
                    }
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
            VALUES (\(job.id), \(job.jobBuffer), \(Status.pending))
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
extension HBPostgresQueue {
    public struct AsyncIterator: AsyncIteratorProtocol {
        let queue: HBPostgresQueue

        public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.withLockedValue({ $0 }) {
                    return nil
                }
                if let job = try await queue.popFirst() {
                    return job
                }
                // we only sleep if we didn't receive a job
                try await Task.sleep(for: self.queue.configuration.pollTime)
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(queue: self)
    }
}

@_spi(ConnectionPool)
extension HBJobQueueDriver where Self == HBPostgresQueue {
    /// Return Postgres driver for Job Queue
    /// - Parameters:
    ///   - client: Postgres client
    ///   - configuration: Queue configuration
    ///   - logger: Logger used by queue
    public static func postgres(client: PostgresClient, configuration: HBPostgresQueue.Configuration = .init(), logger: Logger) -> Self {
        .init(client: client, configuration: configuration, logger: logger)
    }
}
