import Foundation
import HummingbirdJobs
@_spi(ConnectionPool) import HummingbirdPostgres
import Logging
import NIOConcurrencyHelpers
@_spi(ConnectionPool) import PostgresNIO

@_spi(ConnectionPool)
public final class HBPostgresJobQueue: HBJobQueue {
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

    let client: PostgresClient
    let logger: Logger
    let isStopped: NIOLockedValueBox<Bool>

    public init(client: PostgresClient, logger: Logger) {
        self.client = client
        self.logger = logger
        self.isStopped = .init(false)
    }

    public func onInit() async throws {
        do {
            _ = try await self.client.withConnection { connection in
                try await connection.query(
                    """
                    CREATE TABLE IF NOT EXISTS _hb_jobs (
                        id uuid PRIMARY KEY,
                        job json
                    )     
                    """,
                    logger: self.logger
                )
                try await connection.query(
                    """
                    CREATE TABLE IF NOT EXISTS _hb_job_queue (
                        job_id uuid PRIMARY KEY,
                        createdAt timestamp with time zone
                    )
                    """,
                    logger: self.logger
                )
                try await connection.query(
                    """
                    CREATE INDEX IF NOT EXISTS _hb_job_queue_idx 
                    ON _hb_job_queue (createdAt ASC)
                    """,
                    logger: self.logger
                )
                try await connection.query(
                    "CREATE TABLE IF NOT EXISTS _hb_job_processing ( job_id uuid PRIMARY KEY )",
                    logger: self.logger
                )
                try await connection.query(
                    "CREATE TABLE IF NOT EXISTS _hb_job_failed ( job_id uuid PRIMARY KEY )",
                    logger: self.logger
                )
            }
        } catch let error as PSQLError {
            print("\(String(reflecting: error))")
            throw error
        }
    }

    @discardableResult public func push(_ job: HummingbirdJobs.HBJob) async throws -> HummingbirdJobs.JobIdentifier {
        try await self.client.withConnection { connection in
            let queuedJob = HBQueuedJob(job)
            try await add(queuedJob, connection: connection)
            try await connection.query(
                """
                INSERT INTO _hb_job_queue (job_id, createdAt) VALUES (\(queuedJob.id), \(Date.now))
                """,
                logger: self.logger
            )
            return queuedJob.id
        }
    }

    public func finished(jobId: HummingbirdJobs.JobIdentifier) async throws {
        _ = try await self.client.withConnection { connection in
            try await connection.query(
                "DELETE FROM _hb_job_processing WHERE job_id = \(jobId)",
                logger: self.logger
            )
            try await self.delete(jobId: jobId, connection: connection)
        }
    }

    public func failed(jobId: HummingbirdJobs.JobIdentifier, error: Error) async throws {
        _ = try await self.client.withConnection { connection in
            try await connection.query(
                "DELETE FROM _hb_job_processing WHERE job_id = \(jobId)",
                logger: self.logger
            )
            try await connection.query(
                """
                INSERT INTO _hb_job_queue (job_id) VALUES (\(jobId))
                """,
                logger: self.logger
            )
        }
    }

    public func stop() async {
        self.isStopped.withLockedValue { $0 = true }
    }

    public func shutdownGracefully() async {}

    func popFirst() async throws -> HBQueuedJob? {
        do {
            return try await self.client.withConnection { connection -> HBQueuedJob? in
                try Task.checkCancellation()
                let stream = try await connection.query(
                    """
                    DELETE
                    FROM _hb_job_queue pse
                    WHERE pse.job_id =
                          (SELECT pse_inner.job_id
                           FROM _hb_job_queue pse_inner
                           ORDER BY pse_inner.createdAt ASC
                               FOR UPDATE SKIP LOCKED
                           LIMIT 1)
                    RETURNING pse.job_id
                    """,
                    logger: self.logger
                )
                guard let uuid = try await stream.decode(UUID.self, context: .default).first(where: { _ in true }) else {
                    return nil
                }
                let stream2 = try await connection.query(
                    "SELECT job FROM _hb_jobs WHERE id = \(uuid)",
                    logger: self.logger
                )

                guard let job = try await stream2.decode(HBAnyCodableJob.self, context: .default).first(where: { _ in true }) else {
                    return nil
                }
                return HBQueuedJob(id: uuid, job: job.job)
            }
        } catch let error as PSQLError {
            print("\(String(reflecting: error))")
            throw error
        }
    }

    func add(_ job: HBQueuedJob, connection: PostgresConnection) async throws {
        try await connection.query(
            """
            INSERT INTO _hb_jobs (id, job)
            VALUES (\(job.id), \(job.anyCodableJob))
            """,
            logger: self.logger
        )
    }

    func delete(jobId: JobIdentifier, connection: PostgresConnection) async throws {
        try await connection.query(
            "DELETE FROM _hb_jobs WHERE id = \(jobId)",
            logger: self.logger
        )
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
