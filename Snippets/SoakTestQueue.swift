import HummingbirdPostgres
import Jobs
import JobsPostgres
import Logging
import NIOCore
import NIOPosix
import PostgresNIO
import ServiceLifecycle

var logger = Logger(label: "Soak")
logger.logLevel = .debug
let postgresClient = PostgresClient(
    configuration: .init(host: "localhost", port: 5432, username: "test_user", password: "test_password", database: "test_db", tls: .disable),
    backgroundLogger: logger
)
let postgresMigrations = PostgresMigrations()
let jobQueue = await JobQueue(
    .postgres(
        client: postgresClient,
        migrations: postgresMigrations,
        configuration: .init(pendingJobsInitialization: .remove, failedJobsInitialization: .remove, processingJobsInitialization: .remove, pollTime: .milliseconds(1)),
        logger: logger
    ),
    numWorkers: 4,
    logger: logger
)

struct MyJob: JobParameters {
    static var jobName = "Test"

    let sleep: Int
}

struct MyError: Error {}
jobQueue.registerJob(parameters: MyJob.self, maxRetryCount: 4) { parameters, _ in
    try await Task.sleep(for: .milliseconds(parameters.sleep))
    if Int.random(in: 0..<100) < 3 {
        throw MyError()
    }
}

try await withThrowingTaskGroup(of: Void.self) { group in
    let serviceGroup = ServiceGroup(
        configuration: .init(
            services: [postgresClient, jobQueue],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: logger
        )
    )
    group.addTask {
        try await serviceGroup.run()
    }
    group.addTask {
        try await postgresMigrations.apply(client: postgresClient, groups: [.jobQueue], logger: logger, dryRun: false)
    }
    try await group.next()
    group.addTask {
        for _ in 0..<100_000 {
            try await jobQueue.push(MyJob(sleep: Int.random(in: 1..<20)))
            try await Task.sleep(for: .milliseconds(Int.random(in: 1..<10)))
        }
    }
    group.addTask {
        for _ in 0..<100_000 {
            try await jobQueue.push(MyJob(sleep: Int.random(in: 1..<20)))
            try await Task.sleep(for: .milliseconds(Int.random(in: 1..<10)))
        }
    }
    try await group.next()
    try await group.next()
    await serviceGroup.triggerGracefulShutdown()
}
