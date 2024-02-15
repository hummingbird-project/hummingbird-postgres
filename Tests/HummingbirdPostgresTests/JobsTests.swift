//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Hummingbird
import HummingbirdJobs
@testable @_spi(ConnectionPool) import HummingbirdJobsPostgres
import HummingbirdXCT
@_spi(ConnectionPool) import PostgresNIO
import ServiceLifecycle
import XCTest

extension XCTestExpectation {
    convenience init(description: String, expectedFulfillmentCount: Int) {
        self.init(description: description)
        self.expectedFulfillmentCount = expectedFulfillmentCount
    }
}

final class JobsTests: XCTestCase {
    func wait(for expectations: [XCTestExpectation], timeout: TimeInterval) async {
        #if (os(Linux) && swift(<5.9)) || swift(<5.8)
        super.wait(for: expectations, timeout: timeout)
        #else
        await fulfillment(of: expectations, timeout: timeout)
        #endif
    }

    static let env = HBEnvironment()
    static let redisHostname = env.get("REDIS_HOSTNAME") ?? "localhost"

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        configuration: HBPostgresJobQueue.Configuration = .init(failedJobsInitialization: .remove, processingJobsInitialization: .remove),
        test: (HBPostgresJobQueue) async throws -> T
    ) async throws -> T {
        var logger = Logger(label: "HummingbirdJobsTests")
        logger.logLevel = .debug
        let postgresClient = try await PostgresClient(
            configuration: getPostgresConfiguration(),
            backgroundLogger: logger
        )
        let postgresJobQueue = HBPostgresJobQueue(
            client: postgresClient,
            configuration: configuration,
            logger: logger
        )
        let jobQueueHandler = HBJobQueueHandler(
            queue: postgresJobQueue,
            numWorkers: numWorkers,
            logger: logger
        )

        do {
            return try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(
                    configuration: .init(
                        services: [PostgresClientService(client: postgresClient), jobQueueHandler],
                        gracefulShutdownSignals: [.sigterm, .sigint],
                        logger: Logger(label: "JobQueueService")
                    )
                )
                group.addTask {
                    try await serviceGroup.run()
                }
                try await Task.sleep(for: .seconds(1))
                do {
                    let value = try await test(postgresJobQueue)
                    await serviceGroup.triggerGracefulShutdown()
                    return value
                } catch let error as PSQLError {
                    XCTFail("\(String(reflecting: error))")
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                } catch {
                    await serviceGroup.triggerGracefulShutdown()
                    throw error
                }
            }
        } catch let error as PSQLError {
            XCTFail("\(String(reflecting: error))")
            throw error
        }
    }

    func testBasic() async throws {
        struct TestJob: HBJob {
            static let name = "testBasic"
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

            let value: Int
            func execute(logger: Logger) async throws {
                print(self.value)
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                Self.expectation.fulfill()
            }
        }
        TestJob.register()
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            try await jobQueue.push(TestJob(value: 1))
            try await jobQueue.push(TestJob(value: 2))
            try await jobQueue.push(TestJob(value: 3))
            try await jobQueue.push(TestJob(value: 4))
            try await jobQueue.push(TestJob(value: 5))
            try await jobQueue.push(TestJob(value: 6))
            try await jobQueue.push(TestJob(value: 7))
            try await jobQueue.push(TestJob(value: 8))
            try await jobQueue.push(TestJob(value: 9))
            try await jobQueue.push(TestJob(value: 10))

            await self.wait(for: [TestJob.expectation], timeout: 5)

            let pendingJobs = try await jobQueue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }

    func testMultipleWorkers() async throws {
        struct TestJob: HBJob {
            static let name = "testMultipleWorkers"
            static let runningJobCounter = ManagedAtomic(0)
            static let maxRunningJobCounter = ManagedAtomic(0)
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

            let value: Int
            func execute(logger: Logger) async throws {
                let runningJobs = Self.runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
                if runningJobs > Self.maxRunningJobCounter.load(ordering: .relaxed) {
                    Self.maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<100)))
                print(self.value)
                Self.expectation.fulfill()
                Self.runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
            }
        }
        TestJob.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob(value: 1))
            try await jobQueue.push(TestJob(value: 2))
            try await jobQueue.push(TestJob(value: 3))
            try await jobQueue.push(TestJob(value: 4))
            try await jobQueue.push(TestJob(value: 5))
            try await jobQueue.push(TestJob(value: 6))
            try await jobQueue.push(TestJob(value: 7))
            try await jobQueue.push(TestJob(value: 8))
            try await jobQueue.push(TestJob(value: 9))
            try await jobQueue.push(TestJob(value: 10))

            await self.wait(for: [TestJob.expectation], timeout: 5)

            XCTAssertGreaterThan(TestJob.maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(TestJob.maxRunningJobCounter.load(ordering: .relaxed), 4)

            let pendingJobs = try await jobQueue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }

    func testErrorRetryCount() async throws {
        struct FailedError: Error {}

        struct TestJob: HBJob {
            static let name = "testErrorRetryCount"
            static let maxRetryCount = 3
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
            func execute(logger: Logger) async throws {
                Self.expectation.fulfill()
                throw FailedError()
            }
        }
        TestJob.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob())

            await self.wait(for: [TestJob.expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.getJobs(withStatus: .failed)
            XCTAssertEqual(failedJobs.count, 1)
            let pendingJobs = try await jobQueue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }

    /// Test job is cancelled on shutdown
    func testShutdownJob() async throws {
        struct TestJob: HBJob {
            static let name = "testShutdownJob"
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
            func execute(logger: Logger) async throws {
                Self.expectation.fulfill()
                try await Task.sleep(for: .seconds(10))
            }
        }
        TestJob.register()
        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob())
            await self.wait(for: [TestJob.expectation], timeout: 5)
        }

        try await self.testJobQueue(
            numWorkers: 4,
            configuration: .init(failedJobsInitialization: .doNothing, processingJobsInitialization: .doNothing)
        ) { jobQueue in
            let failedJobs = try await jobQueue.getJobs(withStatus: .processing)
            XCTAssertEqual(failedJobs.count, 1)
            let pendingJobs = try await jobQueue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        struct TestJob1: HBJob {
            static let name = "testFailToDecode"
            func execute(logger: Logger) async throws {}
        }
        struct TestJob2: HBJob {
            static let name = "testFailToDecode2"
            static var value: String?
            static let expectation = XCTestExpectation(description: "TestJob2.execute was called")
            let value: String
            func execute(logger: Logger) async throws {
                Self.value = self.value
                Self.expectation.fulfill()
            }
        }
        TestJob2.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob1())
            try await jobQueue.push(TestJob2(value: "test"))
            // stall to give job chance to start running
            await self.wait(for: [TestJob2.expectation], timeout: 5)

            let pendingJobs = try await jobQueue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }

        XCTAssertEqual(TestJob2.value, "test")
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    func testRerunAtStartup() async throws {
        struct RetryError: Error {}
        struct TestJob: HBJob {
            static let name = "testRerunAtStartup"
            static let maxRetryCount: Int = 0
            static var firstTime = ManagedAtomic(true)
            static var finished = ManagedAtomic(false)
            static let failedExpectation = XCTestExpectation(description: "TestJob failed", expectedFulfillmentCount: 1)
            static let succeededExpectation = XCTestExpectation(description: "TestJob2 succeeded", expectedFulfillmentCount: 1)
            func execute(logger: Logger) async throws {
                if Self.firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                    Self.failedExpectation.fulfill()
                    throw RetryError()
                }
                Self.succeededExpectation.fulfill()
                Self.finished.store(true, ordering: .relaxed)
            }
        }
        TestJob.register()

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            try await jobQueue.push(TestJob())

            await self.wait(for: [TestJob.failedExpectation], timeout: 10)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))

            XCTAssertFalse(TestJob.firstTime.load(ordering: .relaxed))
            XCTAssertFalse(TestJob.finished.load(ordering: .relaxed))
        }

        try await self.testJobQueue(numWorkers: 4, configuration: .init(failedJobsInitialization: .rerun)) { _ in
            await self.wait(for: [TestJob.succeededExpectation], timeout: 10)
            XCTAssertTrue(TestJob.finished.load(ordering: .relaxed))
        }
    }

    func testCustomTableNames() async throws {
        struct TestJob: HBJob {
            static let name = "testBasic"
            static let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

            let value: Int
            func execute(logger: Logger) async throws {
                print(self.value)
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                Self.expectation.fulfill()
            }
        }
        TestJob.register()
        try await self.testJobQueue(
            numWorkers: 4,
            configuration: .init(jobTable: "_test_job_table", jobQueueTable: "_test_job_queue_table")
        ) { jobQueue in
            try await jobQueue.push(TestJob(value: 1))
            try await jobQueue.push(TestJob(value: 2))
            try await jobQueue.push(TestJob(value: 3))
            try await jobQueue.push(TestJob(value: 4))
            try await jobQueue.push(TestJob(value: 5))
            try await jobQueue.push(TestJob(value: 6))
            try await jobQueue.push(TestJob(value: 7))
            try await jobQueue.push(TestJob(value: 8))
            try await jobQueue.push(TestJob(value: 9))
            try await jobQueue.push(TestJob(value: 10))

            await self.wait(for: [TestJob.expectation], timeout: 5)

            let pendingJobs = try await jobQueue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }
}
