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
import NIOConcurrencyHelpers
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

    func createJobQueue(numWorkers: Int, configuration: HBPostgresQueue.Configuration) async throws -> HBJobQueue<HBPostgresQueue> {
        let logger = {
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let postgresClient = try await PostgresClient(
            configuration: getPostgresConfiguration(),
            backgroundLogger: logger
        )
        return HBJobQueue(
            HBPostgresQueue(
                client: postgresClient,
                configuration: configuration,
                logger: logger
            ),
            numWorkers: numWorkers,
            logger: logger
        )
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        jobQueue: HBJobQueue<HBPostgresQueue>,
        test: (HBJobQueue<HBPostgresQueue>) async throws -> T
    ) async throws -> T {
        do {
            return try await withThrowingTaskGroup(of: Void.self) { group in
                let serviceGroup = ServiceGroup(
                    configuration: .init(
                        services: [PostgresClientService(client: jobQueue.queue.client), jobQueue],
                        gracefulShutdownSignals: [.sigterm, .sigint],
                        logger: jobQueue.queue.logger
                    )
                )
                group.addTask {
                    try await serviceGroup.run()
                }
                try await Task.sleep(for: .seconds(1))
                do {
                    let value = try await test(jobQueue)
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

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        configuration: HBPostgresQueue.Configuration = .init(failedJobsInitialization: .remove, processingJobsInitialization: .remove),
        test: (HBJobQueue<HBPostgresQueue>) async throws -> T
    ) async throws -> T {
        let jobQueue = try await self.createJobQueue(numWorkers: numWorkers, configuration: configuration)
        return try await self.testJobQueue(jobQueue: jobQueue, test: test)
    }

    func testBasic() async throws {
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }
            try await jobQueue.push(id: jobIdentifer, parameters: 1)
            try await jobQueue.push(id: jobIdentifer, parameters: 2)
            try await jobQueue.push(id: jobIdentifer, parameters: 3)
            try await jobQueue.push(id: jobIdentifer, parameters: 4)
            try await jobQueue.push(id: jobIdentifer, parameters: 5)
            try await jobQueue.push(id: jobIdentifer, parameters: 6)
            try await jobQueue.push(id: jobIdentifer, parameters: 7)
            try await jobQueue.push(id: jobIdentifer, parameters: 8)
            try await jobQueue.push(id: jobIdentifer, parameters: 9)
            try await jobQueue.push(id: jobIdentifer, parameters: 10)

            await self.wait(for: [expectation], timeout: 5)
        }
    }

    func testMultipleWorkers() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let runningJobCounter = ManagedAtomic(0)
        let maxRunningJobCounter = ManagedAtomic(0)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { parameters, context in
                let runningJobs = runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
                if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                    maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                context.logger.info("Parameters=\(parameters)")
                expectation.fulfill()
                runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
            }

            try await jobQueue.push(id: jobIdentifer, parameters: 1)
            try await jobQueue.push(id: jobIdentifer, parameters: 2)
            try await jobQueue.push(id: jobIdentifer, parameters: 3)
            try await jobQueue.push(id: jobIdentifer, parameters: 4)
            try await jobQueue.push(id: jobIdentifer, parameters: 5)
            try await jobQueue.push(id: jobIdentifer, parameters: 6)
            try await jobQueue.push(id: jobIdentifer, parameters: 7)
            try await jobQueue.push(id: jobIdentifer, parameters: 8)
            try await jobQueue.push(id: jobIdentifer, parameters: 9)
            try await jobQueue.push(id: jobIdentifer, parameters: 10)

            await self.wait(for: [expectation], timeout: 5)

            XCTAssertGreaterThan(maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(maxRunningJobCounter.load(ordering: .relaxed), 4)
        }
    }

    func testErrorRetryCount() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(jobIdentifer, maxRetryCount: 3) { _, _ in
                expectation.fulfill()
                throw FailedError()
            }
            try await jobQueue.push(id: jobIdentifer, parameters: 0)

            await self.wait(for: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.getJobs(withStatus: .failed)
            XCTAssertEqual(failedJobs.count, 1)
            let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
        }
    }

    func testJobSerialization() async throws {
        struct TestJobParameters: Codable {
            let id: Int
            let message: String
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called")
        let jobIdentifer = HBJobIdentifier<TestJobParameters>(#function)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { parameters, _ in
                XCTAssertEqual(parameters.id, 23)
                XCTAssertEqual(parameters.message, "Hello!")
                expectation.fulfill()
            }
            try await jobQueue.push(id: jobIdentifer, parameters: .init(id: 23, message: "Hello!"))

            await self.wait(for: [expectation], timeout: 5)
        }
    }

    /// Test job is cancelled on shutdown
    func testShutdownJob() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        var logger = Logger(label: "HummingbirdJobsTests")
        logger.logLevel = .trace
        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(jobIdentifer) { _, _ in
                expectation.fulfill()
                try await Task.sleep(for: .milliseconds(1000))
            }
            try await jobQueue.push(id: jobIdentifer, parameters: 0)
            await self.wait(for: [expectation], timeout: 5)

            let failedJobs = try await jobQueue.queue.getJobs(withStatus: .processing)
            XCTAssertEqual(failedJobs.count, 1)
            let pendingJobs = try await jobQueue.queue.getJobs(withStatus: .pending)
            XCTAssertEqual(pendingJobs.count, 0)
            return jobQueue
        }
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        let string: NIOLockedValueBox<String> = .init("")
        let jobIdentifer1 = HBJobIdentifier<Int>(#function)
        let jobIdentifer2 = HBJobIdentifier<String>(#function)
        let expectation = XCTestExpectation(description: "job was called", expectedFulfillmentCount: 1)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(jobIdentifer2) { parameters, _ in
                string.withLockedValue { $0 = parameters }
                expectation.fulfill()
            }
            try await jobQueue.push(id: jobIdentifer1, parameters: 2)
            try await jobQueue.push(id: jobIdentifer2, parameters: "test")
            await self.wait(for: [expectation], timeout: 5)
        }
        string.withLockedValue {
            XCTAssertEqual($0, "test")
        }
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    func testRerunAtStartup() async throws {
        struct RetryError: Error {}
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let firstTime = ManagedAtomic(true)
        let finished = ManagedAtomic(false)
        let failedExpectation = XCTestExpectation(description: "TestJob failed", expectedFulfillmentCount: 1)
        let succeededExpectation = XCTestExpectation(description: "TestJob2 succeeded", expectedFulfillmentCount: 1)
        let job = HBJobDefinition(id: jobIdentifer) { _, _ in
            if firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                failedExpectation.fulfill()
                throw RetryError()
            }
            succeededExpectation.fulfill()
            finished.store(true, ordering: .relaxed)
        }
        let jobQueue = try await createJobQueue(numWorkers: 1, configuration: .init(pendingJobsInitialization: .remove, failedJobsInitialization: .rerun))
        jobQueue.registerJob(job)
        try await self.testJobQueue(jobQueue: jobQueue) { jobQueue in

            try await jobQueue.push(id: jobIdentifer, parameters: 0)

            await self.wait(for: [failedExpectation], timeout: 10)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))

            XCTAssertFalse(firstTime.load(ordering: .relaxed))
            XCTAssertFalse(finished.load(ordering: .relaxed))
        }

        let jobQueue2 = try await createJobQueue(numWorkers: 1, configuration: .init(failedJobsInitialization: .rerun))
        jobQueue2.registerJob(job)
        try await self.testJobQueue(jobQueue: jobQueue2) { _ in
            await self.wait(for: [succeededExpectation], timeout: 10)
            XCTAssertTrue(finished.load(ordering: .relaxed))
        }
    }

    func testMultipleJobQueueHandlers() async throws {
        let jobIdentifer = HBJobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 200)
        let logger = {
            var logger = Logger(label: "HummingbirdJobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let job = HBJobDefinition(id: jobIdentifer) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        let postgresClient = try await PostgresClient(
            configuration: getPostgresConfiguration(),
            backgroundLogger: logger
        )
        let jobQueue = HBJobQueue(
            .postgres(client: postgresClient, logger: logger),
            numWorkers: 2,
            logger: logger
        )
        let jobQueue2 = HBJobQueue(
            HBPostgresQueue(
                client: postgresClient,
                logger: logger
            ),
            numWorkers: 2,
            logger: logger
        )
        jobQueue.registerJob(job)
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [PostgresClientService(client: postgresClient), jobQueue, jobQueue2],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            do {
                for i in 0..<200 {
                    try await jobQueue.push(id: jobIdentifer, parameters: i)
                }
                await self.wait(for: [expectation], timeout: 5)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                XCTFail("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }
}
