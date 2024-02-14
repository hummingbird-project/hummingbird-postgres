import Atomics
@testable @_spi(ConnectionPool) import HummingbirdPostgres
import Logging
@_spi(ConnectionPool) import PostgresNIO
import XCTest

final class MigrationTests: XCTestCase {
    struct TestMigration1: HBPostgresMigration {
        func apply(connection: PostgresConnection, logger: Logger) async throws {}
        func revert(connection: PostgresConnection, logger: Logger) async throws {}
    }

    struct TestMigration2: HBPostgresMigration {
        func apply(connection: PostgresConnection, logger: Logger) async throws {}
        func revert(connection: PostgresConnection, logger: Logger) async throws {}
    }

    struct TestMigration3: HBPostgresMigration {
        func apply(connection: PostgresConnection, logger: Logger) async throws {}
        func revert(connection: PostgresConnection, logger: Logger) async throws {}
    }

    struct TestMigration4: HBPostgresMigration {
        func apply(connection: PostgresConnection, logger: Logger) async throws {}
        func revert(connection: PostgresConnection, logger: Logger) async throws {}
    }

    let logger = Logger(label: "MigrationTests")

    override func setUp() async throws {}

    func testMigrations(
        revert: Bool = true,
        _ setup: (HBPostgresMigrations) async throws -> Void,
        verify: (HBPostgresMigrations, PostgresClient) async throws -> Void
    ) async throws {
        let logger = {
            var logger = Logger(label: "MigrationTests")
            logger.logLevel = .debug
            return logger
        }()
        let client = try await PostgresClient(
            configuration: getPostgresConfiguration(),
            backgroundLogger: logger
        )
        let migrations = HBPostgresMigrations()
        try await setup(migrations)
        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }
                do {
                    try await verify(migrations, client)
                    if revert { try await migrations.revert(client: client, logger: logger, dryRun: false) }
                } catch {
                    if revert { try await migrations.revert(client: client, logger: logger, dryRun: false) }
                    throw error
                }
                group.cancelAll()
            }
        } catch let error as PSQLError {
            XCTFail("\(String(reflecting: error))")
        }
    }

    func getAll(client: PostgresClient) async throws -> [String] {
        let repository = HBPostgresMigrationRepository(client: client)
        return try await repository.withContext(logger: self.logger) { context in
            try await repository.getAll(context: context)
        }
    }

    // MARK: Tests

    func testMigrate() async throws {
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "TestMigration1")
            XCTAssertEqual(migrations[1], "TestMigration2")
        }
    }

    func testRevert() async throws {
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            try await migrations.revert(client: client, logger: self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 0)
        }
    }

    func testRevertOrder() async throws {
        struct TestRevertedMigration: HBPostgresMigration {
            static let reverted = ManagedAtomic(false)
            func apply(connection: PostgresConnection, logger: Logger) async throws {}
            func revert(connection: PostgresConnection, logger: Logger) async throws {
                XCTAssertEqual(TestRevertedMigration2.reverted.load(ordering: .relaxed), true)
                Self.reverted.store(true, ordering: .relaxed)
            }
        }
        struct TestRevertedMigration2: HBPostgresMigration {
            static let reverted = ManagedAtomic(false)
            func apply(connection: PostgresConnection, logger: Logger) async throws {}
            func revert(connection: PostgresConnection, logger: Logger) async throws {
                XCTAssertEqual(TestRevertedMigration.reverted.load(ordering: .relaxed), false)
                Self.reverted.store(true, ordering: .relaxed)
            }
        }
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            try await migrations.revert(client: client, logger: self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 0)
        }
    }

    func testSecondMigrate() async throws {
        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
        }
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
            await migrations.add(TestMigration3())
            await migrations.add(TestMigration4())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 4)
            XCTAssertEqual(migrations[0], "TestMigration1")
            XCTAssertEqual(migrations[1], "TestMigration2")
            XCTAssertEqual(migrations[2], "TestMigration3")
            XCTAssertEqual(migrations[3], "TestMigration4")
        }
    }

    func testRemoveMigration() async throws {
        struct TestRevertedMigration: HBPostgresMigration {
            static let reverted = ManagedAtomic(false)
            func apply(connection: PostgresConnection, logger: Logger) async throws {}
            func revert(connection: PostgresConnection, logger: Logger) async throws {
                Self.reverted.store(true, ordering: .relaxed)
            }
        }

        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
            await migrations.add(TestRevertedMigration())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
        }
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
            await migrations.add(revert: TestRevertedMigration())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "TestMigration1")
            XCTAssertEqual(migrations[1], "TestMigration2")
            XCTAssertEqual(TestRevertedMigration.reverted.load(ordering: .relaxed), true)
        }
    }

    func testReplaceMigration() async throws {
        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
            await migrations.add(TestMigration3())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
        }
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
            await migrations.add(TestMigration4())
            await migrations.add(revert: TestMigration3())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 3)
            XCTAssertEqual(migrations[0], "TestMigration1")
            XCTAssertEqual(migrations[1], "TestMigration2")
            XCTAssertEqual(migrations[2], "TestMigration4")
        }
    }

    func testDryRun() async throws {
        do {
            try await self.testMigrations { migrations in
                await migrations.add(TestMigration1())
                await migrations.add(TestMigration2())
            } verify: { migrations, client in
                try await migrations.apply(client: client, logger: self.logger, dryRun: true)
            }
            XCTFail("Shouldn't get here")
        } catch let error as HBPostgresMigrationError where error == .requiresChanges {}
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration1())
            await migrations.add(TestMigration2())
        } verify: { migrations, client in
            try await migrations.apply(client: client, logger: self.logger, dryRun: false)
            try await migrations.apply(client: client, logger: self.logger, dryRun: true)
        }
    }
}
