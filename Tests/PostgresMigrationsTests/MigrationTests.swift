import Atomics
import Foundation
import Logging
import PostgresNIO
import XCTest

@testable import PostgresMigrations

func getPostgresConfiguration() async throws -> PostgresClient.Configuration {
    .init(
        host: ProcessInfo.processInfo.environment["POSTGRES_HOSTNAME"] ?? "localhost",
        port: 5432,
        username: ProcessInfo.processInfo.environment["POSTGRES_USER"] ?? "test_user",
        password: ProcessInfo.processInfo.environment["POSTGRES_PASSWORD"] ?? "test_password",
        database: ProcessInfo.processInfo.environment["POSTGRES_DB"] ?? "test_db",
        tls: .disable
    )
}

final class MigrationTests: XCTestCase {
    /// Migration you can set name and group of
    struct TestMigration: DatabaseMigration {
        init(name: String, group: DatabaseMigrationGroup = .default) {
            self.name = name
            self.group = group
        }

        func apply(connection: PostgresConnection, logger: Logger) async throws {}
        func revert(connection: PostgresConnection, logger: Logger) async throws {}

        let name: String
        let group: DatabaseMigrationGroup
    }
    /// Test migration used to verify order or apply and reverts
    struct TestOrderMigration: DatabaseMigration {
        final class Order: Sendable {
            let value: ManagedAtomic<Int>

            init() {
                self.value = .init(1)
            }

            func expect(_ value: Int, file: StaticString = #filePath, line: UInt = #line) {
                XCTAssertEqual(value, self.value.load(ordering: .relaxed), file: file, line: line)
                self.value.wrappingIncrement(by: 1, ordering: .relaxed)
            }
        }

        init(
            name: String,
            order: Order = Order(),
            applyOrder: Int? = nil,
            revertOrder: Int? = nil,
            group: DatabaseMigrationGroup = .default
        ) {
            self.order = order
            self.name = name
            self.group = group
            self.expectedApply = applyOrder
            self.expectedRevert = revertOrder
        }

        func apply(connection: PostgresConnection, logger: Logger) async throws {
            if let expectedApply {
                self.order.expect(expectedApply)
            }
        }

        func revert(connection: PostgresConnection, logger: Logger) async throws {
            if let expectedRevert {
                self.order.expect(expectedRevert)
            }
        }

        let name: String
        let group: DatabaseMigrationGroup
        let order: Order
        let expectedApply: Int?
        let expectedRevert: Int?
    }

    static let logger = Logger(label: "MigrationTests")

    override func setUp() async throws {}

    func testMigrations(
        revert: Bool = true,
        groups: [DatabaseMigrationGroup] = [.default],
        _ setup: (DatabaseMigrations) async throws -> Void,
        verify: (DatabaseMigrations, PostgresClient) async throws -> Void,
        file: StaticString = #filePath,
        line: UInt = #line
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
        let migrations = DatabaseMigrations()
        try await setup(migrations)
        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }
                do {
                    try await verify(migrations, client)
                    if revert { try await migrations.revert(client: client, groups: groups, logger: logger, dryRun: false) }
                } catch {
                    if revert { try await migrations.revert(client: client, groups: groups, logger: logger, dryRun: false) }
                    throw error
                }
                group.cancelAll()
            }
        } catch let error as PSQLError {
            XCTFail("\(String(reflecting: error))", file: file, line: line)
        }
    }

    func getAll(client: PostgresClient, groups: [DatabaseMigrationGroup] = [.default]) async throws -> [String] {
        let repository = PostgresMigrationRepository(client: client)
        return try await repository.getAll(client: client, logger: Self.logger).compactMap { migration in
            if groups.first(where: { group in group == migration.group }) != nil {
                return migration.name
            } else {
                return nil
            }
        }
    }

    // MARK: Tests

    func testMigrate() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            order.expect(3)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
        }
    }

    func testRevert() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, revertOrder: 4))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2, revertOrder: 3))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            try await migrations.revert(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            order.expect(5)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 0)
        }
    }

    func testSecondMigrate() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
        }
        try await self.testMigrations { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
            await migrations.add(TestOrderMigration(name: "test3", order: order, applyOrder: 3))
            await migrations.add(TestOrderMigration(name: "test4", order: order, applyOrder: 4))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            order.expect(5)
            XCTAssertEqual(migrations.count, 4)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
            XCTAssertEqual(migrations[2], "test3")
            XCTAssertEqual(migrations[3], "test4")
        }
    }

    func testRemoveMigration() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
            await migrations.add(TestOrderMigration(name: "test3", order: order, applyOrder: 3))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
        }
        do {
            try await self.testMigrations { migrations in
                await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
                await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
                await migrations.register(TestOrderMigration(name: "test3", order: order, applyOrder: 3, revertOrder: 4))
            } verify: { migrations, client in
                try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
                XCTFail("Applying migrations should fail as a migration has been removed")
            }
        } catch let error as DatabaseMigrationError where error == .appliedMigrationsInconsistent {
        }
    }

    func testReplaceMigration() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
            await migrations.add(TestOrderMigration(name: "test3", order: order, applyOrder: 3))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
        }
        do {
            try await self.testMigrations { migrations in
                await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
                await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
                await migrations.add(TestOrderMigration(name: "test4", order: order, applyOrder: 5))
                await migrations.register(TestOrderMigration(name: "test3", order: order, applyOrder: 3, revertOrder: 4))
            } verify: { migrations, client in
                try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
                XCTFail("Applying migrations should fail as a migration has been replaced")
            }
        } catch let error as DatabaseMigrationError where error == .appliedMigrationsInconsistent {
        }
    }

    func testRevertInconsistent() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations(revert: false) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
            await migrations.add(TestOrderMigration(name: "test3", order: order, applyOrder: 3))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
        }

        try await self.testMigrations { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2))
            await migrations.register(TestOrderMigration(name: "test3", order: order, applyOrder: 3, revertOrder: 4))
        } verify: { migrations, client in
            try await migrations.revertInconsistent(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client)
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
        }

    }

    func testDryRun() async throws {
        do {
            try await self.testMigrations(groups: [.default, .test]) { migrations in
                await migrations.add(TestOrderMigration(name: "test1"))
                await migrations.add(TestOrderMigration(name: "test2"))
            } verify: { migrations, client in
                try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: true)
            }
            XCTFail("Shouldn't get here")
        } catch let error as DatabaseMigrationError where error == .requiresChanges {}
        try await self.testMigrations(groups: [.default, .test]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1"))
            await migrations.add(TestOrderMigration(name: "test2"))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: true)
        }
    }

    func testGroups() async throws {
        let order = TestOrderMigration.Order()
        try await self.testMigrations(groups: [.default, .test]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, group: .default))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2, group: .test))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
            order.expect(3)
            let migrations = try await getAll(client: client, groups: [.default, .test])
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
        }
    }

    func testAddingToGroup() async throws {
        let order = TestOrderMigration.Order()
        // Add two migrations from different groups
        try await self.testMigrations(revert: false, groups: [.default, .test]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, group: .default))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2, group: .test))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client, groups: [.default, .test])
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
        }
        // Add additional migration to default group before the migration from the test group
        try await self.testMigrations(groups: [.default, .test]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, group: .default))
            await migrations.add(TestOrderMigration(name: "test1_2", order: order, applyOrder: 3, group: .default))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2, group: .test))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client, groups: [.default, .test])
            XCTAssertEqual(migrations.count, 3)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
            XCTAssertEqual(migrations[2], "test1_2")
        }
    }

    func testRemovingFromGroup() async throws {
        let order = TestOrderMigration.Order()
        // Add two migrations from different groups
        try await self.testMigrations(revert: false, groups: [.default, .test]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, group: .default))
            await migrations.add(TestOrderMigration(name: "test1_2", order: order, applyOrder: 2, group: .default))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 3, group: .test))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client, groups: [.default, .test])
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test1_2")
            XCTAssertEqual(migrations[2], "test2")
        }
        // Remove migration from default group before the migration from the test group
        do {
            try await self.testMigrations(groups: [.default, .test]) { migrations in
                await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, group: .default))
                await migrations.register(TestOrderMigration(name: "test1_2", order: order, revertOrder: 4, group: .default))
                await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2, group: .test))
            } verify: { migrations, client in
                try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
                XCTFail("Applying migrations should fail as a migration has been removed")
            }
        } catch let error as DatabaseMigrationError where error == .appliedMigrationsInconsistent {
        }

    }

    func testGroupsIgnoreOtherGroups() async throws {
        let order = TestOrderMigration.Order()
        // Add two migrations from different groups
        try await self.testMigrations(revert: false, groups: [.default, .test]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, group: .default))
            await migrations.add(TestOrderMigration(name: "test2", order: order, applyOrder: 2, group: .test))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client, groups: [.default, .test])
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
        }
        // Only add the migration from the first group, but also only process the first group
        try await self.testMigrations(groups: [.default]) { migrations in
            await migrations.add(TestOrderMigration(name: "test1", order: order, applyOrder: 1, revertOrder: 3, group: .default))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default], logger: Self.logger, dryRun: false)
            let migrations = try await getAll(client: client, groups: [.default, .test])
            XCTAssertEqual(migrations.count, 2)
            XCTAssertEqual(migrations[0], "test1")
            XCTAssertEqual(migrations[1], "test2")
        }
        try await self.testMigrations(groups: [.default, .test]) { migrations in
            await migrations.register(TestOrderMigration(name: "test2", order: order, applyOrder: 2, revertOrder: 4, group: .test))
        } verify: { _, _ in
        }
        order.expect(5)
    }

    func testUniqueElements() {
        XCTAssertEqual([1, 4, 67, 2, 1, 1, 5, 4].uniqueElements, [1, 4, 67, 2, 5])
        XCTAssertEqual([1, 1, 1, 2, 2].uniqueElements, [1, 2])
        XCTAssertEqual([2, 1, 1, 1, 2, 2].uniqueElements, [2, 1])
    }

    /// Test we catch when migrations with duplicate names are added
    func testDuplicateMigrations() async throws {
        do {
            try await self.testMigrations { migrations in
                await migrations.add(TestMigration(name: "test1"))
                await migrations.add(TestMigration(name: "test1"))
            } verify: { migrations, client in
                try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: true)
            }
            XCTFail("Duplicate migration names should throw an error")
        } catch let error as DatabaseMigrationError where error == .dupicateNames {
        } catch {
            XCTFail("\(error)")
        }
    }

    /// Test we don't error on migrations with the same name but in different groups
    func testDuplicateMigrationsAcrossGroups() async throws {
        try await self.testMigrations { migrations in
            await migrations.add(TestMigration(name: "test1"))
            await migrations.add(TestMigration(name: "test1", group: .test))
            await migrations.add(TestMigration(name: "test2"))
        } verify: { migrations, client in
            try await migrations.apply(client: client, groups: [.default, .test], logger: Self.logger, dryRun: false)
        }
    }
}

extension DatabaseMigrationGroup {
    static var test: Self { .init("test") }
}
