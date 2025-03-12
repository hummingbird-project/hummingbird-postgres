//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import PostgresNIO

/// Database migration support
public actor DatabaseMigrations {
    enum State {
        case waiting([CheckedContinuation<Void, Error>])
        case completed
        case failed(Error)
    }

    var migrations: [DatabaseMigration]
    var reverts: [String: DatabaseMigration]
    var state: State

    /// Initialize a DatabaseMigrations object
    public init() {
        self.migrations = []
        self.reverts = [:]
        self.state = .waiting([])
    }

    /// Add migration to list of migrations to be be applied
    /// - Parameters
    ///   - migration: DatabaseMigration to be applied
    ///   - skipDuplicates: Only add migration if it doesn't exist in the list
    public func add(_ migration: DatabaseMigration, skipDuplicates: Bool = false) {
        if skipDuplicates {
            let existingMigration = self.migrations.first {
                type(of: $0) == type(of: migration)
            }
            guard existingMigration == nil else { return }
        }
        self.migrations.append(migration)
    }

    /// Register migration without it being applied
    ///
    /// This is useful for migrations you might have to revert.
    /// - Parameter migration: DatabaseMigration to be registerd
    public func register(_ migration: DatabaseMigration) {
        self.reverts[migration.name] = migration
    }

    /// Apply database migrations
    ///
    /// This function compares the list of applied migrations and the list of desired migrations. If there
    /// are migrations in the applied list that don't exist in the desired list or the order of migrations
    /// is different in the applied list then an error is thrown.
    ///
    /// - Parameters:
    ///   - client: Postgres client
    ///   - groups: Migration groups to apply, an empty array means all groups
    ///   - logger: Logger to use
    ///   - dryRun: Should migrations actually be applied, or should we just report what would be applied and reverted
    public func apply(
        client: PostgresClient,
        groups: [DatabaseMigrationGroup] = [],
        logger: Logger,
        dryRun: Bool
    ) async throws {
        try checkForDuplicates(logger: logger)
        // wait a small period to ensure the PostgresClient has started up
        try await Task.sleep(for: .microseconds(100))
        switch self.state {
        case .completed, .failed:
            self.state = .waiting([])
        case .waiting:
            break
        }
        let migrations = self.migrations
        let repository = PostgresMigrationRepository(client: client)
        do {
            // setup migration repository (create table)
            _ = try await repository.setup(client: client, logger: logger)
            // get migrations currently applied in the order they were applied
            let appliedMigrations = try await repository.getAll(client: client, logger: logger)

            // if groups array passed in is empty then work out list of migration groups by combining
            // list of groups from migrations and applied migrations
            let groups =
                groups.count == 0
                ? (migrations.map(\.group) + appliedMigrations.map(\.group)).uniqueElements
                : groups
            var migrationsToApply: [DatabaseMigration] = .init()
            // for each group apply/revert migrations
            for group in groups {
                let groupMigrations = migrations.filter { $0.group == group }
                let appliedGroupMigrations = appliedMigrations.filter { $0.group == group }

                let minMigrationCount = min(groupMigrations.count, appliedGroupMigrations.count)
                var i = 0
                // while migrations and applied migrations are the same
                while i < minMigrationCount,
                    appliedGroupMigrations[i].name == groupMigrations[i].name
                {
                    i += 1
                }
                guard i == appliedGroupMigrations.count else {
                    logger.error("Applied migrations in \(group.name) group are inconsistent with migration list")
                    printMigrationComparison(expected: groupMigrations.map(\.name), applied: appliedGroupMigrations.map(\.name), logger: logger)
                    throw DatabaseMigrationError.appliedMigrationsInconsistent
                }
                // Add migrations that have not been applied to list
                migrationsToApply.append(contentsOf: groupMigrations[i...])
                for migration in migrationsToApply {
                    logger.info("Migrating \(migration.name) from group \(group.name) \(dryRun ? " (dry run)" : "")")
                }
            }
            if dryRun {
                if migrationsToApply.count > 0 {
                    throw DatabaseMigrationError.requiresChanges
                }
            } else if migrationsToApply.count > 0 {
                _ = try await repository.withTransaction(logger: logger) { context in
                    for migration in migrationsToApply {
                        try await migration.apply(
                            connection: context.connection,
                            logger: context.logger
                        )
                        try await repository.add(migration, context: context)
                    }
                }
            }
        } catch {
            self.setFailed(error)
            throw error
        }
        self.setCompleted()
    }

    /// Revert database migrations
    ///
    /// This will revert all the migrations in the applied migration list
    /// - Parameters:
    ///   - client: Postgres client
    ///   - groups: Migration groups to revert, an empty array means all groups
    ///   - logger: Logger to use
    ///   - dryRun: Should migrations actually be reverted, or should we just report what would be reverted
    public func revert(
        client: PostgresClient,
        groups: [DatabaseMigrationGroup] = [],
        logger: Logger,
        dryRun: Bool
    ) async throws {
        let repository = PostgresMigrationRepository(client: client)
        do {
            let migrations = self.migrations
            // build map of registered migrations
            let registeredMigrations = {
                var registeredMigrations = self.reverts
                for migration in migrations {
                    registeredMigrations[migration.name] = migration
                }
                return registeredMigrations
            }()

            // setup migration repository (create table)
            _ = try await repository.setup(client: client, logger: logger)
            // get migrations currently applied in the order they were applied
            let appliedMigrations = try await repository.getAll(client: client, logger: logger)

            // if groups array passed in is empty then work out list of migration groups by combining
            // list of groups from migrations and applied migrations
            let groups =
                groups.count == 0
                ? (migrations.map(\.group) + appliedMigrations.map(\.group)).uniqueElements
                : groups
            var migrationsToRevert: [DatabaseMigration] = .init()
            // for each group revert migrations
            for group in groups {
                let appliedGroupMigrations = appliedMigrations.filter { $0.group == group }
                // Revert migrations in reverse
                for j in (0..<appliedGroupMigrations.count).reversed() {
                    let migrationName = appliedGroupMigrations[j].name
                    // look for migration to revert in registered migration list and revert dictionary.
                    guard let migration = registeredMigrations[migrationName]
                    else {
                        logger.error("Failed to find migration \(migrationName)")
                        throw DatabaseMigrationError.cannotRevertMigration
                    }
                    migrationsToRevert.append(migration)
                    logger.info("Reverting \(migrationName) from group \(group.name) \(dryRun ? " (dry run)" : "")")
                }
            }
            if dryRun {
                if migrationsToRevert.count > 0 {
                    throw DatabaseMigrationError.requiresChanges
                }
            } else if migrationsToRevert.count > 0 {
                _ = try await repository.withTransaction(logger: logger) { context in
                    for migration in migrationsToRevert {
                        try await migration.revert(
                            connection: context.connection,
                            logger: context.logger
                        )
                        try await repository.remove(migration, context: context)
                    }
                }
            }
        } catch {
            self.setFailed(error)
            throw error
        }
    }

    /// Revert database migrations that are inconsistent with the migration list
    ///
    /// This will revert any migrations in the applied migration list after an inconsistency has been found in
    /// list eg a migration is missing or the order of migrations has changed. This is a destructive action
    /// so it is best to run this with dryRun set to true before running it without so you know what migrations
    /// it will revert.
    ///
    /// For a migration to be removed it has to have been registered either using ``DatabaseMigrations.apply(_:)``
    /// or ``DatabaseMigrations.register(_:)``.
    ///
    /// - Parameters:
    ///   - client: Postgres client
    ///   - groups: Migration groups to revert, an empty array means all groups
    ///   - logger: Logger to use
    ///   - dryRun: Should migrations actually be reverted, or should we just report what would be reverted
    public func revertInconsistent(
        client: PostgresClient,
        groups: [DatabaseMigrationGroup] = [],
        logger: Logger,
        dryRun: Bool
    ) async throws {
        let repository = PostgresMigrationRepository(client: client)
        do {
            let migrations = self.migrations
            // build map of registered migrations
            let registeredMigrations = {
                var registeredMigrations = self.reverts
                for migration in migrations {
                    registeredMigrations[migration.name] = migration
                }
                return registeredMigrations
            }()
            // setup migration repository (create table)
            _ = try await repository.setup(client: client, logger: logger)
            // get migrations currently applied in the order they were applied
            let appliedMigrations = try await repository.getAll(client: client, logger: logger)

            var migrationsToRevert: [DatabaseMigration] = .init()
            // if groups array passed in is empty then work out list of migration groups by combining
            // list of groups from migrations and applied migrations
            let groups =
                groups.count == 0
                ? (migrations.map(\.group) + appliedMigrations.map(\.group)).uniqueElements
                : groups
            // for each group revert migrations
            for group in groups {
                let groupMigrations = migrations.filter { $0.group == group }
                let appliedGroupMigrations = appliedMigrations.filter { $0.group == group }

                let minMigrationCount = min(groupMigrations.count, appliedGroupMigrations.count)
                var i = 0
                // while migrations and applied migrations are the same
                while i < minMigrationCount,
                    appliedGroupMigrations[i].name == groupMigrations[i].name
                {
                    i += 1
                }
                // Revert migrations in reverse
                for j in (i..<appliedGroupMigrations.count).reversed() {
                    let migrationName = appliedGroupMigrations[j].name
                    // look for migration to revert in registered migration list and revert dictionary.
                    guard let migration = registeredMigrations[migrationName]
                    else {
                        logger.error("Failed to find migration \(migrationName)")
                        throw DatabaseMigrationError.cannotRevertMigration
                    }
                    migrationsToRevert.append(migration)
                    logger.info("Reverting \(migrationName) from group \(group.name) \(dryRun ? " (dry run)" : "")")
                }
            }
            if dryRun {
                if migrationsToRevert.count > 0 {
                    throw DatabaseMigrationError.requiresChanges
                }
            } else if migrationsToRevert.count > 0 {
                _ = try await repository.withTransaction(logger: logger) { context in
                    for migration in migrationsToRevert {
                        try await migration.revert(
                            connection: context.connection,
                            logger: context.logger
                        )
                        try await repository.remove(migration, context: context)
                    }
                }
            }
        } catch {
            self.setFailed(error)
            throw error
        }
    }

    /// Report if the migration process has completed
    public func waitUntilCompleted() async throws {
        switch self.state {
        case .waiting(var continuations):
            return try await withCheckedThrowingContinuation { cont in
                continuations.append(cont)
                self.state = .waiting(continuations)
            }
        case .completed:
            return
        case .failed(let error):
            throw error
        }
    }

    func setCompleted() {
        switch self.state {
        case .waiting(let continuations):
            for cont in continuations {
                cont.resume()
            }
            self.state = .completed
        case .completed:
            break
        case .failed:
            preconditionFailure("Cannot set it has completed after having set it has failed")
        }
    }

    func setFailed(_ error: Error) {
        switch self.state {
        case .waiting(let continuations):
            for cont in continuations {
                cont.resume(throwing: error)
            }
            self.state = .failed(error)
        case .completed:
            preconditionFailure("Cannot set it has failed after having set it has completed")
        case .failed(let error):
            self.state = .failed(error)
        }
    }

    /// verify migration list doesnt have duplicates
    func checkForDuplicates(logger: Logger) throws {
        var foundDuplicates = false
        let groups = migrations.map(\.group).uniqueElements
        for group in groups {
            let groupMigrations = self.migrations.filter { $0.group == group }
            let groupMigrationSet = Set(groupMigrations.map(\.name))
            guard groupMigrationSet.count != groupMigrations.count else {
                continue
            }
            foundDuplicates = true
            for index in 0..<groupMigrations.count {
                if groupMigrations[(index + 1)...].first(where: { $0.name == groupMigrations[index].name }) != nil {
                    logger.error("Database migration \(groupMigrations[index].name) has been added twice.")
                }
            }
        }
        if foundDuplicates {
            throw DatabaseMigrationError.dupicateNames
        }
    }

    nonisolated func printMigrationComparison(expected expectedList: [String], applied appliedList: [String], logger: Logger) {
        let maxLength = max((expectedList.max { $0.count > $1.count }?.count ?? 0) + 4, 13)
        func printLine(expected: String, applied: String) {
            let gap = String(repeating: " ", count: maxLength - expected.count)
            logger.error("\(expected)\(gap)\(applied)")
        }

        var expectedIndex = 0
        var appliedIndex = 0
        printLine(expected: "Expected:", applied: "Applied:")
        while true {
            let expected = expectedIndex < expectedList.count ? expectedList[expectedIndex] : ""
            var applied = appliedIndex < appliedList.count ? appliedList[appliedIndex] : ""

            if expected == "" && applied == "" {
                break
            }
            if applied != "" {
                if expected != applied {
                    applied += " ❌"
                }
            }
            printLine(expected: expected, applied: applied)

            expectedIndex += 1
            appliedIndex += 1
        }
    }
}

/// Create, remove and list migrations
struct PostgresMigrationRepository: Sendable {
    struct Context: Sendable {
        let connection: PostgresConnection
        let logger: Logger
    }

    let client: PostgresClient
    #if compiler(>=6.0)
    func withTransaction<Value: Sendable>(
        logger: Logger,
        isolation: isolated (any Actor)? = #isolation,
        _ process: (Context) async throws -> Value
    ) async throws -> Value {
        try await self.client.withTransaction(logger: logger) { connection in
            try await process(.init(connection: connection, logger: logger))
        }
    }
    #else
    func withTransaction<Value: Sendable>(
        logger: Logger,
        _ process: (Context) async throws -> Value
    ) async throws -> Value {
        try await self.client.withTransaction(logger: logger) { connection in
            try await process(.init(connection: connection, logger: logger))
        }
    }
    #endif

    func setup(client: PostgresClient, logger: Logger) async throws {
        try await self.createMigrationsTable(client: client, logger: logger)
    }

    func add(_ migration: DatabaseMigration, context: Context) async throws {
        try await context.connection.query(
            "INSERT INTO _hb_pg_migrations (\"name\", \"group\") VALUES (\(migration.name), \(migration.group.name))",
            logger: context.logger
        )
    }

    func remove(_ migration: DatabaseMigration, context: Context) async throws {
        try await context.connection.query(
            "DELETE FROM _hb_pg_migrations WHERE name = \(migration.name)",
            logger: context.logger
        )
    }

    func getAll(client: PostgresClient, logger: Logger) async throws -> [(name: String, group: DatabaseMigrationGroup)] {
        let stream = try await client.query(
            "SELECT \"name\", \"group\" FROM _hb_pg_migrations ORDER BY \"order\"",
            logger: logger
        )
        var result: [(String, DatabaseMigrationGroup)] = []
        for try await (name, group) in stream.decode((String, String).self, context: .default) {
            result.append((name, .init(group)))
        }
        return result
    }

    private func createMigrationsTable(client: PostgresClient, logger: Logger) async throws {
        try await client.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_pg_migrations (
                "order" SERIAL PRIMARY KEY,
                "name" text, 
                "group" text
            )
            """,
            logger: logger
        )
    }
}

extension Array where Element: Equatable {
    /// The list of unique elements in the list, in the order they are found
    var uniqueElements: [Element] {
        self.reduce([]) { result, name in
            if result.first(where: { $0 == name }) == nil {
                var result = result
                result.append(name)
                return result
            }
            return result
        }
    }
}
