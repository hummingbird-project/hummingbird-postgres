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
    /// - Parameter migration: DatabaseMigration to be applied
    public func add(_ migration: DatabaseMigration) {
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
    /// are migrations in the applied list that don't exist in the desired list then every migration after
    /// the missing migration is reverted. Then every unapplied migration from the desired list is applied.
    ///
    /// This means removing a single migration from the desired list will revert every migration after the
    /// removed migation, changing the order will revert the moved migrations and any migration after.
    ///
    /// As migrating can be a destructive process it is best to run this with `dryRun`` set to true by default
    /// and only run it properly if an error is thrown to indicate a migration is required. But check the list
    /// of reported migrations and reverts before doing this though.
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
        try await self.migrate(
            client: client,
            migrations: self.migrations,
            groups: groups,
            logger: logger,
            completeMigrations: true,
            dryRun: dryRun
        )
    }

    /// Revert database migrations
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
        try await self.migrate(
            client: client,
            migrations: [],
            groups: groups,
            logger: logger,
            completeMigrations: false,
            dryRun: dryRun
        )
    }

    private func migrate(
        client: PostgresClient,
        migrations: [DatabaseMigration],
        groups: [DatabaseMigrationGroup],
        logger: Logger,
        completeMigrations: Bool,
        dryRun: Bool
    ) async throws {
        switch self.state {
        case .completed, .failed:
            self.state = .waiting([])
        case .waiting:
            break
        }
        let repository = PostgresMigrationRepository(client: client)
        do {
            // build map of registered migrations
            let registeredMigrations = {
                var registeredMigrations = self.reverts
                for migration in self.migrations {
                    registeredMigrations[migration.name] = migration
                }
                return registeredMigrations
            }()
            _ = try await repository.withContext(logger: logger) { context in
                // setup migration repository (create table)
                try await repository.setup(context: context)
                var requiresChanges = false
                // get migrations currently applied in the order they were applied
                let appliedMigrations = try await repository.getAll(context: context)
                // if groups array passed in is empty then work out list of migration groups by combining
                // list of groups from migrations and applied migrations
                let groups =
                    groups.count == 0
                    ? (migrations.map(\.group) + appliedMigrations.map(\.group)).uniqueElements
                    : groups
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
                    // Revert deleted migrations, and any migrations after a deleted migration
                    for j in (i..<appliedGroupMigrations.count).reversed() {
                        let migrationName = appliedGroupMigrations[j].name
                        // look for migration to revert in registered migration list and revert dictionary.
                        guard let migration = registeredMigrations[migrationName]
                        else {
                            logger.error("Failed to find migration \(migrationName)")
                            throw DatabaseMigrationError.cannotRevertMigration
                        }
                        logger.info("Reverting \(migrationName) from group \(group.name) \(dryRun ? " (dry run)" : "")")
                        if !dryRun {
                            try await migration.revert(
                                connection: context.connection,
                                logger: context.logger
                            )
                            try await repository.remove(migration, context: context)
                        } else {
                            requiresChanges = true
                        }
                    }
                    // Apply migration
                    for j in i..<groupMigrations.count {
                        let migration = groupMigrations[j]
                        logger.info(
                            "Migrating \(migration.name) from group \(group.name) \(dryRun ? " (dry run)" : "")"
                        )
                        if !dryRun {
                            try await migration.apply(
                                connection: context.connection,
                                logger: context.logger
                            )
                            try await repository.add(migration, context: context)
                        } else {
                            requiresChanges = true
                        }
                    }
                }
                // if changes are required
                guard requiresChanges == false else {
                    throw DatabaseMigrationError.requiresChanges
                }
            }
        } catch {
            self.setFailed(error)
            throw error
        }
        if completeMigrations {
            self.setCompleted()
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
}

/// Create, remove and list migrations
struct PostgresMigrationRepository: Sendable {
    struct Context: Sendable {
        let connection: PostgresConnection
        let logger: Logger
    }

    let client: PostgresClient

    func withContext<Value: Sendable>(logger: Logger, _ process: @Sendable (Context) async throws -> Value) async throws -> Value {
        try await self.client.withConnection { connection in
            try await process(.init(connection: connection, logger: logger))
        }
    }

    func setup(context: Context) async throws {
        try await self.createMigrationsTable(connection: context.connection, logger: context.logger)
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

    func getAll(context: Context) async throws -> [(name: String, group: DatabaseMigrationGroup)] {
        let stream = try await context.connection.query(
            "SELECT \"name\", \"group\" FROM _hb_pg_migrations ORDER BY \"order\"",
            logger: context.logger
        )
        var result: [(String, DatabaseMigrationGroup)] = []
        for try await (name, group) in stream.decode((String, String).self, context: .default) {
            result.append((name, .init(group)))
        }
        return result
    }

    private func createMigrationsTable(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
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
