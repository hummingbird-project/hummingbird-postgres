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
@_spi(ConnectionPool) import PostgresNIO

/// Database migration support
public final class HBPostgresMigrations {
    var migrations: [HBPostgresMigration]
    var reverts: [String: HBPostgresMigration]

    public init() {
        self.migrations = []
        self.reverts = [:]
    }

    /// Add migration to list of reverts, that can be applied
    @MainActor
    public func add(_ migration: HBPostgresMigration) {
        self.migrations.append(migration)
    }

    /// Add migration to list of reverts, that can be applied
    @MainActor
    public func add(revert migration: HBPostgresMigration) {
        self.reverts[migration.name] = migration
    }

    /// Apply database migrations
    @_spi(ConnectionPool)
    @MainActor
    public func apply(client: PostgresClient, logger: Logger, dryRun: Bool) async throws {
        try await self.migrate(client: client, migrations: self.migrations, logger: logger, dryRun: dryRun)
    }

    @_spi(ConnectionPool)
    @MainActor
    public func revert(client: PostgresClient, logger: Logger, dryRun: Bool) async throws {
        try await self.migrate(client: client, migrations: [], logger: logger, dryRun: dryRun)
    }

    private func migrate(client: PostgresClient, migrations: [HBPostgresMigration], logger: Logger, dryRun: Bool) async throws {
        let repository = HBPostgresMigrationRepository(client: client)
        do {
            _ = try await repository.withContext(logger: logger) { context in
                // setup migration repository (create table)
                try await repository.setup(context: context)
                var requiresChanges = false
                // get migrations currently applied in the order they were applied
                let appliedMigrations = try await repository.getAll(context: context)
                // work out list of migration groups
                let groups = Array(Set(migrations.map(\.group) + appliedMigrations.map(\.group)))
                // for each group apply/revert migrations
                for group in groups {
                    let groupMigrations = migrations.filter { $0.group == group }
                    let appliedGroupMigrations = appliedMigrations.filter { $0.group == group }

                    let minMigrationCount = min(groupMigrations.count, appliedGroupMigrations.count)
                    var i = 0
                    while i < minMigrationCount, appliedGroupMigrations[i].name == groupMigrations[i].name {
                        i += 1
                    }
                    // Revert deleted migrations, and any migrations after a deleted migration
                    for j in (i..<appliedGroupMigrations.count).reversed() {
                        let migrationName = appliedGroupMigrations[j].name
                        // look for migration to revert in migration list and revert dictionary. NB we are looking in the migration
                        // array belonging to the type, not the one supplied to the function
                        guard let migration = self.migrations.first(where: { $0.name == migrationName }) ?? self.reverts[migrationName] else {
                            throw HBPostgresMigrationError.cannotRevertMigration
                        }
                        logger.info("Reverting \(migration.name)\(dryRun ? " (dry run)" : "")")
                        if !dryRun {
                            try await migration.revert(connection: context.connection, logger: context.logger)
                            try await repository.remove(migration, context: context)
                        } else {
                            requiresChanges = true
                        }
                    }
                    // Apply migration
                    for j in i..<groupMigrations.count {
                        let migration = groupMigrations[j]
                        logger.info("Migrating \(migration.name)\(dryRun ? " (dry run)" : "")")
                        if !dryRun {
                            try await migration.apply(connection: context.connection, logger: context.logger)
                            try await repository.add(migration, context: context)
                        } else {
                            requiresChanges = true
                        }
                    }
                }
                // if changes are required
                guard requiresChanges == false else {
                    throw HBPostgresMigrationError.requiresChanges
                }
            }
        }
    }
}

/// Create, remove and list migrations
struct HBPostgresMigrationRepository {
    struct Context {
        let connection: PostgresConnection
        let logger: Logger
    }

    let client: PostgresClient

    func withContext<Value>(logger: Logger, _ process: (Context) async throws -> Value) async throws -> Value {
        try await self.client.withConnection { connection in
            try await process(.init(connection: connection, logger: logger))
        }
    }

    func setup(context: Context) async throws {
        try await self.createMigrationsTable(connection: context.connection, logger: context.logger)
    }

    func add(_ migration: HBPostgresMigration, context: Context) async throws {
        try await context.connection.query(
            "INSERT INTO _hb_migrations (\"name\", \"group\") VALUES (\(migration.name), \(migration.group.name))",
            logger: context.logger
        )
    }

    func remove(_ migration: HBPostgresMigration, context: Context) async throws {
        try await context.connection.query(
            "DELETE FROM _hb_migrations WHERE name = \(migration.name)",
            logger: context.logger
        )
    }

    func getAll(context: Context) async throws -> [(name: String, group: HBMigrationGroup)] {
        let stream = try await context.connection.query(
            "SELECT \"name\", \"group\" FROM _hb_migrations ORDER BY \"order\"",
            logger: context.logger
        )
        var result: [(String, HBMigrationGroup)] = []
        for try await (name, group) in stream.decode((String, String).self, context: .default) {
            result.append((name, .init(group)))
        }
        return result
    }

    private func createMigrationsTable(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_migrations (
                "order" SERIAL PRIMARY KEY,
                "name" text, 
                "group" text
            )
            """,
            logger: logger
        )
    }
}
