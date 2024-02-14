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

/// Protocol for a database migration
///
/// Requires two functions one to apply the database migration and one to revert it.
public protocol HBPostgresMigration {
    /// Apply database migration
    func apply(connection: PostgresConnection, logger: Logger) async throws
    /// Revert database migration
    func revert(connection: PostgresConnection, logger: Logger) async throws
}

extension HBPostgresMigration {
    public var name: String { String(describing: Self.self) }
}

///  Error thrown by migration code
public struct HBPostgresMigrationError: Error, Equatable {
    enum _Internal {
        case requiresChanges
        case cannotRevertMigration
    }

    private let value: _Internal

    private init(_ value: _Internal) {
        self.value = value
    }

    /// The database requires a migration before the application can run
    static var requiresChanges: Self { .init(.requiresChanges) }
    /// Cannot revert a migration as we do not have its details. Add it to the revert list using
    /// HBPostgresMigrations.add(revert:)
    static var cannotRevertMigration: Self { .init(.cannotRevertMigration) }
}

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
        _ = try await repository.withContext(logger: logger) { context in
            // setup migration repository (create table)
            try await repository.setup(context: context)
            var requiresChanges = false
            // get migrations currently applied in the order they were applied
            let appliedMigrations = try await repository.getAll(context: context)
            let minMigrationCount = min(migrations.count, appliedMigrations.count)
            var i = 0
            while i < minMigrationCount, appliedMigrations[i] == migrations[i].name {
                i += 1
            }
            // Revert deleted migrations, and any migrations after a deleted migration
            for j in (i..<appliedMigrations.count).reversed() {
                let migrationName = appliedMigrations[j]
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
            for j in i..<migrations.count {
                let migration = migrations[j]
                logger.info("Migrating \(migration.name)\(dryRun ? " (dry run)" : "")")
                if !dryRun {
                    try await migration.apply(connection: context.connection, logger: context.logger)
                    try await repository.add(migration, context: context)
                } else {
                    requiresChanges = true
                }
            }
            // if changes are required
            guard requiresChanges == false else {
                throw HBPostgresMigrationError.requiresChanges
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
            "INSERT INTO _hb_migrations (name) VALUES (\(migration.name))",
            logger: context.logger
        )
    }

    func remove(_ migration: HBPostgresMigration, context: Context) async throws {
        try await context.connection.query(
            "DELETE FROM _hb_migrations WHERE name = \(migration.name)",
            logger: context.logger
        )
    }

    func getAll(context: Context) async throws -> [String] {
        let stream = try await context.connection.query(
            "SELECT name FROM _hb_migrations ORDER BY \"order\"",
            logger: context.logger
        )
        var result: [String] = []
        for try await name in stream.decode(String.self, context: .default) {
            result.append(name)
        }
        return result
    }

    private func createMigrationsTable(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_migrations (
                "order" SERIAL PRIMARY KEY,
                "name" text 
            )
            """,
            logger: logger
        )
    }
}
