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
public struct HBPostgresMigrationError: Error {
    public let message: String
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
            let storedMigrations = try await repository.getAll(context: context)
            let minMigrationCount = min(migrations.count, storedMigrations.count)
            var i = 0
            while i < minMigrationCount, storedMigrations[i] == migrations[i].name {
                i += 1
            }
            // Revert deleted migrations, and any migrations after a deleted migration
            for j in (i..<storedMigrations.count).reversed() {
                let migrationName = storedMigrations[j]
                // look for migration to revert in migration list and revert dictionary. NB we are looking in the migration
                // array belonging to the type, not the one supplied to the function
                guard let migration = self.migrations.first(where: { $0.name == migrationName }) ?? self.reverts[migrationName] else {
                    throw HBPostgresMigrationError(message: "Cannot find migration \(migrationName) to revert it.")
                }
                logger.info("Reverting \(migration.name)\(dryRun ? " (dry run)" : "")")
                if !dryRun {
                    try await migration.revert(connection: context.connection, logger: context.logger)
                    try await repository.remove(migration, context: context)
                }
            }
            // Apply migration
            for j in i..<migrations.count {
                let migration = migrations[j]
                logger.info("Migrating \(migration.name)\(dryRun ? " (dry run)" : "")")
                if !dryRun {
                    try await migration.apply(connection: context.connection, logger: context.logger)
                    try await repository.add(migration, context: context)
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

    func withContext(logger: Logger, _ process: (Context) async throws -> Void) async throws {
        _ = try await self.client.withConnection { connection in
            try await self.createMigrationsTable(connection: connection, logger: logger)
            try await process(.init(connection: connection, logger: logger))
        }
    }

    func add(_ migration: HBPostgresMigration, context: Context) async throws {
        try await context.connection.query(
            "INSERT INTO _migrations_ (name) VALUES (\(migration.name))",
            logger: context.logger
        )
    }

    func remove(_ migration: HBPostgresMigration, context: Context) async throws {
        try await context.connection.query(
            "DELETE FROM _migrations_ WHERE name = \(migration.name)",
            logger: context.logger
        )
    }

    func getAll(context: Context) async throws -> [String] {
        let stream = try await context.connection.query(
            "SELECT name FROM _migrations_ ORDER BY \"order\"",
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
            CREATE TABLE IF NOT EXISTS _hb_migrations_ (
                "order" SERIAL PRIMARY KEY,
                "name" text 
            )
            """,
            logger: logger
        )
    }
}
