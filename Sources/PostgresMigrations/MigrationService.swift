//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
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
import ServiceLifecycle

/// Service that runs a database migration
public struct DatabaseMigrationService: Service {
    let client: PostgresClient
    let groups: [DatabaseMigrationGroup]
    let migrations: DatabaseMigrations
    let logger: Logger
    let dryRun: Bool

    ///  Initialize DatabaseMigrationService
    /// - Parameters:
    ///   - client: Postgres client
    ///   - migrations: Migrations to apply
    ///   - groups: Migration groups to apply
    ///   - logger: logger
    ///   - dryRun: Is this a dry run
    public init(
        client: PostgresClient,
        migrations: DatabaseMigrations,
        groups: [DatabaseMigrationGroup] = [],
        logger: Logger,
        dryRun: Bool
    ) {
        self.client = client
        self.groups = groups
        self.migrations = migrations
        self.logger = logger
        self.dryRun = dryRun
    }

    public func run() async throws {
        try await self.migrations.apply(client: self.client, groups: self.groups, logger: self.logger, dryRun: self.dryRun)
        try? await gracefulShutdown()
    }
}
