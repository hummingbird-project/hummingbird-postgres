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
import PostgresMigrations
import PostgresNIO

struct CreateJobs: DatabaseMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_pg_jobs (
                id uuid PRIMARY KEY,
                job bytea,
                status smallint,
                lastModified TIMESTAMPTZ DEFAULT NOW()
            )     
            """,
            logger: logger
        )
        try await connection.query(
            """
            CREATE INDEX IF NOT EXISTS _hb_job_status
            ON _hb_pg_jobs(status)
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "DROP TABLE _hb_pg_jobs",
            logger: logger
        )
    }

    var name: String { "_Create_Jobs_Table_" }
    var group: DatabaseMigrationGroup { .jobQueue }
}

extension DatabaseMigrationGroup {
    /// JobQueue migration group
    public static var jobQueue: Self { .init("_hb_jobqueue") }
}
