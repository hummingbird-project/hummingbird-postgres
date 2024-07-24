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

import HummingbirdPostgres
import Logging
import PostgresNIO

struct CreateJobQueue: PostgresMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_pg_job_queue (
                job_id uuid PRIMARY KEY,
                created_at timestamp with time zone
            )
            """,
            logger: logger
        )
        try await connection.query(
            """
            CREATE INDEX IF NOT EXISTS _hb_job_queueidx 
            ON _hb_pg_job_queue(created_at ASC)
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "DROP TABLE _hb_pg_job_queue",
            logger: logger
        )
    }

    var name: String { "_Create_JobQueue_Table_" }
    var group: PostgresMigrationGroup { .jobQueue }
}
