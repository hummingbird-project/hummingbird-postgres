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

struct CreateJobQueueMetadata: DatabaseMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_pg_job_queue_metadata (
                key text PRIMARY KEY,
                value bytea
            )
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "DROP TABLE _hb_pg_job_queue_metadata",
            logger: logger
        )
    }

    var name: String { "_Create_JobQueue_Metadata_Table_" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
