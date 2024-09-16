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

struct CreateJobDelay: DatabaseMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE _hb_pg_job_queue ADD COLUMN IF NOT EXISTS delayed_until TIMESTAMP WITH TIME ZONE
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "ALTER TABLE _hb_pg_job_queue DROP COLUMN delayed_until",
            logger: logger
        )
    }

    var name: String { "_Create_JobQueueDelay_Table_" }
    var group: DatabaseMigrationGroup { .jobQueue }
}
