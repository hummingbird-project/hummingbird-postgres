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

struct CreateJobQueuedAt: PostgresMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            ALTER TABLE _hb_pg_jobs ADD COLUMN queued_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            """,
            logger: logger
        )
        try await connection.query(
            """
            ALTER TABLE _hb_pg_job_queue ADD COLUMN queued_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            " ALTER TABLE _hb_pg_jobs DROP COLUMN queued_at",
            logger: logger
        )
        try await connection.query(
            " ALTER TABLE _hb_pg_job_queue DROP COLUMN queued_at",
            logger: logger
        )
    }

    var name: String { "_Add_QueuedAt_To_Job_Table_" }
    var group: PostgresMigrationGroup { .jobQueue }
}
