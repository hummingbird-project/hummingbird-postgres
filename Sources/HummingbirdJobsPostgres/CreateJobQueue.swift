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
@_spi(ConnectionPool) import PostgresNIO

struct CreateJobQueue: HBPostgresMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_jobs (
                id uuid PRIMARY KEY,
                job json,
                status smallint
            )     
            """,
            logger: logger
        )
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_job_queue (
                job_id uuid PRIMARY KEY,
                createdAt timestamp with time zone
            )
            """,
            logger: logger
        )
        try await connection.query(
            """
            CREATE INDEX IF NOT EXISTS _hb_job_queueidx 
            ON _hb_job_queue (createdAt ASC)
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "DROP TABLE _hb_jobs",
            logger: logger
        )
        try await connection.query(
            "DROP TABLE _hb_job_queue",
            logger: logger
        )
    }

    var name: String { "_Create_JobQueue_Table_" }
    var group: HBMigrationGroup { .jobQueue }
}

extension HBMigrationGroup {
    /// JobQueue migration group
    public static var jobQueue: Self { .init("_hb_jobqueue") }
}
