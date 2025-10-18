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
public import PostgresMigrations
import PostgresNIO

struct CreatePersistTable: DatabaseMigration {
    func apply(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            """
            CREATE TABLE IF NOT EXISTS _hb_pg_persist (
                "id" text PRIMARY KEY,
                "data" json NOT NULL,
                "expires" timestamp with time zone NOT NULL
            )
            """,
            logger: logger
        )
    }

    func revert(connection: PostgresConnection, logger: Logger) async throws {
        try await connection.query(
            "DROP TABLE _hb_pg_persist",
            logger: logger
        )
    }

    var name: String { "_Create_Persist_Table_" }
    var group: DatabaseMigrationGroup { .persist }
}

extension DatabaseMigrationGroup {
    /// Persist driver migration group
    public static var persist: Self { .init("_hb_pg_persist") }
}
