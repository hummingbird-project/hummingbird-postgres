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

import PostgresNIO

extension PostgresClient {
    func withTransaction<Value>(logger: Logger, _ process: (PostgresConnection) async throws -> Value) async throws -> Value {
        try await withConnection { connection in
            do {
                try await connection.query("BEGIN;", logger: logger)
                let value = try await process(connection)
                try await connection.query("COMMIT;", logger: logger)
                return value
            } catch {
                try await connection.query("ROLLBACK;", logger: logger)
                throw error
            }
        }
    }
}
