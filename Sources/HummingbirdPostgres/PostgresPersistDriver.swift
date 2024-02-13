//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import AsyncAlgorithms
import FluentKit
import Foundation
import Hummingbird
import NIOCore
import ServiceLifecycle

/// Fluent driver for persist system for storing persistent cross request key/value pairs
public final class HBPostgresPersistDriver: HBPersistDriver {
    let client: PostgresClient
    let logger: Logger
    let tidyUpFrequency: Duration

    /// Initialize HBFluentPersistDriver
    /// - Parameters:
    ///   - client: Postgres client
    ///   - tidyUpFrequequency: How frequently cleanup expired database entries should occur
    public init(client: PostgresClient, tidyUpFrequency: Duration = .seconds(600), logger: Logger) {
        self.client = client
        self.logger = logger
        self.tidyUpFrequency = tidyUpFrequency
        self.tidy()
    }

    /// Create new key. This doesn't check for the existence of this key already so may fail if the key already exists
    public func create<Object: Codable>(key: String, value: Object, expires: Duration?) async throws {
        try await client.withConnection { connection in
            do {
                if let expires {
                    _ = connection.query(
                        "INSERT INTO _hb_psql_persist (id, data, expires) VALUES (\(key), \(value), \(expires))",
                        logger: logger
                    )
                } else {
                    _ = connection.query(
                        "INSERT INTO _hb_psql_persist (id, data) VALUES (\(key), \(value))",
                        logger: logger
                    )
                }
            } catch {
                print(String(reflecting: error))
            }
        }
    }

    /// Set value for key.
    public func set<Object: Codable>(key: String, value: Object, expires: Duration?) async throws {
        try await client.withConnection { connection in
            do {
                try await create(key: key, value: value, expires: expires)
            } catch let error as HBPersistError where error == .duplicate {
                if let expires {
                    _ = connection.query(
                        "UPDATE _hb_psql_persist SET data = \(value), expires = \(expires) WHERE id = \(key)",
                        logger: logger
                    )
                } else {
                    _ = connection.query(
                        "UPDATE _hb_psql_persist SET data = \(value) WHERE id = \(key)",
                        logger: logger
                    )
                }
            }
        }
    }

    /// Get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type) async throws -> Object? {
        try await client.withConnection { connection in
            let stream = connection.query(
                "SELECT data FROM _hb_psql_persist WHERE id = \(key)",
                logger: logger
            )
            return try await stream.decode(Object.self).first { _ in true}
        }
    }

    /// Remove key
    public func remove(key: String) async throws {
        try await client.withConnection { connection in
            _ = connection.query(
                "DELETE FROM _hb_psql_persist WHERE id = \(key)",
                logger: logger
            )
        }
    }

    /// tidy up database by cleaning out expired keys
    func tidy() {
//        _ = PersistModel.query(on: self.fluent.db(self.databaseID))
//            .filter(\.$expires < Date())
//            .delete()
    }
}

/// Service protocol requirements
extension HBFluentPersistDriver {
    public func run() async throws {
        // create table to save persist models
        self.client.withConnection { connection in
            try await connection.query(
                """
                CREATE TABLE IF NOT EXISTS _hb_psql_persisit (
                    "id" text PRIMARY KEY,
                    "data" json not null,
                    "expires" timestamp
                )
                """,
                logger: logger
            )
        }
        let timerSequence = AsyncTimerSequence(interval: self.tidyUpFrequency, clock: .suspending)
            .cancelOnGracefulShutdown()
        for try await _ in timerSequence {
            self.tidy()
        }
    }
}
