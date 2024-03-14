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
import Foundation
import Hummingbird
import NIOCore
import PostgresNIO

extension PSQLError {
    public var serverError: PostgresError.Code? {
        switch self.code {
        case .server: return self.serverInfo?[.sqlState].map { PostgresError.Code(raw: $0) } ?? nil
        default: return nil
        }
    }
}

/// Fluent driver for persist system for storing persistent cross request key/value pairs
public final class PostgresPersistDriver: PersistDriver {
    struct WrapperObject<Value: Codable>: PostgresCodable, Codable {
        let value: Value

        init(_ value: Value) {
            self.value = value
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.singleValueContainer()
            self.value = try container.decode(Value.self)
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.singleValueContainer()
            try container.encode(self.value)
        }
    }

    let client: PostgresClient
    let logger: Logger
    let tidyUpFrequency: Duration
    let migrations: PostgresMigrations

    /// Initialize FluentPersistDriver
    /// - Parameters:
    ///   - client: Postgres client
    ///   - tidyUpFrequequency: How frequently cleanup expired database entries should occur

    public init(client: PostgresClient, migrations: PostgresMigrations, tidyUpFrequency: Duration = .seconds(600), logger: Logger) async {
        self.client = client
        self.logger = logger
        self.tidyUpFrequency = tidyUpFrequency
        self.migrations = migrations
        await migrations.add(CreatePersistTable())
    }

    /// Create new key. This doesn't check for the existence of this key already so may fail if the key already exists
    public func create(key: String, value: some Codable, expires: Duration?) async throws {
        let expires = expires.map { Date.now + Double($0.components.seconds) } ?? Date.distantFuture
        do {
            try await self.client.query(
                "INSERT INTO _hb_pg_persist (id, data, expires) VALUES (\(key), \(WrapperObject(value)), \(expires))",
                logger: self.logger
            )
        } catch let error as PSQLError {
            if error.serverError == .uniqueViolation {
                throw PersistError.duplicate
            } else {
                throw error
            }
        }
    }

    /// Set value for key.
    public func set(key: String, value: some Codable, expires: Duration?) async throws {
        let expires = expires.map { Date.now + Double($0.components.seconds) } ?? Date.distantFuture
        try await self.client.query(
            """
            INSERT INTO _hb_pg_persist (id, data, expires) VALUES (\(key), \(WrapperObject(value)), \(expires))
            ON CONFLICT (id)
            DO UPDATE SET data = \(WrapperObject(value)), expires = \(expires)
            """,
            logger: self.logger
        )
    }

    /// Get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type) async throws -> Object? {
        let stream = try await self.client.query(
            "SELECT data, expires FROM _hb_pg_persist WHERE id = \(key)",
            logger: self.logger
        )
        guard let (object, expires) = try await stream.decode((WrapperObject<Object>, Date).self)
            .first(where: { _ in true })
        else {
            return nil
        }
        guard expires > .now else { return nil }
        return object.value
    }

    /// Remove key
    public func remove(key: String) async throws {
        try await self.client.query(
            "DELETE FROM _hb_pg_persist WHERE id = \(key)",
            logger: self.logger
        )
    }

    /// tidy up database by cleaning out expired keys
    func tidy() async throws {
        try await self.client.query(
            "DELETE FROM _hb_pg_persist WHERE expires < \(Date.now)",
            logger: self.logger
        )
    }
}

/// Service protocol requirements
extension PostgresPersistDriver {
    public func run() async throws {
        self.logger.info("Waiting for persist driver migrations to complete")
        try await self.migrations.waitUntilCompleted()

        // do an initial tidy to clear out expired values
        self.logger.info("Tidy persist database")
        try await self.tidy()

        let timerSequence = AsyncTimerSequence(
            interval: self.tidyUpFrequency,
            clock: .suspending
        ).cancelOnGracefulShutdown()

        for try await _ in timerSequence {
            try await self.tidy()
        }
    }
}
