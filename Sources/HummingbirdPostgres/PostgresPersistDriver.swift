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
@_spi(ConnectionPool) import PostgresNIO

extension PSQLError {
    public var serverError: PostgresError.Code? {
        switch self.code {
        case .server: return self.serverInfo?[.sqlState].map { PostgresError.Code(raw: $0) } ?? nil
        default: return nil
        }
    }
}

/// Fluent driver for persist system for storing persistent cross request key/value pairs
public final class HBPostgresPersistDriver: HBPersistDriver {
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

    /// Initialize HBFluentPersistDriver
    /// - Parameters:
    ///   - client: Postgres client
    ///   - tidyUpFrequequency: How frequently cleanup expired database entries should occur
    @_spi(ConnectionPool)
    public init(client: PostgresClient, tidyUpFrequency: Duration = .seconds(600), logger: Logger) {
        self.client = client
        self.logger = logger
        self.tidyUpFrequency = tidyUpFrequency
    }

    /// Create new key. This doesn't check for the existence of this key already so may fail if the key already exists
    public func create(key: String, value: some Codable, expires: Duration?) async throws {
        let expires = expires.map { Date.now + Double($0.components.seconds) } ?? Date.distantFuture
        try await self.client.withConnection { connection in
            do {
                _ = try await connection.execute(CreateStatement(key: key, value: value, expires: expires), logger: self.logger)
            } catch let error as PSQLError {
                if error.serverError == .uniqueViolation {
                    throw HBPersistError.duplicate
                } else {
                    throw error
                }
            }
        }
    }

    /// Set value for key.
    public func set(key: String, value: some Codable, expires: Duration?) async throws {
        let expires = expires.map { Date.now + Double($0.components.seconds) } ?? Date.distantFuture
        _ = try await self.client.withConnection { connection in
            _ = try await connection.execute(SetStatement(key: key, value: value, expires: expires), logger: self.logger)
        }
    }

    /// Get value for key
    public func get<Object: Codable>(key: String, as object: Object.Type) async throws -> Object? {
        try await self.client.withConnection { connection in
            let rows = try await connection.execute(GetStatement(key: key), logger: self.logger)
            guard let row = try await rows.first(where: { _ in true }) else { return nil }
            guard row.1 > .now else { return nil }
            return try JSONDecoder().decode(Object.self, from: row.0)
        }
    }

    /// Remove key
    public func remove(key: String) async throws {
        _ = try await self.client.withConnection { connection in
            _ = try await connection.execute(DeleteRowStatement(key: key), logger: self.logger)
        }
    }

    /// tidy up database by cleaning out expired keys
    func tidy() async throws {
        _ = try await self.client.withConnection { connection in
            _ = try await connection.execute(DeleteExpiredStatement(), logger: self.logger)
        }
    }
}

extension HBPostgresPersistDriver {
    struct CreateStatement: PostgresPreparedStatement {
        typealias Row = Int

        let key: String
        let value: Data
        let expires: Date

        static let sql = "INSERT INTO _hb_psql_persist (id, data, expires) VALUES ($1, $2, $3)"

        init(key: String, value: some Encodable, expires: Date) throws {
            self.key = key
            self.value = try JSONEncoder().encode(value)
            self.expires = expires
        }

        func makeBindings() throws -> PostgresNIO.PostgresBindings {
            var bindings = PostgresNIO.PostgresBindings()
            bindings.append(.init(string: self.key))
            bindings.append(.init(json: self.value))
            bindings.append(.init(date: self.expires))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row { try row.decode(Row.self) }
    }

    struct SetStatement: PostgresPreparedStatement {
        typealias Row = Int

        let key: String
        let value: Data
        let expires: Date

        static let sql = """
        INSERT INTO _hb_psql_persist (id, data, expires) VALUES ($1, $2, $3)
        ON CONFLICT (id)
        DO UPDATE SET data = $2, expires = $3
        """

        init(key: String, value: some Encodable, expires: Date) throws {
            self.key = key
            self.value = try JSONEncoder().encode(value)
            self.expires = expires
        }

        func makeBindings() throws -> PostgresNIO.PostgresBindings {
            var bindings = PostgresNIO.PostgresBindings()
            bindings.append(.init(string: self.key))
            bindings.append(.init(json: self.value))
            bindings.append(.init(date: self.expires))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row { try row.decode(Row.self) }
    }

    struct GetStatement: PostgresPreparedStatement {
        typealias Row = (Data, Date)

        let key: String
        static var sql = "SELECT data, expires FROM _hb_psql_persist WHERE id = $1"

        func makeBindings() throws -> PostgresNIO.PostgresBindings {
            var bindings = PostgresNIO.PostgresBindings()
            bindings.append(.init(string: self.key))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row { try row.decode(Row.self) }
    }

    struct DeleteRowStatement: PostgresPreparedStatement {
        typealias Row = Int

        let key: String
        static var sql = "DELETE FROM _hb_psql_persist WHERE id = $1"

        func makeBindings() throws -> PostgresNIO.PostgresBindings {
            var bindings = PostgresNIO.PostgresBindings()
            bindings.append(.init(string: self.key))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row { try row.decode(Row.self) }
    }

    struct DeleteExpiredStatement: PostgresPreparedStatement {
        typealias Row = Int

        static var sql = "DELETE FROM _hb_psql_persist WHERE expires < $1"

        func makeBindings() throws -> PostgresNIO.PostgresBindings {
            var bindings = PostgresNIO.PostgresBindings()
            bindings.append(.init(date: .now))
            return bindings
        }

        func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row { try row.decode(Row.self) }
    }
}

/// Service protocol requirements
extension HBPostgresPersistDriver {
    public func run() async throws {
        // create table to save persist models
        _ = try await self.client.withConnection { connection in
            try await connection.query(
                """
                CREATE TABLE IF NOT EXISTS _hb_psql_persist (
                    "id" text PRIMARY KEY,
                    "data" json NOT NULL,
                    "expires" timestamp with time zone NOT NULL
                )
                """,
                logger: self.logger
            )
        }
        // do an initial tidy to clear out expired values
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
