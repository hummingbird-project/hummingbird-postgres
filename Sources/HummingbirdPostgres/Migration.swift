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
@_spi(ConnectionPool) import PostgresNIO

/// Protocol for a database migration
///
/// Requires two functions one to apply the database migration and one to revert it.
public protocol HBPostgresMigration {
    /// Apply database migration
    func apply(connection: PostgresConnection, logger: Logger) async throws
    /// Revert database migration
    func revert(connection: PostgresConnection, logger: Logger) async throws
    /// Migration name
    var name: String { get }
    /// Group of migrations
    var group: HBMigrationGroup { get }
}

extension HBPostgresMigration {
    /// Default implementaion of name
    public var name: String { String(describing: Self.self) }
    /// Default group is default
    public var group: HBMigrationGroup { .default }
}

/// Group identifier for a group of migrations
public struct HBMigrationGroup: Hashable, Equatable {
    let name: String

    public init(_ name: String) {
        self.name = name
    }

    public static var `default`: Self { .init("_hb_default") }
}
