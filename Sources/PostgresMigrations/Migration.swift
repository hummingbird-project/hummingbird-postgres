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
import PostgresNIO

/// Protocol for a database migration
///
/// Requires two functions one to apply the database migration and one to revert it.
public protocol DatabaseMigration: Sendable {
    /// Apply database migration
    func apply(connection: PostgresConnection, logger: Logger) async throws
    /// Revert database migration
    func revert(connection: PostgresConnection, logger: Logger) async throws
    /// DatabaseMigration name
    var name: String { get }
    /// Group migration belongs to
    var group: DatabaseMigrationGroup { get }
}

extension DatabaseMigration {
    /// Default implementaion of name
    public var name: String { String(describing: Self.self) }
    /// Default group is default
    public var group: DatabaseMigrationGroup { .default }
}

/// Group identifier for a group of migrations.
///
/// DatabaseMigrations in one group are treated independently of migrations in other groups. You can add a
/// migration to a group and it will not affect any subsequent migrations not in that group. By default
/// all migrations belong to the ``default`` group.
///
/// To add a migration to a separate group you first need to define the group by adding a static variable
/// to `DatabaseMigrationGroup`.
/// ```
/// extension DatabaseMigrationGroup {
///     public static var `myGroup`: Self { .init("myGroup") }
/// }
/// ```
/// After that set `DatabaseMigration.group` to `.myGroup`.
///
/// Only use a group different from `.default` if you are certain that the database elements you are
/// creating within that group will always be independent of everything else in the database. Groups
/// are useful for libraries that use migrations to setup their database elements.
public struct DatabaseMigrationGroup: Hashable, Equatable, Sendable {
    let name: String

    public init(_ name: String) {
        self.name = name
    }

    public static var `default`: Self { .init("_hb_default") }
}
