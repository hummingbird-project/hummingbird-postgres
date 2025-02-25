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

///  Error thrown by migration code
public struct DatabaseMigrationError: Error, Equatable {
    enum _Internal {
        case dupicateNames
        case requiresChanges
        case appliedMigrationsInconsistent
        case cannotRevertMigration
    }

    fileprivate let value: _Internal

    fileprivate init(_ value: _Internal) {
        self.value = value
    }

    /// The migration list has duplicate names in it
    public static var dupicateNames: Self { .init(.dupicateNames) }
    /// The database requires a migration before the application can run
    public static var requiresChanges: Self { .init(.requiresChanges) }
    /// Applied migrations are inconsistent with expected list
    public static var appliedMigrationsInconsistent: Self { .init(.appliedMigrationsInconsistent) }
    /// Cannot revert a migration as we do not have its details. Add it to the revert list using
    /// PostgresMigrations.add(revert:)
    public static var cannotRevertMigration: Self { .init(.cannotRevertMigration) }
}

extension DatabaseMigrationError: CustomStringConvertible {
    public var description: String {
        switch self.value {
        case .dupicateNames: "The migration list has duplicate names in it."
        case .requiresChanges: "Database requires changes. Run `migrate` with `dryRun` set to false."
        case .appliedMigrationsInconsistent: "Applied migrations are inconsistent with expected list."
        case .cannotRevertMigration:
            "Cannot revert migration because we don't have its details. Use `PostgresMigrations.register` to register the DatabaseMigration."
        }
    }
}
