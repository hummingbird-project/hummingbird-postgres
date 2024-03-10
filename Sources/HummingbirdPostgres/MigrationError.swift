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
public struct PostgresMigrationError: Error, Equatable {
    enum _Internal {
        case requiresChanges
        case cannotRevertMigration
    }

    private let value: _Internal

    private init(_ value: _Internal) {
        self.value = value
    }

    /// The database requires a migration before the application can run
    static var requiresChanges: Self { .init(.requiresChanges) }
    /// Cannot revert a migration as we do not have its details. Add it to the revert list using
    /// PostgresMigrations.add(revert:)
    static var cannotRevertMigration: Self { .init(.cannotRevertMigration) }
}
