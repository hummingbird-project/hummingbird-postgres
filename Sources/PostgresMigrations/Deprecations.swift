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

// Below is a list of unavailable symbols with the "HB" prefix. These are available
// temporarily to ease transition from the old symbols that included the "HB"
// prefix to the new ones.
//
// This file will be removed before we do a 2.0 release

@_documentation(visibility: internal) @available(*, deprecated, renamed: "DatabaseMigration")
public typealias PostgresMigration = DatabaseMigration
@_documentation(visibility: internal) @available(*, deprecated, renamed: "DatabaseMigrations")
public typealias PostgresMigrations = DatabaseMigrations
@_documentation(visibility: internal) @available(*, deprecated, renamed: "DatabaseMigrationGroup")
public typealias PostgresMigrationGroup = DatabaseMigrationGroup
