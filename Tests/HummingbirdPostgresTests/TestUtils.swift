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

import Hummingbird
import PostgresNIO
import ServiceLifecycle

func getPostgresConfiguration() async throws -> PostgresClient.Configuration {
    let env = try await Environment().merging(with: .dotEnv())
    return .init(
        host: env.get("POSTGRES_HOSTNAME") ?? "localhost",
        port: env.get("POSTGRES_PORT", as: Int.self) ?? 5432,
        username: env.get("POSTGRES_USER") ?? "test_user",
        password: env.get("POSTGRES_PASSWORD") ?? "test_password",
        database: env.get("POSTGRES_DB") ?? "test_db",
        tls: .disable
    )
}
