import Hummingbird
import PostgresNIO
import ServiceLifecycle

func getPostgresConfiguration() async throws -> PostgresClient.Configuration {
    let env = try await Environment().merging(with: .dotEnv())
    return .init(
        host: env.get("POSTGRES_HOSTNAME") ?? "localhost",
        port: env.get("POSTGRES_PORT", as: Int.self) ?? 5434,
        username: env.get("POSTGRES_USER") ?? "hummingbird",
        password: env.get("POSTGRES_PASSWORD") ?? "hummingbird",
        database: env.get("POSTGRES_DB") ?? "hummingbird",
        tls: .disable
    )
}
