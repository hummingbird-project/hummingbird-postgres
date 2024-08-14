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
