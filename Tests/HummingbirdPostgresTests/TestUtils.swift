import Hummingbird
@_spi(ConnectionPool) import PostgresNIO
import ServiceLifecycle

/// Manage the lifecycle of a PostgresClient
struct PostgresClientService: Service {
    let client: PostgresClient

    func run() async {
        await cancelOnGracefulShutdown {
            await self.client.run()
        }
    }
}

func getPostgresConfiguration() async throws -> PostgresClient.Configuration {
    let env = try await HBEnvironment.shared.merging(with: .dotEnv())
    return .init(
        host: env.get("POSTGRES_HOSTNAME") ?? "localhost",
        port: env.get("POSTGRES_PORT", as: Int.self) ?? 5432,
        username: env.get("POSTGRES_USER") ?? "test_user",
        password: env.get("POSTGRES_PASSWORD") ?? "test_pasword",
        database: env.get("POSTGRES_DB") ?? "test_db",
        tls: .disable
    )
}
