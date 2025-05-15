// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swiftSettings: [SwiftSetting] = [.enableExperimentalFeature("StrictConcurrency=complete")]

let package = Package(
    name: "hummingbird-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "HummingbirdPostgres", targets: ["HummingbirdPostgres"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.5.0"),
        .package(url: "https://github.com/hummingbird-project/postgres-migrations", from: "0.1.0"),
        .package(url: "https://github.com/vapor/postgres-nio", from: "1.25.0"),
    ],
    targets: [
        .target(
            name: "HummingbirdPostgres",
            dependencies: [
                .product(name: "PostgresMigrations", package: "postgres-migrations"),
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "HummingbirdPostgresTests",
            dependencies: [
                "HummingbirdPostgres",
                .product(name: "HummingbirdTesting", package: "hummingbird"),
            ]
        ),
    ],
    swiftLanguageVersions: [.v5, .version("6")]
)
