// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "hummingbird-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "HummingbirdPostgres", targets: ["HummingbirdPostgres"]),
        .library(name: "PostgresMigrations", targets: ["PostgresMigrations"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.0.0"),
        .package(url: "https://github.com/vapor/postgres-nio", from: "1.21.0"),
    ],
    targets: [
        .target(
            name: "HummingbirdPostgres",
            dependencies: [
                "PostgresMigrations",
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .target(
            name: "PostgresMigrations",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .testTarget(
            name: "HummingbirdPostgresTests",
            dependencies: [
                "HummingbirdPostgres",
                .product(name: "HummingbirdTesting", package: "hummingbird"),
            ]
        ),
        .testTarget(
            name: "PostgresMigrationsTests",
            dependencies: [
                "PostgresMigrations",
            ]
        ),
    ]
)
