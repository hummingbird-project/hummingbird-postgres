// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "hummingbird-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "HummingbirdPostgres", targets: ["HummingbirdPostgres"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", branch: "2.x.x"),
        .package(url: "https://github.com/vapor/postgres-nio", from: "1.20.0"),
    ],
    targets: [
        .target(
            name: "HummingbirdPostgres",
            dependencies: [
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .target(
            name: "HummingbirdJobsPostgres",
            dependencies: [
                "HummingbirdPostgres",
                .product(name: "HummingbirdJobs", package: "hummingbird"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .testTarget(
            name: "HummingbirdPostgresTests",
            dependencies: [
                "HummingbirdPostgres",
                "HummingbirdJobsPostgres",
                .product(name: "HummingbirdTesting", package: "hummingbird"),
            ]
        ),
    ]
)
