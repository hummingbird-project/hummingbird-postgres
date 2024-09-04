// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swiftSettings: [SwiftSetting] = [.enableExperimentalFeature("StrictConcurrency=complete")]

let package = Package(
    name: "hummingbird-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "HummingbirdPostgres", targets: ["HummingbirdPostgres"]),
        .library(name: "JobsPostgres", targets: ["JobsPostgres"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.0.0-rc.1"),
        .package(url: "https://github.com/thoven87/swift-jobs.git", branch: "stevenson/delayed-jobs"),
        .package(url: "https://github.com/vapor/postgres-nio", from: "1.21.0"),
    ],
    targets: [
        .target(
            name: "HummingbirdPostgres",
            dependencies: [
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            swiftSettings: swiftSettings
        ),
        .target(
            name: "JobsPostgres",
            dependencies: [
                "HummingbirdPostgres",
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "HummingbirdPostgresTests",
            dependencies: [
                "HummingbirdPostgres",
                "JobsPostgres",
                .product(name: "HummingbirdTesting", package: "hummingbird"),
            ],
            swiftSettings: swiftSettings
        ),
    ]
)
