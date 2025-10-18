// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swiftSettings: [SwiftSetting] = [
    // https://github.com/apple/swift-evolution/blob/main/proposals/0335-existential-any.md
    .enableUpcomingFeature("ExistentialAny"),

    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
    .enableUpcomingFeature("MemberImportVisibility"),

    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0409-access-level-on-imports.md
    .enableUpcomingFeature("InternalImportsByDefault"),
]

let package = Package(
    name: "hummingbird-postgres",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "HummingbirdPostgres", targets: ["HummingbirdPostgres"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.16.0"),
        .package(url: "https://github.com/hummingbird-project/postgres-migrations", from: "1.0.0"),
        .package(url: "https://github.com/vapor/postgres-nio", from: "1.29.0"),
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
    ]
)
