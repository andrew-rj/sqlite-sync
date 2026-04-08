// swift-tools-version: 6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "CloudSync",
    platforms: [.macOS(.v11), .iOS(.v11)],
    products: [
        .library(
            name: "CloudSync",
            targets: ["CloudSync"])
    ],
    targets: [
        .binaryTarget(
            name: "CloudSyncBinary",
            url: "https://github.com/sqliteai/sqlite-sync/releases/download/1.0.10/cloudsync-apple-xcframework-1.0.10.zip",
            checksum: "5be814c3c2cd86618af2af4bee43f43f9995671beb5b87a6770973c16399f1de"
        ),
        .target(
            name: "CloudSync",
            dependencies: ["CloudSyncBinary"],
            path: "packages/swift"
        ),
    ]
)
