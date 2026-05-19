// swift-tools-version: 6.1.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "SwiftPG",
  platforms: [
    .macOS(.v15)
  ],
  products: [
    // Products define the executables and libraries a package produces, making them visible to other packages.
    .library(
      name: "SwiftPG",
      targets: ["SwiftPG"])
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
    .package(url: "https://github.com/apple/swift-nio-ssl", from: "2.29.3"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-crypto.git", from: "3.12.2"),
    .package(url: "https://github.com/apple/swift-async-algorithms", from: "1.0.3"),
  ],
  targets: [
    .target(
      name: "SwiftPG",
      dependencies: [
        .product(name: "NIO", package: "swift-nio"),
        .product(name: "NIOTLS", package: "swift-nio"),
        .product(name: "NIOSSL", package: "swift-nio-ssl"),
        .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
        .product(name: "Logging", package: "swift-log"),
        .product(name: "Crypto", package: "swift-crypto"),
      ]
    ),
    .testTarget(
      name: "SwiftPGTests",
      dependencies: ["SwiftPG"]
    ),
    .testTarget(
      name: "SwiftPGPublicAPITests",
      dependencies: ["SwiftPG"],
      path: "Sources/SwiftPGPublicAPITests"
    ),
  ]
)
