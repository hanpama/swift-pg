import Foundation
import NIO
import NIOSSL

@testable import SwiftPG

let POSTGRES_17_HOST = ProcessInfo.processInfo.environment["POSTGRES_17_HOST"]
let POSTGRES_17_HOST_GOOD_CN = ProcessInfo.processInfo.environment["POSTGRES_17_HOST_GOOD_CN"]
let POSTGRES_17_HOST_BAD_CN = ProcessInfo.processInfo.environment["POSTGRES_17_HOST_BAD_CN"]
let POSTGRES_17_PORT = ProcessInfo.processInfo.environment["POSTGRES_17_PORT"]
let POSTGRES_17_SOCKET = ProcessInfo.processInfo.environment["POSTGRES_17_SOCKET"]

let POSTGRES_16_HOST = ProcessInfo.processInfo.environment["POSTGRES_16_HOST"]
let POSTGRES_16_HOST_GOOD_CN = ProcessInfo.processInfo.environment["POSTGRES_16_HOST_GOOD_CN"]
let POSTGRES_16_HOST_BAD_CN = ProcessInfo.processInfo.environment["POSTGRES_16_HOST_BAD_CN"]
let POSTGRES_16_PORT = ProcessInfo.processInfo.environment["POSTGRES_16_PORT"]
let POSTGRES_16_SOCKET = ProcessInfo.processInfo.environment["POSTGRES_16_SOCKET"]

let ROOT_CERT = ProcessInfo.processInfo.environment["ROOT_CERT"]
let ROOT_CERT_UNKNOWN = ProcessInfo.processInfo.environment["ROOT_CERT_UNKNOWN"]
let CLIENT_CERT = ProcessInfo.processInfo.environment["CLIENT_CERT"]
let CLIENT_KEY = ProcessInfo.processInfo.environment["CLIENT_KEY"]
let CLIENT_CERT_UNKNOWN = ProcessInfo.processInfo.environment["CLIENT_CERT_UNKNOWN"]
let CLIENT_KEY_UNKNOWN = ProcessInfo.processInfo.environment["CLIENT_KEY_UNKNOWN"]

struct TestEnvironment {
    let host: String
    let port: Int
    let socket: String?
    let rootCert: String?
    let hostUnknownCn: String?
}

// Dedicated EventLoopGroup for tests to avoid singleton issues
let testEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

// Setup signal handling to ensure proper cleanup
private class TestCleanup {
    static let shared = TestCleanup()
    private var hasShutdown = false
    
    init() {
        signal(SIGTERM) { _ in TestCleanup.shared.shutdown() }
        signal(SIGINT) { _ in TestCleanup.shared.shutdown() }
        atexit { TestCleanup.shared.shutdown() }
    }
    
    func shutdown() {
        guard !hasShutdown else { return }
        hasShutdown = true
        try? testEventLoopGroup.syncShutdownGracefully()
    }
}

// Initialize cleanup handler
private let _ = TestCleanup.shared

let postgres17HostPort =
    if let host = POSTGRES_17_HOST, let port = POSTGRES_17_PORT {
        ConnectionConfigs.SocketAddress.hostPort(host: host, port: Int(port) ?? 6453)
    } else {
        ConnectionConfigs.SocketAddress.hostPort(host: "localhost", port: 6453)
    }

let postgres16HostPort =
    if let host = POSTGRES_16_HOST, let port = POSTGRES_16_PORT {
        ConnectionConfigs.SocketAddress.hostPort(host: host, port: Int(port) ?? 6454)
    } else {
        ConnectionConfigs.SocketAddress.hostPort(host: "localhost", port: 6454)
    }

let postgres17UnixSocket: ConnectionConfigs.SocketAddress? =
    if let socket = POSTGRES_17_SOCKET {
        ConnectionConfigs.SocketAddress.unixDomainSocket(directory: socket, port: 5432)
    } else {
        nil
    }

let postgres16UnixSocket: ConnectionConfigs.SocketAddress? =
    if let socket = POSTGRES_16_SOCKET {
        ConnectionConfigs.SocketAddress.unixDomainSocket(directory: socket, port: 5432)
    } else {
        nil
    }

let postgres17GoodCnHostPort: ConnectionConfigs.SocketAddress? =
    if let host = POSTGRES_17_HOST_GOOD_CN {
        ConnectionConfigs.SocketAddress.hostPort(host: host, port: Int(POSTGRES_17_PORT!)!)
    } else {
        nil
    }
let postgres17BadCnHostPort: ConnectionConfigs.SocketAddress? =
    if let host = POSTGRES_17_HOST_BAD_CN {
        ConnectionConfigs.SocketAddress.hostPort(host: host, port: Int(POSTGRES_17_PORT!)!)
    } else {
        nil
    }
let postgres16GoodCnHostPort: ConnectionConfigs.SocketAddress? =
    if let host = POSTGRES_16_HOST_GOOD_CN {
        ConnectionConfigs.SocketAddress.hostPort(host: host, port: Int(POSTGRES_16_PORT!)!)
    } else {
        nil
    }
let postgres16BadCnHostPort: ConnectionConfigs.SocketAddress? =
    if let host = POSTGRES_16_HOST_BAD_CN {
        ConnectionConfigs.SocketAddress.hostPort(host: host, port: Int(POSTGRES_16_PORT!)!)
    } else {
        nil
    }

// func

// local_trust
// local_reject
// local_scram_sha_256
// host_trust
// host_reject
// host_scram_sha_256
// hostssl_trust
// hostssl_reject
// hostssl_scram_sha_256
// hostssl_clientcert_verify_ca
// hostssl_clientcert_verify_full
// hostnossl_trust
// hostnossl_reject
// hostnossl_scram_sha_256

func getPlainTrustConnectionConfigs() -> ConnectionConfigs {
    return .init(
        socketAddress: getPlainTrustHostPort(),
        username: "postgres",
        password: "postgres",
        database: "postgres",
        sslmode: .disable,
    )
}

func getPlainSaslConnectionConfigs() -> ConnectionConfigs {
    return .init(
        socketAddress: getPlainSaslHostPort(),
        username: "postgres",
        password: "postgres",
        database: "postgres",
        sslmode: .disable,
    )
}

func getPlainTrustHost() -> String {
    return ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_HOST"] ?? "localhost"
}

func getPlainTrustHostPort() -> ConnectionConfigs.SocketAddress {
    let host = ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_HOST"] ?? "localhost"
    let port = Int(ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_PORT"] ?? "6450") ?? 6450
    return .hostPort(host: host, port: port)
}

func getPlainTrustUnixSocket() -> ConnectionConfigs.SocketAddress? {
    guard let socket = ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_UNIX_SOCKET_DIR"] else {
        return nil
    }
    return .unixDomainSocket(directory: socket, port: 5432)
}

func getPlainSaslHostPort() -> ConnectionConfigs.SocketAddress {
    let host = ProcessInfo.processInfo.environment["PG_PLAIN_SASL_HOST"] ?? "localhost"
    let port = Int(ProcessInfo.processInfo.environment["PG_PLAIN_SASL_PORT"] ?? "6451") ?? 6451
    return .hostPort(host: host, port: port)
}

func getPlainSaslUnixSocket() -> ConnectionConfigs.SocketAddress? {
    guard let socket = ProcessInfo.processInfo.environment["PG_PLAIN_SASL_UNIX_SOCKET_DIR"] else {
        return nil
    }
    return .unixDomainSocket(directory: socket, port: 5432)
}

func getTlsSaslHostPort() -> ConnectionConfigs.SocketAddress {
    let host = ProcessInfo.processInfo.environment["PG_TLS_SASL_HOST"] ?? "localhost"
    let port = Int(ProcessInfo.processInfo.environment["PG_TLS_SASL_PORT"] ?? "6452") ?? 6452
    return .hostPort(host: host, port: port)
}

// Shared EventLoopGroup for tests that will be shut down properly
func createTestConnection() async throws -> Connection {
    let conn = Connection(eventLoopGroup: testEventLoopGroup)
    try await conn.connect(configs: getPlainSaslConnectionConfigs())
    return conn
}
