import Foundation
import NIO
import NIOSSL

@testable import SwiftPG

// enum TestPostgreSQLVersion {
//     case postgres17(host: String, port: Int, socket: String)
// }

// func getPostgres17Host() -> String {
//     return ProcessInfo.processInfo.environment["POSTGRES_17_HOST"] ?? "localhost"
// }
// func getPostgres17HostPort() -> ConnectionConfigs.SocketAddress {
//     let host = ProcessInfo.processInfo.environment["POSTGRES_17_HOST"] ?? "localhost"
//     let port = Int(ProcessInfo.processInfo.environment["POSTGRES_17_PORT"] ?? "6450") ?? 6450
//     return .hostPort(host: host, port: port)
// }
// func getPostgres17UnixSocket() -> ConnectionConfigs.SocketAddress {
//     let socket = ProcessInfo.processInfo.environment["POSTGRES_17_SOCKET"]!
//     return .unixDomainSocket(directory: socket, port: 5432)
// }

struct TestEnvironment {
    let host: String
    let port: Int
    let socket: String?
}

let environments: [TestEnvironment] = [
    .init(
        host: ProcessInfo.processInfo.environment["POSTGRES_17_HOST"] ?? "localhost",
        port: Int(ProcessInfo.processInfo.environment["POSTGRES_17_PORT"] ?? "6453")!,
        socket: ProcessInfo.processInfo.environment["POSTGRES_17_SOCKET"]
    )
]

func getLocalTrustConnectionConfigs(_ env: TestEnvironment) -> ConnectionConfigs? {
    guard let socketDir = env.socket else {
        return nil
    }
    return .init(
        socketAddress: .unixDomainSocket(directory: socketDir, port: 5432),
        username: "local_trust",
        sslmode: .disable,
    )
}

func getHostTrustConnectionConfigs(_ env: TestEnvironment) -> ConnectionConfigs {
    return .init(
        socketAddress: .hostPort(host: env.host, port: env.port),
        username: "host_trust",
        sslmode: .disable,
    )
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
        sslcert: nil,
        sslkey: nil,
        sslrootcert: nil,
        sslcrl: nil
    )
}

func getPlainSaslConnectionConfigs() -> ConnectionConfigs {
    return .init(
        socketAddress: getPlainSaslHostPort(),
        username: "postgres",
        password: "postgres",
        database: "postgres",
        sslmode: .disable,
        sslcert: nil,
        sslkey: nil,
        sslrootcert: nil,
        sslcrl: nil
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

func createTestConnection() async throws -> Connection {
    let conn = Connection()
    try await conn.connect(configs: getPlainSaslConnectionConfigs())
    return conn
}
