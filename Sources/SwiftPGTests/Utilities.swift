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

struct TestEnvironment {
    let host: String
    let port: Int
    let socket: String?
    let rootCert: String?
    let hostUnknownCn: String?
}

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

// let pg17Host = ProcessInfo.processInfo.environment["POSTGRES_17_HOST"] ?? "localhost"
// let pg17Port = Int(ProcessInfo.processInfo.environment["POSTGRES_17_PORT"] ?? "6453") ?? 6453
// let pg16Host = ProcessInfo.processInfo.environment["POSTGRES_16_HOST"] ?? "localhost"
// let pg16Port = Int(ProcessInfo.processInfo.environment["POSTGRES_16_PORT"] ?? "6454") ?? 6454

// let postgres17HostPort = ConnectionConfigs.SocketAddress.hostPort(
//     host: ProcessInfo.processInfo.environment["POSTGRES_17_HOST"] ?? "localhost",
//     port: Int(ProcessInfo.processInfo.environment["POSTGRES_17_PORT"] ?? "6453") ?? 6453
// )
// let postgres16HostPort = ConnectionConfigs.SocketAddress.hostPort(
//     host: ProcessInfo.processInfo.environment["POSTGRES_16_HOST"] ?? "localhost",
//     port: Int(ProcessInfo.processInfo.environment["POSTGRES_16_PORT"] ?? "6454") ?? 6454
// )

let testEnvs: [TestEnvironment] = [
    .init(
        host: ProcessInfo.processInfo.environment["POSTGRES_17_HOST"] ?? "localhost",
        port: Int(ProcessInfo.processInfo.environment["POSTGRES_17_PORT"] ?? "6453")!,
        socket: ProcessInfo.processInfo.environment["POSTGRES_17_SOCKET"],
        rootCert: ProcessInfo.processInfo.environment["POSTGRES_17_ROOT_CERT"],
        hostUnknownCn: ProcessInfo.processInfo.environment["POSTGRES_17_HOST_UNKNOWN_CN"],
    ),
    .init(
        host: ProcessInfo.processInfo.environment["POSTGRES_16_HOST"] ?? "localhost",
        port: Int(ProcessInfo.processInfo.environment["POSTGRES_16_PORT"] ?? "6454")!,
        socket: ProcessInfo.processInfo.environment["POSTGRES_16_SOCKET"],
        rootCert: ProcessInfo.processInfo.environment["POSTGRES_16_ROOT_CERT"],
        hostUnknownCn: ProcessInfo.processInfo.environment["POSTGRES_16_HOST_UNKNOWN_CN"],
    ),
]

func getLocalTrustConnectionConfigsList() -> [ConnectionConfigs] {
    let configsList: [ConnectionConfigs?] = testEnvs.map { env in
        if let socket = env.socket {
            return .init(
                socketAddress: .unixDomainSocket(directory: socket, port: env.port),
                username: "local_trust",
                password: "a1~!@#$%^&*()_+",
                sslmode: .disable,
            )
        }
        return nil
    }
    return configsList.compactMap { $0 }
}

func getHostTrustConnectionConfigsList() -> [ConnectionConfigs] {
    return testEnvs.map { env in
        return .init(
            socketAddress: .hostPort(host: env.host, port: env.port),
            username: "host_trust",
            password: "a1~!@#$%^&*()_+",
            sslmode: .disable
        )
    }
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

func createTestConnection() async throws -> Connection {
    let conn = Connection()
    try await conn.connect(configs: getPlainSaslConnectionConfigs())
    return conn
}
