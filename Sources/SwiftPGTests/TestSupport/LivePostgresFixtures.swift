import Foundation

@testable import SwiftPG

private let environment = ProcessInfo.processInfo.environment

enum TestConfigurationError: Error, CustomStringConvertible {
    case missingEnvironment(String)

    var description: String {
        switch self {
        case .missingEnvironment(let name):
            return "Missing required test environment variable: \(name)"
        }
    }
}

let ROOT_CERT = environment["ROOT_CERT"]
let ROOT_CERT_UNKNOWN = environment["ROOT_CERT_UNKNOWN"]
let CLIENT_CERT = environment["CLIENT_CERT"]
let CLIENT_KEY = environment["CLIENT_KEY"]
let CLIENT_CERT_UNKNOWN = environment["CLIENT_CERT_UNKNOWN"]
let CLIENT_KEY_UNKNOWN = environment["CLIENT_KEY_UNKNOWN"]

let postgres17HostPort = hostPort(
    hostEnvironment: "POSTGRES_17_HOST",
    portEnvironment: "POSTGRES_17_PORT",
    defaultHost: "localhost",
    defaultPort: 6453
)

let postgres16HostPort = hostPort(
    hostEnvironment: "POSTGRES_16_HOST",
    portEnvironment: "POSTGRES_16_PORT",
    defaultHost: "localhost",
    defaultPort: 6454
)

let postgres17UnixSocket = unixSocket(
    directoryEnvironment: "POSTGRES_17_SOCKET",
    defaultPort: 5432
)

let postgres16UnixSocket = unixSocket(
    directoryEnvironment: "POSTGRES_16_SOCKET",
    defaultPort: 5432
)

let postgres17GoodCnHostPort = optionalHostPort(
    hostEnvironment: "POSTGRES_17_HOST_GOOD_CN",
    portEnvironment: "POSTGRES_17_PORT"
)

let postgres17BadCnHostPort = optionalHostPort(
    hostEnvironment: "POSTGRES_17_HOST_BAD_CN",
    portEnvironment: "POSTGRES_17_PORT"
)

let postgres16GoodCnHostPort = optionalHostPort(
    hostEnvironment: "POSTGRES_16_HOST_GOOD_CN",
    portEnvironment: "POSTGRES_16_PORT"
)

let postgres16BadCnHostPort = optionalHostPort(
    hostEnvironment: "POSTGRES_16_HOST_BAD_CN",
    portEnvironment: "POSTGRES_16_PORT"
)

let liveHostPortEndpoints = [
    postgres17HostPort,
    postgres16HostPort,
]

let liveUnixSocketEndpoints = [
    postgres17UnixSocket,
    postgres16UnixSocket,
].compactMap { $0 }

let livePlainEndpoints = liveHostPortEndpoints + liveUnixSocketEndpoints

let liveTLSGoodCNEndpoints = [
    postgres17GoodCnHostPort,
    postgres16GoodCnHostPort,
].compactMap { $0 }

let liveTLSBadCNEndpoints = [
    postgres17BadCnHostPort,
    postgres16BadCnHostPort,
].compactMap { $0 }

func requireEnvironment(_ value: String?, _ name: String) throws -> String {
    guard let value else {
        throw TestConfigurationError.missingEnvironment(name)
    }
    return value
}

func liveTrustConnectionConfig(
    socketAddress: ConnectionConfigs.SocketAddress,
    sslmode: ConnectionConfigs.SSLMode = .disable
) -> ConnectionConfigs {
    ConnectionConfigs(
        socketAddress: socketAddress,
        username: "user_trust",
        database: "test",
        sslmode: sslmode
    )
}

func livePasswordConnectionConfig(
    socketAddress: ConnectionConfigs.SocketAddress,
    password: String = "a1~!@#$%^&*()_+",
    sslmode: ConnectionConfigs.SSLMode = .disable
) -> ConnectionConfigs {
    ConnectionConfigs(
        socketAddress: socketAddress,
        username: "user_scram_sha_256",
        password: password,
        database: "test",
        sslmode: sslmode
    )
}

private func hostPort(
    hostEnvironment: String,
    portEnvironment: String,
    defaultHost: String,
    defaultPort: Int
) -> ConnectionConfigs.SocketAddress {
    .hostPort(
        host: environment[hostEnvironment] ?? defaultHost,
        port: Int(environment[portEnvironment] ?? "") ?? defaultPort
    )
}

private func optionalHostPort(
    hostEnvironment: String,
    portEnvironment: String
) -> ConnectionConfigs.SocketAddress? {
    guard let host = environment[hostEnvironment] else {
        return nil
    }
    return .hostPort(
        host: host,
        port: Int(environment[portEnvironment] ?? "") ?? 5432
    )
}

private func unixSocket(
    directoryEnvironment: String,
    defaultPort: Int
) -> ConnectionConfigs.SocketAddress? {
    guard let directory = environment[directoryEnvironment] else {
        return nil
    }
    return .unixDomainSocket(directory: directory, port: defaultPort)
}
