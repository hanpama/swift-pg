import Foundation
import NIO
import NIOSSL

@testable import SwiftPG

func getPlainTrustConnectionConfigs() -> PostgreSQLConnectionConfigs {
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

func getPlainSaslConnectionConfigs() -> PostgreSQLConnectionConfigs {
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

func getPlainTrustHostPort() -> PostgreSQLConnectionConfigs.SocketAddress {
  let host = ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_PORT"] ?? "6450") ?? 6450
  return .hostPort(host: host, port: port)
}

func getPlainTrustUnixSocket() -> PostgreSQLConnectionConfigs.SocketAddress? {
  guard let socket = ProcessInfo.processInfo.environment["PG_PLAIN_TRUST_UNIX_SOCKET_DIR"] else {
    return nil
  }
  return .unixDomainSocket(directory: socket, port: 5432)
}

func getPlainSaslHostPort() -> PostgreSQLConnectionConfigs.SocketAddress {
  let host = ProcessInfo.processInfo.environment["PG_PLAIN_SASL_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_PLAIN_SASL_PORT"] ?? "6451") ?? 6451
  return .hostPort(host: host, port: port)
}

func getPlainSaslUnixSocket() -> PostgreSQLConnectionConfigs.SocketAddress? {
  guard let socket = ProcessInfo.processInfo.environment["PG_PLAIN_SASL_UNIX_SOCKET_DIR"] else {
    return nil
  }
  return .unixDomainSocket(directory: socket, port: 5432)
}

func getTlsSaslHostPort() -> PostgreSQLConnectionConfigs.SocketAddress {
  let host = ProcessInfo.processInfo.environment["PG_TLS_SASL_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_TLS_SASL_PORT"] ?? "6452") ?? 6452
  return .hostPort(host: host, port: port)
}

func createTestConnection() async throws -> PostgreSQLConnection {
  let conn = PostgreSQLConnection()
  try await conn.connect(configs: getPlainSaslConnectionConfigs())
  return conn
}
