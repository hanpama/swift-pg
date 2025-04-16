import Foundation
import NIO
import NIOSSL

@testable import SwiftPG

func getInsecureConfigs() -> PostgreSQLConnectionConfigs {
  let host = ProcessInfo.processInfo.environment["PG_INSECURE_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_INSECURE_PORT"] ?? "6450") ?? 6450
  return .init(
    socketAddress: .hostPort(host: host, port: port),
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

func getSecureConfigs() -> PostgreSQLConnectionConfigs {
  let host = ProcessInfo.processInfo.environment["PG_SECURE_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_SECURE_PORT"] ?? "6451") ?? 6451
  return .init(
    socketAddress: .hostPort(host: host, port: port),
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

func getTLSHostPort() -> PostgreSQLConnectionConfigs.SocketAddress {
  let host = ProcessInfo.processInfo.environment["PG_TLS_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_TLS_PORT"] ?? "6452") ?? 6452
  return .hostPort(host: host, port: port)
}

func createTestConnection() async throws -> PostgreSQLConnection {
  let conn = try await PostgreSQLConnection(configs: getSecureConfigs())
  return conn
}
