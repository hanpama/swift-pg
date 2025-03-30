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
    sslmode: nil,
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
    sslmode: nil,
    sslcert: nil,
    sslkey: nil,
    sslrootcert: nil,
    sslcrl: nil
  )
}

func getTLSConfigs() -> PostgreSQLConnectionConfigs {
  let host = ProcessInfo.processInfo.environment["PG_TLS_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_TLS_PORT"] ?? "6452") ?? 6452

  return .init(
    socketAddress: .hostPort(host: host, port: port),
    username: "postgres",
    password: "postgres",
    database: "postgres",
    sslmode: .require,
    sslcert: nil,
    sslkey: nil,
    sslrootcert: nil,
    sslcrl: nil
  )
}

// New helper function for SSL tests
func getSSLTestConfigs(
  sslmode: PostgreSQLConnectionConfigs.SSLMode,
  sslrootcert: String? = nil,
  hostOverride: String? = nil
) -> PostgreSQLConnectionConfigs {
  let defaultHost = ProcessInfo.processInfo.environment["PG_TLS_HOST"] ?? "localhost"
  let host = hostOverride ?? defaultHost
  let port = Int(ProcessInfo.processInfo.environment["PG_TLS_PORT"] ?? "6452") ?? 6452

  return .init(
    socketAddress: .hostPort(host: host, port: port),
    username: "postgres",
    password: "postgres",
    database: "postgres",
    sslmode: sslmode,
    sslcert: nil, // Client cert not needed for these tests
    sslkey: nil, // Client key not needed for these tests
    sslrootcert: sslrootcert,
    sslcrl: nil // CRL not needed for these tests
  )
}


func createConnectionTrust() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(configs: getInsecureConfigs())

  return conn
}

func createConnectionSASL() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(configs: getSecureConfigs())

  return conn
}

func createConnectionTLS() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(configs: getTLSConfigs())

  return conn
}
