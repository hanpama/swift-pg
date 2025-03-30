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
    tls: nil
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
    tls: nil
  )
}

func getTLSConfigs() -> PostgreSQLConnectionConfigs {
  let host = ProcessInfo.processInfo.environment["PG_TLS_HOST"] ?? "localhost"
  let port = Int(ProcessInfo.processInfo.environment["PG_TLS_PORT"] ?? "6452") ?? 6452
  var tlsConfig = TLSConfiguration.makeClientConfiguration()
  tlsConfig.applicationProtocols = ["postgresql"]
  tlsConfig.certificateVerification = .none

  return .init(
    socketAddress: .hostPort(host: host, port: port),
    username: "postgres",
    password: "postgres",
    database: "postgres",
    tls: tlsConfig
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
