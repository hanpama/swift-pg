import NIO
import NIOSSL

@testable import SwiftPG

func createConnectionTrust() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(
    configs: .init(
      socketAddress: .hostPort(host: "postgres_insecure", port: 5432),
      username: "postgres",
      password: "postgres",
      database: "postgres",
      tls: nil
    ))

  return conn
}

func createConnectionSASL() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(
    configs: .init(
      socketAddress: .hostPort(host: "postgres_secure", port: 5432),
      username: "postgres",
      password: "postgres",
      database: "postgres",
      tls: nil
    ))

  return conn
}

func createConnectionTLS() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
  var tlsConfig = TLSConfiguration.makeClientConfiguration()
  tlsConfig.applicationProtocols = ["postgresql"]

  tlsConfig.certificateVerification = .none

  try await conn.connect(
    configs: .init(
      socketAddress: .hostPort(host: "postgres_ssl", port: 5432),
      username: "postgres",
      password: "postgres",
      database: "postgres",
      tls: tlsConfig
    ))

  return conn
}
