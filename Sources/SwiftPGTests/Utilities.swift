import NIO
import NIOSSL

@testable import SwiftPG

func createConnectionTrust() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(
    configs: .init(
      socketAddress: .hostPort(host: "localhost", port: 6450),
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
      socketAddress: .hostPort(host: "localhost", port: 6451),
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
      socketAddress: .hostPort(host: "localhost", port: 6452),
      username: "postgres",
      password: "postgres",
      database: "postgres",
      tls: tlsConfig
    ))

  return conn
}
