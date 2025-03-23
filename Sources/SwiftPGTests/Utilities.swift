import NIO

@testable import SwiftPG


func createConnectionTrust() async throws -> PostgreSQLConnection {
  let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

  try await conn.connect(
    configs: .init(
      socketAddress: .hostPort(host: "localhost", port: 6450),
      username: "postgres",
      password: "postgres",
      database: "postgres"
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
      database: "postgres"
    ))

  return conn
}
