import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionTest {
  @Test func testConnectionInsecure() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let connection = try await PostgreSQLConnection.connect(
      eventLoopGroup: loopGroup,
      configuration: .init(
        host: "localhost",
        port: 6450,
        username: "postgres",
        password: "postgres",
        database: "postgres"
      ))
    let rows = try await connection.query("SELECT VERSION();")

    for try await row in rows {
      let version: String = try row.get(String.self, at: 0)
      #expect(version.starts(with: "PostgreSQL"))
    }
  }

  @Test func testConnectionSasl() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let connection = try await PostgreSQLConnection.connect(
      eventLoopGroup: loopGroup,
      configuration: .init(
        host: "localhost",
        port: 6451,
        username: "postgres",
        password: "postgres",
        database: "postgres"
      ))

    let rows = try await connection.query("SELECT VERSION();")

    for try await row in rows {
      let version: String = try row.get(at: 0)
      #expect(version.starts(with: "PostgreSQL"))
    }
  }
}
