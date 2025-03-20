import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLQueryTest {
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

    let rows = try await connection.batchQuery(
      "SELECT $1::int8;",
      [
        [1],
        [2],
        [3],
      ])

    let values = try rows.map { try $0.get(at: 0) as Int }

    #expect(values == [1, 2, 3])
  }
}
