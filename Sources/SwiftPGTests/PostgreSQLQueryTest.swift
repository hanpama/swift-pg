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

    var values = rows.map { try $0.get(at: 0) as Int }.makeAsyncIterator()

    #expect(try await values.next() == 1)
    #expect(try await values.next() == 2)
    #expect(try await values.next() == 3)
  }
}
