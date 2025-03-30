import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLQueryTest {
  @Test func testConnectionSasl() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.batchQuery(
      "SELECT $1::int8;",
      [
        [1],
        [2],
        [3],
      ])

    let values = rows.map {
      row in
      let value = try row.decode(Int64.self)
      return value
    }
    var iterator = values.makeAsyncIterator()

    #expect(try await iterator.next() == 1)
    #expect(try await iterator.next() == 2)
    #expect(try await iterator.next() == 3)
    #expect(try await iterator.next() == nil)
  }
}
