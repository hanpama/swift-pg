import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionPoolTest {
  @Test func testPool() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let pool = PostgreSQLConnectionPool(
      eventLoopGroup: loopGroup,
      configuration: .init(
        socketAddress: .hostPort(host: "localhost", port: 6451),
        username: "postgres",
        password: "postgres",
        database: "postgres"
      ),
      maxConnections: 3
    )

    let conn1 = try await pool.acquire()
    let conn2 = try await pool.acquire()
    let conn3 = try await pool.acquire()

    // Pool is full
    var conn4: PostgreSQLConnection? = nil
    Task {
      conn4 = try await pool.acquire()
    }
    try await Task.sleep(nanoseconds: 1_000_000)
    #expect(conn4 == nil)

    await pool.release(conn1)

    try await Task.sleep(nanoseconds: 1_000_000)
    #expect(conn4 != nil)
  }
}
