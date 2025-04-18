import Foundation
import NIO
import NIOConcurrencyHelpers
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionPoolTest {
  @Test func testPool() async throws {
    let pool = PostgreSQLConnectionPool(
      configuration: getPlainSaslConnectionConfigs(),
      maxConnections: 3
    )

    let conn1 = try await pool.acquire()
    let _ = try await pool.acquire()
    let _ = try await pool.acquire()

    // Pool is full
    var conn4: PostgreSQLConnection? = nil
    Task {
      conn4 = try await pool.acquire()
    }
    try await Task.sleep(nanoseconds: 1_000_000)
    #expect(conn4 == nil)

    await pool.release(conn1)

    try await Task.sleep(nanoseconds: 1_000_000)
    #expect(ObjectIdentifier(conn4!) == ObjectIdentifier(conn1))
  }

  @Test func testConcurrentQuery() async throws {
    let pool = PostgreSQLConnectionPool(
      configuration: getPlainSaslConnectionConfigs(),
      maxConnections: 3
    )

    let executionOrder: NIOLockedValueBox<[String]> = .init([])

    let t1 = Task {
      let rows = try await pool.query("SELECT pg_sleep(0.1)")
      executionOrder.withLockedValue { $0.append("1") }
      for try await _ in rows {}
      executionOrder.withLockedValue { $0.append("2") }
    }

    let t2 = Task {
      let rows = try await pool.query("SELECT pg_sleep(0.1)")
      executionOrder.withLockedValue { $0.append("1") }
      for try await _ in rows {}
      executionOrder.withLockedValue { $0.append("2") }
    }

    let t3 = Task {
      let rows = try await pool.query("SELECT pg_sleep(0.1)")
      executionOrder.withLockedValue { $0.append("1") }
      for try await _ in rows {}
      executionOrder.withLockedValue { $0.append("2") }
    }

    let t4 = Task {
      let rows = try await pool.query("SELECT pg_sleep(0.1)")
      executionOrder.withLockedValue { $0.append("1") }
      for try await _ in rows {}
      executionOrder.withLockedValue { $0.append("2") }
    }

    try await t1.result.get()
    try await t2.result.get()
    try await t3.result.get()
    try await t4.result.get()

    let result = executionOrder.withLockedValue { $0 }

    #expect(result.count == 8)
    #expect(result[...3].contains("2"))
    #expect(result[4...].contains("1"))
  }
}
