import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLTimeoutTest {
  @Test func testTimeout() async throws {
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

    let rows1 = try await connection.query(
      "SELECT pg_sleep(0.5);"
    )
    for try await _ in rows1 {}

    // Timeout
    let rows2 = try await connection.query(
      timeout: .seconds(1),
      "SELECT pg_sleep(10);"
    )
    var err: PostgreSQLError? = nil
    do {
      for try await _ in rows2 {}
    } catch {
      err = error as? PostgreSQLError
    }
    guard case .clientTimeout = err else {
      throw err!
    }

    // Timeout closes the connection
    do {
      let _ = try await connection.query(
        "SELECT pg_sleep(10);"
      )
    } catch {
      err =  error as? PostgreSQLError
    }
    guard case .transportError = err else {
      throw err!
    }
  }
}
