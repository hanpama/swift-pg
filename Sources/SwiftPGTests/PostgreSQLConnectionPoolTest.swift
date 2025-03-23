import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionPoolTest {
  @Test func testPool() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let connection = PostgreSQLConnectionPool(
      eventLoopGroup: loopGroup,
      configuration: .init(
        host: "localhost",
        port: 6450,
        username: "postgres",
        password: "postgres",
        database: "postgres"
      ),
      maxConnections: 3
    )
  }
}