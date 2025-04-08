import Foundation
import NIO
import NIOSSL  // Needed for error checking
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionAuthorizationTest {
  @Test func testConnectionTrust() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = try await PostgreSQLConnection(eventLoopGroup: loopGroup, configs: getInsecureConfigs())
    try await conn.execute("SELECT VERSION();")
    try await conn.close()
  }

  @Test func testConnectionSasl() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = try await PostgreSQLConnection(eventLoopGroup: loopGroup, configs: getSecureConfigs())

    try await conn.execute("SELECT VERSION();")
    try await conn.close()
  }
}
