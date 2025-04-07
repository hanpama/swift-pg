import Foundation
import NIO
import NIOSSL  // Needed for error checking
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionTLSTest {
  @Test func testConnectionTLSRequire() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    try await conn.connect(
      configs: .init(
        socketAddress: getTLSHostPort(),
        username: "postgres",
        password: "postgres",
        database: "postgres",
        sslmode: .require,
        sslcert: nil,
        sslkey: nil,
        sslrootcert: nil,
        sslcrl: nil
      )
    )
    try await conn.execute("SELECT VERSION();")
    try await conn.close()
  }

  @Test func testConnectionTLSVerifyCA() async throws {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)

    await #expect(throws: NIOSSLError.self) {
      try await conn.connect(
        configs: .init(
          socketAddress: getTLSHostPort(),
          username: "postgres",
          password: "postgres",
          database: "postgres",
          sslmode: .verifyCA,
          sslcert: nil,
          sslkey: nil,
          sslrootcert: nil,
          sslcrl: nil
        )
      )
    }
  }
}
