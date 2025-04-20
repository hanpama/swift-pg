//
import Foundation
import NIO
import NIOConcurrencyHelpers
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionTests {
  @Test func connectSuccessHostPort() async throws {
    // Verify successful connection using host/port.
    let conn = PostgreSQLConnection()
    try await conn.connect(
      configs: .init(
        socketAddress: getPlainTrustHostPort(),
        username: "postgres",
        password: "postgres",
        database: "postgres",
        sslmode: .disable,
        sslcert: nil,
        sslkey: nil,
        sslrootcert: nil,
        sslcrl: nil
      )
    )
    #expect(conn.isConnected())
    conn.close()
  }

  @Test(.enabled(if: getPlainTrustUnixSocket() != nil))
  func connectSuccessUnixSocket() async throws {
    // Verify successful connection using Unix socket.
    let conn = PostgreSQLConnection()
    try await conn.connect(
      configs: .init(
        socketAddress: getPlainTrustUnixSocket()!,
        username: "postgres",
        password: "postgres",
        database: "postgres",
        sslmode: .disable,
        sslcert: nil,
        sslkey: nil,
        sslrootcert: nil,
        sslcrl: nil
      )
    )
    #expect(conn.isConnected())
    conn.close()
  }

  @Test() func connectFailureInvalidCredentials() async throws {
    // Verify connection failure with invalid credentials.
    let conn = PostgreSQLConnection()

    let err = await #expect(throws: DatabaseError.self) {
      try await conn.connect(
        configs: .init(
          socketAddress: getPlainTrustHostPort(),
          username: "invalid_user",
          password: "invalid_password",
          database: "postgres",
          sslmode: .disable,
          sslcert: nil,
          sslkey: nil,
          sslrootcert: nil,
          sslcrl: nil
        )
      )
    }
    
    // #expect(!conn.isConnected())
    // conn.close()
  }
}
