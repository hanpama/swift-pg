import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionTest {
  @Test func testConnectionTrust() async throws {
    let connection = try await createConnectionTrust()
    let rows = try await connection.query("SELECT VERSION();")

    for try await row in rows {
      let version: String = try row.get(String.self, at: 0)
      #expect(version.starts(with: "PostgreSQL"))
    }
    try await connection.close()
  }

  @Test func testConnectionSasl() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query("SELECT VERSION();")

    for try await row in rows {
      let version: String = try row.get(at: 0)
      #expect(version.starts(with: "PostgreSQL"))
    }
    try await connection.close()
  }

  @Test func testConnectionTls() async throws {
    let connection = try await createConnectionTLS()

    let rows = try await connection.query("SELECT VERSION();")

    for try await row in rows {
      let version: String = try row.get(at: 0)
      #expect(version.starts(with: "PostgreSQL"))
    }
    try await connection.close()
  }
}
