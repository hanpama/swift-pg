import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionTest {
  @Test func testConnectionTrust() async throws {
    let connection = try await createConnectionTrust()
    try await connection.execute("SELECT VERSION();")
    try await connection.close()
  }

  @Test func testConnectionSasl() async throws {
    let connection = try await createConnectionSASL()
    try await connection.execute("SELECT VERSION();")
    try await connection.close()
  }

  @Test func testConnectionTls() async throws {
    let connection = try await createConnectionTLS()
    try await connection.execute("SELECT VERSION();")
    try await connection.close()
  }
}
