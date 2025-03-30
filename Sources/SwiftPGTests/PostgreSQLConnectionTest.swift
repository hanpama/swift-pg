import Foundation
import NIO
import NIOSSL // Needed for error checking
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

  // --- SSL Mode Tests ---

  @Test func testSSLModeDisableSuccess() async throws {
    // Connect to non-SSL server with sslmode=disable
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    // Use insecure configs which connect to postgres_insecure:6450
    var configs = getInsecureConfigs()
    configs = .init(
        socketAddress: configs.socketAddress,
        username: configs.username,
        password: configs.password,
        database: configs.database,
        sslmode: .disable, // Explicitly set disable
        sslcert: nil, sslkey: nil, sslrootcert: nil, sslcrl: nil
    )
    try await conn.connect(configs: configs)
    // If connect didn't throw, it succeeded.
    try await conn.execute("SELECT 1;") // Simple query to confirm
    try await conn.close()
  }

  @Test func testSSLModeDisableFailure() async throws {
    // Connect to SSL server (postgres_ssl:6452) with sslmode=disable - should fail
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    let configs = getSSLTestConfigs(sslmode: .disable) // Connects to TLS host

    await #expect(throws: NIOSSLError.self) { // Expect an SSL-related error
        try await conn.connect(configs: configs)
    }
    // If connect threw as expected, it failed.
    try await conn.close() // Close should still work even if connect failed
  }

  @Test func testSSLModeRequireSuccess() async throws {
    // Connect to SSL server with sslmode=require
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    let configs = getSSLTestConfigs(sslmode: .require) // Connects to TLS host
    try await conn.connect(configs: configs)
    // If connect didn't throw, it succeeded.
    try await conn.execute("SELECT 1;")
    try await conn.close()
  }

  @Test func testSSLModeVerifyCASuccess() async throws {
    // Connect to SSL server with sslmode=verify-ca and correct root cert
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    // Provide the server's self-signed cert as the root CA cert
    let configs = getSSLTestConfigs(sslmode: .verifyCA, sslrootcert: "./certs/server.crt")
    try await conn.connect(configs: configs)
    // If connect didn't throw, it succeeded.
    try await conn.execute("SELECT 1;")
    try await conn.close()
  }

  @Test func testSSLModeVerifyFullFailureHostnameMismatch() async throws {
    // Connect to SSL server (hostname postgres_ssl) with sslmode=verify-full
    // Hostname 'postgres_ssl' != Cert CN 'localhost' -> should fail
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    let configs = getSSLTestConfigs(sslmode: .verifyFull, sslrootcert: "./certs/server.crt") // host = postgres_ssl

    await #expect(throws: NIOSSLError.self) { // Expect an SSL verification error
        try await conn.connect(configs: configs)
    }
    // If connect threw as expected, it failed.
    try await conn.close()
  }

  @Test func testSSLModeVerifyFullSuccessHostnameMatch() async throws {
    // Connect to SSL server (hostname localhost) with sslmode=verify-full
    // Hostname 'localhost' == Cert CN 'localhost' -> should succeed
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let conn = PostgreSQLConnection(eventLoopGroup: loopGroup)
    // Override host to 'localhost' to match the cert CN
    let configs = getSSLTestConfigs(sslmode: .verifyFull, sslrootcert: "./certs/server.crt", hostOverride: "localhost")
    try await conn.connect(configs: configs)
    // If connect didn't throw, it succeeded.
    try await conn.execute("SELECT 1;")
    try await conn.close()
  }
}
