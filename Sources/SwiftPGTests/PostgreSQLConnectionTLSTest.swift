import Foundation
import NIO
import NIOSSL  // Needed for error checking
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionTLSTest {
    @Test func tlsModeDisableSuccessToNonTLSInstance() async throws {
        let conn = PostgreSQLConnection()
        try await conn.connect(
            configs: .init(
                socketAddress: getTlsSaslHostPort(),
                username: "postgres",
                password: "postgres",
                database: "postgres",
                sslmode: .disable,
                sslcert: nil,
                sslkey: nil,
                sslrootcert: nil,
                sslcrl: nil
            ))

        try await conn.execute("SELECT VERSION();")
        try await conn.close()
    }

    // @Test func testConnectionTLSRequire() async throws {
    //   let conn = PostgreSQLConnection()
    //   try await conn.connect(
    //     configs: .init(
    //       socketAddress: getTlsSaslHostPort(),
    //       username: "postgres",
    //       password: "postgres",
    //       database: "postgres",
    //       sslmode: .require,
    //       sslcert: nil,
    //       sslkey: nil,
    //       sslrootcert: nil,
    //       sslcrl: nil
    //     ))

    //   try await conn.execute("SELECT VERSION();")
    //   try await conn.close()
    // }

    // @Test func testConnectionTLSVerifyCA() async throws {
    //   await #expect(throws: NIOSSLError.self) {
    //     let conn = PostgreSQLConnection()
    //     try await conn.connect(
    //       configs: .init(
    //         socketAddress: getTlsSaslHostPort(),
    //         username: "postgres",
    //         password: "postgres",
    //         database: "postgres",
    //         sslmode: .verifyCA,
    //         sslcert: nil,
    //         sslkey: nil,
    //         sslrootcert: nil,
    //         sslcrl: nil
    //       ))
    //   }
    // }
}
