import Foundation
import NIO
import NIOSSL  // Needed for error checking
import Testing

@testable import SwiftPG

final class PostgreSQLConnectionAuthorizationTest {
    @Test func testConnectionTrust() async throws {
        let conn = PostgreSQLConnection()
        try await conn.connect(configs: getPlainTrustConnectionConfigs())
        try await conn.execute("SELECT VERSION();")
        // try await conn.close()
    }

    @Test func testConnectionSasl() async throws {
        let conn = PostgreSQLConnection()
        try await conn.connect(configs: getPlainTrustConnectionConfigs())

        try await conn.execute("SELECT VERSION();")
        // try await conn.close()
    }
}
