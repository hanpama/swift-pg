import Testing

@testable import SwiftPG

final class WireMessageCodecContractTests {
    @Test func connectThrowsForInvalidBackendMessageLength() async throws {
        let server = ScriptedPostgresServer(
            steps: [.readStartup(write: [PostgresWire.invalidMessageLength()])]
        )
        try await server.start()
        let conn = Connection()

        await #expect(throws: Error.self) {
            try await withTimeout {
                try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))
            }
        }

        #expect(conn.isConnected() == false)
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test func connectThrowsForTruncatedAuthenticationMessage() async throws {
        let server = ScriptedPostgresServer(
            steps: [.readStartup(write: [PostgresWire.truncatedAuthenticationMessage()])]
        )
        try await server.start()
        let conn = Connection()

        await #expect(throws: Error.self) {
            try await withTimeout {
                try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))
            }
        }

        #expect(conn.isConnected() == false)
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test func queryThrowsForMalformedRowDescription() async throws {
        let server = ScriptedPostgresServer(
            steps: startupSuccessScript()
                + describeStatementScript(
                    parameterOids: [],
                    result: .malformedRowDescription
                )
        )
        try await server.start()
        let conn = Connection()
        try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))

        await #expect(throws: Error.self) {
            try await withTimeout {
                _ = try await conn.query("SELECT 1")
            }
        }

        try await conn.close()
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }
}
