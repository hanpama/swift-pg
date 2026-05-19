import Testing

@testable import SwiftPG

final class SSLNegotiationContractTests {
    @Test func connectFailsBeforeStartupWhenServerRejectsSSLRequest() async throws {
        let server = ScriptedPostgresServer(
            steps: [.readSSLRequest(write: [PostgresWire.sslNotSupported()])]
        )
        try await server.start()
        let conn = Connection()

        await #expect(throws: ClientError.self) {
            try await withTimeout {
                try await conn.connect(
                    configs: ConnectionConfigs(
                        socketAddress: server.socketAddress,
                        username: "user",
                        password: "password",
                        sslmode: .require
                    )
                )
            }
        }

        #expect(conn.isConnected() == false)
        #expect(server.receivedMessages == [.sslRequest])
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }
}
