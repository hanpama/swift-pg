import NIO
import Testing

@testable import SwiftPG

final class ConnectionUnixSocketTests {
    @Test(arguments: [postgres17UnixSocket, postgres16UnixSocket].compactMap { $0 })
    func connectSuccessLocalTrust(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .disable
        )
        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    func connectFailureLocalTrustInvalidPath() async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: .unixDomainSocket(directory: "/invalid/path", port: 5432),
            username: "user_trust",
            sslmode: .disable
        )

        await #expect(throws: IOError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }
}
