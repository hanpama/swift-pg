import NIO
import Testing

@testable import SwiftPG

final class ConnectionHostPortTests {
    @Test(arguments: [postgres17HostPort, postgres16HostPort].compactMap { $0 })
    func connectSuccessHostTrust(socketAddress: ConnectionConfigs.SocketAddress) async throws {
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

    func connectFailureInvalidHost() async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: .hostPort(host: "invalid_host", port: 5432),
            username: "user_trust",
            sslmode: .disable
        )
        await #expect(throws: NIO.SocketAddressError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort].compactMap { $0 })
    func connectFailureInvalidPort(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        guard case .hostPort(let host, _) = socketAddress else {
            Issue.record("Invalid socket address")
            return
        }
        let configs: ConnectionConfigs = .init(
            socketAddress: .hostPort(host: host, port: 99999),
            username: "user_trust",
            sslmode: .disable
        )
        // SocketAddressError on MacOS, IOError on Linux
        await #expect(throws: Error.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }
}
