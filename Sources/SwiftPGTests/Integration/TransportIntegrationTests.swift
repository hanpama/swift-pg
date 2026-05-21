import NIO
import Testing

@testable import SwiftPG

final class TransportIntegrationTests {
    @Test(arguments: liveHostPortEndpoints)
    func connectsOverHostPort(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        try await conn.connect(configs: liveTrustConnectionConfig(socketAddress: socketAddress))
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test func failsForInvalidHost() async throws {
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

    @Test(arguments: liveHostPortEndpoints)
    func failsForInvalidPort(socketAddress: ConnectionConfigs.SocketAddress) async throws {
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

    @Test(arguments: liveUnixSocketEndpoints)
    func connectsOverUnixSocket(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        try await conn.connect(configs: liveTrustConnectionConfig(socketAddress: socketAddress))
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test func failsForInvalidUnixSocketPath() async throws {
        let conn = Connection()
        let configs = liveTrustConnectionConfig(
            socketAddress: .unixDomainSocket(directory: "/invalid/path", port: 5432)
        )

        await #expect(throws: IOError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }
}
