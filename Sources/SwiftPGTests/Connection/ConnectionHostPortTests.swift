import NIO
import Testing

@testable import SwiftPG

final class ConnectionHostPortTests {
    @Test(arguments: getHostTrustConnectionConfigsList())
    func connectSuccessHostTrust(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: getHostTrustConnectionConfigsList())
    func connectFailureInvalidHost(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        var configs = configs
        guard case .hostPort(_, let port) = configs.socketAddress else {
            Issue.record("Invalid socket address")
            return
        }
        configs.socketAddress = .hostPort(host: "invalid_host", port: port)

        await #expect(throws: NIO.SocketAddressError.self) {
            try await conn.connect(configs: configs)
        }
    }

    @Test(arguments: getHostTrustConnectionConfigsList())
    func connectFailureInvalidPort(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        var configs = configs
        guard case .hostPort(let host, _) = configs.socketAddress else {
            Issue.record("Invalid socket address")
            return
        }

        configs.socketAddress = .hostPort(host: host, port: 99999)

        // SocketAddressError on MacOS, IOError on Linux
        await #expect(throws: Error.self) {
            try await conn.connect(configs: configs)
        }
    }
}
