import NIO
import Testing

@testable import SwiftPG

final class ConnectionUnixSocketTests {

    @Test(arguments: getLocalTrustConnectionConfigsList())
    func connectSuccessLocalTrust(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: getLocalTrustConnectionConfigsList())
    func connectFailureLocalTrustInvalidPath(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        var configs = configs
        configs.socketAddress = .unixDomainSocket(directory: "/invalid/path", port: 5432)

        await #expect(throws: IOError.self) {
            try await conn.connect(configs: configs)
        }
    }
}
