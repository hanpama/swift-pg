import NIOSSL
import Testing

@testable import SwiftPG

final class TLSIntegrationTests {
    // MARK: - sslmode require
    @Test(arguments: liveTLSEndpoints)
    func connectsWithSSLModeRequire(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .require
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: liveTLSEndpoints)
    func rejectsSSLModeRequireWhenServerDisallowsSSL(
        socketAddress: ConnectionConfigs.SocketAddress
    ) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust_hostnossl",
            sslmode: .require
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        expectDatabaseError(err, .invalidAuthorizationSpecification)
        #expect(conn.isConnected() == false)
    }

    // MARK: - sslmode verify-ca
    @Test(arguments: liveTLSEndpoints)
    func connectsWithSSLModeVerifyCA(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyCA,
            sslrootcert: .file(try requireEnvironment(ROOT_CERT, "ROOT_CERT")),
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: liveTLSEndpoints)
    func rejectsSSLModeVerifyCAWithUnknownRootCA(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyCA,
            sslrootcert: .file(try requireEnvironment(ROOT_CERT_UNKNOWN, "ROOT_CERT_UNKNOWN")),
        )
        await #expect(throws: NIOSSLError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }

    @Test(arguments: liveTLSEndpoints)
    func rejectsSSLModeVerifyCAWithoutRootCert(socketAddress: ConnectionConfigs.SocketAddress)
        async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyCA,
        )
        await #expect(throws: NIOSSLError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }

    // MARK: - sslmode verify-full
    @Test(arguments: liveTLSGoodCNEndpoints)
    func connectsWithSSLModeVerifyFull(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyFull,
            sslrootcert: .file(try requireEnvironment(ROOT_CERT, "ROOT_CERT")),
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: liveTLSBadCNEndpoints)
    func rejectsSSLModeVerifyFullWithBadCN(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyFull,
            sslrootcert: .file(try requireEnvironment(ROOT_CERT, "ROOT_CERT")),
        )
        await #expect(throws: NIOSSLExtraError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }
}
