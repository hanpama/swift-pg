import NIOSSL
import Testing

@testable import SwiftPG

final class ConnectionTLSTests {
    // MARK: - sslmode require
    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectSuccessSSLModeRequire(socketAddress: ConnectionConfigs.SocketAddress) async throws {
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

    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectFailureSSLModeRequireNoServerSSL(socketAddress: ConnectionConfigs.SocketAddress)
        async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust_hostnossl",
            sslmode: .require
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        guard case .invalidAuthorizationSpecification = err else {
            Issue.record("Invalid error case: \(String(describing: err))")
            return
        }
        #expect(conn.isConnected() == false)
    }

    // MARK: - sslmode verify-ca
    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectSuccessSSLModeVerifyCA(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyCA,
            sslrootcert: .file(ROOT_CERT!),
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectFailureSSLModeVerifyCAUnknownRootCA(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyCA,
            sslrootcert: .file(ROOT_CERT_UNKNOWN!),
        )
        await #expect(throws: NIOSSLError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }

    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectFailureSSLModeVerifyCANoRootCert(socketAddress: ConnectionConfigs.SocketAddress)
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
    @Test(arguments: [postgres17GoodCnHostPort].compactMap { $0 })
    func connectSuccessSSLModeVerifyFull(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyFull,
            sslrootcert: .file(ROOT_CERT!),
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: [postgres17BadCnHostPort].compactMap { $0 })
    func connectFailureSSLModeVerifyFullBadCN(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .verifyFull,
            sslrootcert: .file(ROOT_CERT!),
        )
        await #expect(throws: NIOSSLExtraError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }
}
