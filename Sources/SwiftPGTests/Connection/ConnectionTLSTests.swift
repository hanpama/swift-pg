import NIOSSL
import Testing

@testable import SwiftPG

final class ConnectionTLSTests {
    // MARK: - sslmode require
    @Test(
        arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 },
        ["hostssl_trust", "host_trust"]
    )
    func connectSuccessSSLModeRequire(socketAddress: ConnectionConfigs.SocketAddress, username: String) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .require
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(
        arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 },
        ["hostnossl_trust"]
    )
    func connectFailureSSLModeRequireNoServerSSL(socketAddress: ConnectionConfigs.SocketAddress, username: String)
        async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .require
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        guard case .invalidAuthorizationSpecification = err else {
            Issue.record("Expected invalid authorization specification error")
            return
        }
    }

    // MARK: - sslmode verify-ca
    @Test(
        .enabled(if: ROOT_CERT != nil),
        arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 },
        ["hostssl_trust", "host_trust"],
    )
    func connectSuccessSSLModeVerifyCA(socketAddress: ConnectionConfigs.SocketAddress, username: String) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .verifyCA,
            sslrootcert: .file(ROOT_CERT!),
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(
        .enabled(if: ROOT_CERT_UNKNOWN != nil),
        arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 },
        ["hostssl_trust", "host_trust"],
    )
    func connectFailureSSLModeVerifyCAUnknownRootCA(socketAddress: ConnectionConfigs.SocketAddress, username: String)
        async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .verifyCA,
            sslrootcert: .file(ROOT_CERT_UNKNOWN!),
        )
        await #expect(throws: NIOSSLError.self) {
            try await conn.connect(configs: configs)
        }
    }

    @Test(
        arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 },
        ["hostssl_trust", "host_trust"],
    )
    func connectFailureSSLModeVerifyCANoRootCert(socketAddress: ConnectionConfigs.SocketAddress, username: String)
        async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .verifyCA,
        )
        await #expect(throws: NIOSSLError.self) {
            try await conn.connect(configs: configs)
        }
    }

    // MARK: - sslmode verify-full
    @Test(
        .enabled(if: ROOT_CERT != nil),
        arguments: [postgres17GoodCnHostPort].compactMap { $0 },
        ["hostssl_trust", "host_trust"],
    )
    func connectSuccessSSLModeVerifyFull(socketAddress: ConnectionConfigs.SocketAddress, username: String) async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .verifyFull,
            sslrootcert: .file(ROOT_CERT!),
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(
        .enabled(if: ROOT_CERT != nil),
        arguments: [postgres17BadCnHostPort].compactMap { $0 },
        ["hostssl_trust", "host_trust"],
    )

    func connectFailureSSLModeVerifyFullBadCN(socketAddress: ConnectionConfigs.SocketAddress, username: String)
        async throws
    {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: username,
            sslmode: .verifyFull,
            sslrootcert: .file(ROOT_CERT!),
        )
        await #expect(throws: NIOSSLExtraError.self) {
            try await conn.connect(configs: configs)
        }
    }
}
