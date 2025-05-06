import NIO
import Testing

@testable import SwiftPG

final class ConnectionAuthenticationTests {
    // - MARK: SCRAM-SHA-256
    @Test(
        arguments: [
            postgres17HostPort,
            postgres16HostPort,
            postgres17UnixSocket,
            postgres17UnixSocket,
        ].compactMap { $0 })
    func connectSuccessScramSha256(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_scram_sha_256",
            password: "a1~!@#$%^&*()_+",
            sslmode: .disable,
        )
        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(
        arguments: [
            postgres17HostPort,
            postgres16HostPort,
            postgres17UnixSocket,
            postgres17UnixSocket,
        ].compactMap { $0 })
    func connectFailureScramSha256InvalidPassword(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn: Connection = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_scram_sha_256",
            password: "invalid_password",
            sslmode: .disable,
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        guard case .invalidPassword = err else {
            Issue.record("Invalid error case: \(String(describing: err))")
            return
        }
        #expect(conn.isConnected() == false)
    }

    // - MARK: clientcert
    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectSuccessClientCert(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_cert",
            sslmode: .require,
            sslcert: try .fromPEMFile(CLIENT_CERT!)[0],
            sslkey: try .init(file: CLIENT_KEY!, format: .pem)
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: [postgres17GoodCnHostPort, postgres17BadCnHostPort].compactMap { $0 })
    func connectFailureClientCertNoClientCert(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_cert",
            sslmode: .require
        )
        // The backend sends an error and the TCP connection gets closed right after.
        // So here what we get is a ChannelError not DatabaseError.
        // The error type might be changed in the future.
        await #expect(throws: ChannelError.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }

    // - MARK: failures
    @Test(
        arguments: [
            postgres17HostPort,
            postgres16HostPort,
            postgres17UnixSocket,
            postgres17UnixSocket,
        ].compactMap { $0 })
    func connectFailurePGHBARejected(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn: Connection = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_reject",
            sslmode: .disable,
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

    @Test(arguments: [postgres17HostPort, postgres16HostPort].compactMap { $0 })
    func connectFailureInvalidDatabase(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_trust",
            database: "invalid_database",
            sslmode: .disable,
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        guard case .invalidCatalogName = err else {
            Issue.record("Invalid error case: \(String(describing: err))")
            return
        }
        #expect(conn.isConnected() == false)
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort].compactMap { $0 })
    func connectFailureInvalidUsername(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "invalid_user",
            password: "invalid_password",
            sslmode: .disable,
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
}
