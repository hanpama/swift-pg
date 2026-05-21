import Testing

@testable import SwiftPG

final class AuthenticationIntegrationTests {
    // - MARK: SCRAM-SHA-256
    @Test(arguments: livePlainEndpoints)
    func connectsWithScramSha256(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        try await conn.connect(configs: livePasswordConnectionConfig(socketAddress: socketAddress))
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: livePlainEndpoints)
    func rejectsInvalidScramSha256Password(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn: Connection = Connection()
        let configs = livePasswordConnectionConfig(
            socketAddress: socketAddress,
            password: "invalid_password"
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        expectDatabaseError(err, .invalidPassword)
        #expect(conn.isConnected() == false)
    }

    // - MARK: clientcert
    @Test(arguments: liveTLSEndpoints)
    func connectsWithClientCertificate(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_cert",
            sslmode: .require,
            sslcert: try .fromPEMFile(try requireEnvironment(CLIENT_CERT, "CLIENT_CERT"))[0],
            sslkey: try .init(file: try requireEnvironment(CLIENT_KEY, "CLIENT_KEY"), format: .pem)
        )

        try await conn.connect(configs: configs)
        #expect(conn.isConnected())
        try await conn.close()
    }

    @Test(arguments: liveTLSEndpoints)
    func rejectsMissingClientCertificate(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_cert",
            sslmode: .require
        )
        await #expect(throws: Error.self) {
            try await conn.connect(configs: configs)
        }
        #expect(conn.isConnected() == false)
    }

    // - MARK: failures
    @Test(arguments: livePlainEndpoints)
    func rejectsPGHBARejectedUser(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let conn: Connection = Connection()
        let configs: ConnectionConfigs = .init(
            socketAddress: socketAddress,
            username: "user_reject",
            sslmode: .disable,
        )
        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        expectDatabaseError(err, .invalidAuthorizationSpecification)
        #expect(conn.isConnected() == false)
    }

    @Test(arguments: livePlainEndpoints)
    func rejectsInvalidDatabase(socketAddress: ConnectionConfigs.SocketAddress) async throws {
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
        expectDatabaseError(err, .invalidCatalogName)
        #expect(conn.isConnected() == false)
    }

    @Test(arguments: livePlainEndpoints)
    func rejectsInvalidUsername(socketAddress: ConnectionConfigs.SocketAddress) async throws {
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
        expectDatabaseError(err, .invalidAuthorizationSpecification)
        #expect(conn.isConnected() == false)
    }
}
