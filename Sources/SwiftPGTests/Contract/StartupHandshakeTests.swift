import NIO
import Testing

@testable import SwiftPG

final class StartupHandshakeTests {
    @Test func connectCompletesAfterAuthenticationOkBackendKeyDataAndReadyForQuery() async throws {
        try await withScriptedPostgresServer(
            steps: [.readStartup(write: startupSuccessFrames())]
        ) { server in
            let conn = Connection()

            try await withTimeout {
                try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))
            }

            #expect(conn.isConnected())
            try await conn.close()
            #expect(server.failures.isEmpty, "\(server.failures)")
        }
    }

    @Test func connectPreservesStartupDatabaseErrorWhenServerCloses() async throws {
        let server = ScriptedPostgresServer(
            steps: [
                .readStartup(write: [
                    PostgresWire.errorResponse(
                        code: "3D000",
                        message: "database does not exist"
                    )
                ]),
                .close,
            ]
        )
        try await server.start()
        let conn = Connection()

        let error = await #expect(throws: DatabaseError.self) {
            try await withTimeout {
                try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))
            }
        }

        guard case .invalidCatalogName = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            await server.stop()
            return
        }
        #expect(conn.isConnected() == false)
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test(arguments: UnsupportedAuthMethod.allCases)
    func connectFailsFastForUnsupportedAuthenticationMethod(_ method: UnsupportedAuthMethod)
        async throws
    {
        let server = ScriptedPostgresServer(
            steps: [.readStartup(write: [method.frame])]
        )
        try await server.start()
        let conn = Connection()

        await #expect(throws: DriverError.self) {
            try await withTimeout {
                try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))
            }
        }

        #expect(conn.isConnected() == false)
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test func connectThrowsConnectionErrorWhenServerClosesWithoutError() async throws {
        let server = ScriptedPostgresServer(
            steps: [
                .readStartup(),
                .close,
            ]
        )
        try await server.start()
        let conn = Connection()

        let error = await #expect(throws: ClientError.self) {
            try await withTimeout {
                try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))
            }
        }

        guard case .connectionError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            await server.stop()
            return
        }
        #expect(conn.isConnected() == false)
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }
}

enum UnsupportedAuthMethod: CaseIterable, CustomStringConvertible {
    case cleartextPassword
    case md5Password
    case kerberosV5
    case gss
    case gssContinue
    case sspi

    var description: String {
        switch self {
        case .cleartextPassword: "cleartext password"
        case .md5Password: "MD5 password"
        case .kerberosV5: "Kerberos V5"
        case .gss: "GSS"
        case .gssContinue: "GSS continuation"
        case .sspi: "SSPI"
        }
    }

    var frame: ByteBuffer {
        switch self {
        case .cleartextPassword: PostgresWire.authenticationCleartextPassword()
        case .md5Password: PostgresWire.authenticationMD5Password()
        case .kerberosV5: PostgresWire.authenticationKerberosV5()
        case .gss: PostgresWire.authenticationGSS()
        case .gssContinue: PostgresWire.authenticationGSSContinue()
        case .sspi: PostgresWire.authenticationSSPI()
        }
    }
}
