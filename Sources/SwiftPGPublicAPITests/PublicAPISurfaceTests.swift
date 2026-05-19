import SwiftPG
import Testing

final class PublicAPISurfaceTests {
    @Test func publicAPIConstructsConnectionConfigs() {
        let configs = ConnectionConfigs(
            socketAddress: .hostPort(host: "localhost", port: 5432),
            username: "app",
            password: "secret",
            database: "appdb",
            sslmode: .disable
        )

        #expect(configs.username == "app")
        #expect(configs.password == "secret")
        #expect(configs.database == "appdb")
        #expect(configs.sslmode == .disable)
    }

    @Test func publicAPIConstructsSocketAddress() {
        let hostPort = ConnectionConfigs.SocketAddress.hostPort(host: "localhost", port: 5432)
        let unixSocket = ConnectionConfigs.SocketAddress.unixDomainSocket(
            directory: "/var/run/postgresql",
            port: 5432
        )

        guard case .hostPort(let host, let port) = hostPort else {
            Issue.record("Expected hostPort")
            return
        }
        #expect(host == "localhost")
        #expect(port == 5432)

        guard case .unixDomainSocket(let directory, let socketPort) = unixSocket else {
            Issue.record("Expected unixDomainSocket")
            return
        }
        #expect(directory == "/var/run/postgresql")
        #expect(socketPort == 5432)
    }

    @Test func publicAPIConstructsConnectionPool() {
        let configs = ConnectionConfigs(
            socketAddress: .hostPort(host: "localhost", port: 5432),
            username: "app",
            sslmode: .disable
        )

        let pool = ConnectionPool(configuration: configs, maxConnections: 2)
        _ = pool
    }

    @Test func publicAPIParsesDatabaseURL() throws {
        let configs = try ConnectionConfigs.fromDatabaseURL(
            "postgres://user:secret@localhost:5432/appdb?sslmode=disable"
        )

        #expect(configs.username == "user")
        #expect(configs.password == "secret")
        #expect(configs.database == "appdb")
        #expect(configs.sslmode == .disable)
    }
}
