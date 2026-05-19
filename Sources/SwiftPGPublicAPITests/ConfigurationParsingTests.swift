import SwiftPG
import Testing

final class ConfigurationParsingTests {
    @Test func parsesHostPortDatabaseURL() throws {
        let configs = try ConnectionConfigs.fromDatabaseURL(
            "postgres://user:secret@localhost:5432/appdb?sslmode=disable"
        )

        guard case .hostPort(let host, let port) = configs.socketAddress else {
            Issue.record("Expected hostPort")
            return
        }
        #expect(host == "localhost")
        #expect(port == 5432)
        #expect(configs.username == "user")
        #expect(configs.password == "secret")
        #expect(configs.database == "appdb")
        #expect(configs.sslmode == .disable)
    }

    @Test func parsesUnixSocketDatabaseURL() throws {
        let configs = try ConnectionConfigs.fromDatabaseURL(
            "postgresql:///appdb?host=/var/run/postgresql&port=5433&sslmode=require"
        )

        guard case .unixDomainSocket(let directory, let port) = configs.socketAddress else {
            Issue.record("Expected unixDomainSocket")
            return
        }
        #expect(directory == "/var/run/postgresql")
        #expect(port == 5433)
        #expect(configs.username == "postgres")
        #expect(configs.database == "appdb")
        #expect(configs.sslmode == .require)
    }

    @Test func usesDefaultsWhenDatabaseURLOmitsOptionalParts() throws {
        let configs = try ConnectionConfigs.fromDatabaseURL("postgresql:///")

        guard case .hostPort(let host, let port) = configs.socketAddress else {
            Issue.record("Expected hostPort")
            return
        }
        #expect(host == "localhost")
        #expect(port == 5432)
        #expect(configs.username == "postgres")
        #expect(configs.password == "")
        #expect(configs.database == "postgres")
        #expect(configs.sslmode == .require)
    }

    @Test func rejectsInvalidDatabaseURLScheme() {
        let err = #expect(throws: ClientError.self) {
            try ConnectionConfigs.fromDatabaseURL(
                "mysql://user:secret@localhost:3306/appdb?sslmode=disable"
            )
        }

        guard case .configurationError(let message) = err else {
            Issue.record("Expected configurationError, got \(String(describing: err))")
            return
        }
        #expect(message.contains("Invalid URL scheme"))
    }

    @Test func rejectsInvalidDatabaseURLSSLMode() {
        let err = #expect(throws: ClientError.self) {
            try ConnectionConfigs.fromDatabaseURL(
                "postgres://user:secret@localhost:5432/appdb?sslmode=prefer"
            )
        }

        guard case .configurationError(let message) = err else {
            Issue.record("Expected configurationError, got \(String(describing: err))")
            return
        }
        #expect(message.contains("Invalid sslmode"))
    }
}
