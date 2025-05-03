import Testing

@testable import SwiftPG

final class ConnectionAuthenticationTests {

    @Test(arguments: getHostTrustConnectionConfigsList())
    func connectFailureInvalidDatabase(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        var configs = configs
        configs.database = "invalid_database"

        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        guard case .invalidCatalogName = err else {
            throw err!
        }
    }

    @Test(arguments: getHostTrustConnectionConfigsList())
    func connectFailureInvalidCredentials(configs: ConnectionConfigs) async throws {
        let conn = Connection()
        var configs = configs
        configs.username = "invalid_user"
        configs.password = "invalid_password"

        let err = await #expect(throws: DatabaseError.self) {
            try await conn.connect(configs: configs)
        }
        guard case .invalidAuthorizationSpecification = err else {
            throw err!
        }
    }
}
