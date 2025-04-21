import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLCancelTest {
    @Test func testTimeout() async throws {
        // let connection = try await createTestConnection()

        // let rows1 = try await connection.query(
        //   "SELECT pg_sleep(0.5);"
        // )
        // for try await _ in rows1 {}

        // // Timeout
        // await #expect(throws: PostgreSQLError.operationTimeout) {
        //   print("...")
        //   let rows = try await connection.query(
        //     "SELECT pg_sleep(3);"
        //   )
        //   print("Queried")
        //   for try await _ in rows {}
        // }

        // #expect(await connection.isClosed())

    }
}
