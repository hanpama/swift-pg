import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLQueryTest {
    @Test
    func testSuccessfulQueryExecution() async throws {
        let connection = try await createTestConnection()
        let rows = try await connection.query("SELECT 1")
        for try await row in rows {
            let value: Int = try row.decode()
            #expect(value == 1)
        }
    }

    @Test
    func testFailedQueryExecution() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: DatabaseError.self) {
            _ = try await connection.query("SELECT * FROM non_existent_table")
        }
        guard case .undefinedTable = error else {
            Issue.record("Invalid error case: \(String(describing: error))")
            return
        }
    }

    @Test
    func testSuccessfulQueryExecutionAfterFailure() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: DatabaseError.self) {
            _ = try await connection.query("SELECT * FROM non_existent_table")
        }
        guard case .undefinedTable = error else {
            Issue.record("Invalid error case: \(String(describing: error))")
            return
        }
        let rows = try await connection.query("SELECT 1")
        for try await row in rows {
            let value: Int = try row.decode()
            #expect(value == 1)
        }
    }

    @Test
    func testQueryCancellation() async throws {
        let connection = try await createTestConnection()
        let task = Task {
            _ = try await connection.query("SELECT pg_sleep(1)")
        }
        task.cancel()
        await #expect(throws: CancellationError.self) {
            _ = try await task.result.get()
        }
    }
}
