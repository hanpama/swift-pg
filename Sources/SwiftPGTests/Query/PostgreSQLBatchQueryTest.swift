import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLBatchQueryTest {
    @Test
    func testSuccessfulBatchQuery() async throws {
        let connection = try await createTestConnection()
        let rows = try await connection.batchQuery(
            "SELECT $1::int",
            [
                [1],
                [2],
                [3]
            ]
        )
        var results: [Int] = []
        for try await row in rows {
            let value: Int = try row.decode()
            results.append(value)
        }
        #expect(results == [1, 2, 3])
    }

    @Test
    func testFailedBatchQuery() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: DatabaseError.self) {
            _ = try await connection.batchQuery(
                "SELECT $1::int",
                [
                    [1],
                    ["invalid"],
                    [3]
                ]
            )
        }
        guard case .invalidTextRepresentation = error else {
            Issue.record("Invalid error case: \(String(describing: error))")
            return
        }
    }

    @Test
    func testSuccessfulBatchQueryAfterFailure() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: ClientError.self) {
            _ = try await connection.batchQuery(
                "SELECT $1::int",
                [
                    [1],
                    ["invalid"],
                    [3]
                ]
            )
        }
        guard case .codecError = error else {
            Issue.record("Invalid error case: \(String(describing: error))")
            return
        }
        let rows = try await connection.batchQuery(
            "SELECT $1::int",
            [
                [1],
                [2],
                [3]
            ]
        )
        var results: [Int] = []
        for try await row in rows {
            let value: Int = try row.decode()
            results.append(value)
        }
        #expect(results == [1, 2, 3])
    }
}
