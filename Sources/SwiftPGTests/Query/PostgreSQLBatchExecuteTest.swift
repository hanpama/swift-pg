import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLBatchExecuteTest {
    @Test
    func testSuccessfulBatchExecute() async throws {
        let connection = try await createTestConnection()
        try await connection.batchExecute(
            "INSERT INTO test (id) VALUES ($1)",
            [
                [1],
                [2],
                [3]
            ]
        )
        let rows = try await connection.query("SELECT id FROM test")
        var results: [Int] = []
        for try await row in rows {
            let value: Int = try row.decode()
            results.append(value)
        }
        #expect(results == [1, 2, 3])
    }

    @Test
    func testFailedBatchExecute() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: ClientError.self) {
            try await connection.batchExecute(
                "INSERT INTO test (id) VALUES ($1)",
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
    }

    @Test
    func testSuccessfulBatchExecuteAfterFailure() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: ClientError.self) {
            try await connection.batchExecute(
                "INSERT INTO test (id) VALUES ($1)",
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
        try await connection.batchExecute(
            "INSERT INTO test (id) VALUES ($1)",
            [
                [1],
                [2],
                [3]
            ]
        )
        let rows = try await connection.query("SELECT id FROM test")
        var results: [Int] = []
        for try await row in rows {
            let value: Int = try row.decode()
            results.append(value)
        }
        #expect(results == [1, 2, 3])
    }
}
