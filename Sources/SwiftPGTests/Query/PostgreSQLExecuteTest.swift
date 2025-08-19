import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLExecuteTest {
    @Test
    func testSuccessfulExecute() async throws {
        let connection = try await createTestConnection()
        try await connection.execute("CREATE TABLE test (id INT)")
        try await connection.execute("INSERT INTO test (id) VALUES (1)")
        let rows = try await connection.query("SELECT id FROM test")
        for try await row in rows {
            let value: Int = try row.decode()
            #expect(value == 1)
        }
        try await connection.execute("DROP TABLE test")
    }

    @Test
    func testFailedExecute() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: DatabaseError.self) {
            try await connection.execute("CREATE TABLE test (id INT)")
            try await connection.execute("INSERT INTO test (id) VALUES ('invalid')")
        }
        guard case .invalidTextRepresentation = error else {
            Issue.record("Invalid error case: \(String(describing: error))")
            return
        }
    }

    @Test
    func testSuccessfulExecuteAfterFailure() async throws {
        let connection = try await createTestConnection()
        let error = await #expect(throws: DatabaseError.self) {
            try await connection.execute("CREATE TABLE test (id INT)")
            try await connection.execute("INSERT INTO test (id) VALUES ('invalid')")
        }
        guard case .invalidTextRepresentation = error else {
            Issue.record("Invalid error case: \(String(describing: error))")
            return
        }
        try await connection.execute("INSERT INTO test (id) VALUES (1)")
        let rows = try await connection.query("SELECT id FROM test")
        for try await row in rows {
            let value: Int = try row.decode()
            #expect(value == 1)
        }
        try await connection.execute("DROP TABLE test")
    }
}
