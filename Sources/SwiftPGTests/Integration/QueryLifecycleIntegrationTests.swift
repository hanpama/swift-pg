import Testing

@testable import SwiftPG

@Suite(.serialized)
final class QueryLifecycleIntegrationTests {
    @Test(arguments: liveHostPortEndpoints)
    func queryReturnsRows(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let rows = try await retryWhileOperationInProgress {
                try await connection.query("SELECT 1")
            }

            for try await row in rows {
                let value: Int = try row.decode()
                #expect(value == 1)
            }
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func queryReportsDatabaseErrors(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let error = await #expect(throws: DatabaseError.self) {
                _ = try await connection.query("SELECT * FROM non_existent_table")
            }
            expectDatabaseError(error, .undefinedTable)
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func queryRecoversAfterDatabaseError(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let error = await #expect(throws: DatabaseError.self) {
                _ = try await connection.query("SELECT * FROM non_existent_table")
            }
            expectDatabaseError(error, .undefinedTable)

            let rows = try await retryWhileOperationInProgress {
                try await connection.query("SELECT 1")
            }
            for try await row in rows {
                let value: Int = try row.decode()
                #expect(value == 1)
            }
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func queryCancellationPropagates(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let task = Task {
                _ = try await connection.query("SELECT pg_sleep(1)")
            }
            task.cancel()

            await #expect(throws: CancellationError.self) {
                _ = try await task.result.get()
            }
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func executeRunsStatements(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            try await connection.execute("CREATE TEMP TABLE test_execute (id INT)")
            try await connection.execute("INSERT INTO test_execute (id) VALUES (1)")

            let rows = try await connection.query("SELECT id FROM test_execute")
            for try await row in rows {
                let value: Int = try row.decode()
                #expect(value == 1)
            }
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func executeReportsDatabaseErrors(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            try await connection.execute("CREATE TEMP TABLE test_execute_error (id INT)")

            let error = await #expect(throws: DatabaseError.self) {
                try await connection.execute("INSERT INTO test_execute_error (id) VALUES ('invalid')")
            }
            expectDatabaseError(error, .invalidTextRepresentation)
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func executeRecoversAfterDatabaseError(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            try await connection.execute("CREATE TEMP TABLE test_execute_recovery (id INT)")

            let error = await #expect(throws: DatabaseError.self) {
                try await connection.execute("INSERT INTO test_execute_recovery (id) VALUES ('invalid')")
            }
            expectDatabaseError(error, .invalidTextRepresentation)

            try await retryWhileOperationInProgress {
                try await connection.execute("INSERT INTO test_execute_recovery (id) VALUES (1)")
            }
            let rows = try await connection.query("SELECT id FROM test_execute_recovery")
            for try await row in rows {
                let value: Int = try row.decode()
                #expect(value == 1)
            }
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func batchQueryReturnsRows(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let rows = try await connection.batchQuery(
                "SELECT $1::int",
                [[1], [2], [3]]
            )

            var results: [Int] = []
            for try await row in rows {
                let value: Int = try row.decode()
                results.append(value)
            }
            #expect(results == [1, 2, 3])
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func batchQueryReportsDatabaseErrors(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let rows = try await connection.batchQuery(
                "SELECT $1::text::int",
                [["1"], ["invalid"], ["3"]]
            )
            let error = await #expect(throws: DatabaseError.self) {
                for try await _ in rows {}
            }
            expectDatabaseError(error, .invalidTextRepresentation)
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func batchQueryRecoversAfterDatabaseError(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            let failedRows = try await connection.batchQuery(
                "SELECT $1::text::int",
                [["1"], ["invalid"], ["3"]]
            )
            let error = await #expect(throws: DatabaseError.self) {
                for try await _ in failedRows {}
            }
            expectDatabaseError(error, .invalidTextRepresentation)

            let rows = try await retryWhileOperationInProgress {
                try await connection.batchQuery(
                    "SELECT $1::int",
                    [[1], [2], [3]]
                )
            }

            var results: [Int] = []
            for try await row in rows {
                let value: Int = try row.decode()
                results.append(value)
            }
            #expect(results == [1, 2, 3])
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func batchExecuteRunsStatements(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            try await connection.execute("CREATE TEMP TABLE test_batch_execute (id INT)")
            try await connection.batchExecute(
                "INSERT INTO test_batch_execute (id) VALUES ($1)",
                [[1], [2], [3]]
            )

            let rows = try await connection.query("SELECT id FROM test_batch_execute ORDER BY id")
            var results: [Int] = []
            for try await row in rows {
                let value: Int = try row.decode()
                results.append(value)
            }
            #expect(results == [1, 2, 3])
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func batchExecuteReportsDatabaseErrors(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            try await connection.execute("CREATE TEMP TABLE test_batch_execute_error (id INT)")

            let error = await #expect(throws: DatabaseError.self) {
                try await connection.batchExecute(
                    "INSERT INTO test_batch_execute_error (id) VALUES ($1::text::int)",
                    [["1"], ["invalid"], ["3"]]
                )
            }
            expectDatabaseError(error, .invalidTextRepresentation)
        }
    }

    @Test(arguments: liveHostPortEndpoints)
    func batchExecuteRecoversAfterDatabaseError(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        try await withConnection(socketAddress: socketAddress) { connection in
            try await connection.execute("CREATE TEMP TABLE test_batch_execute_recovery (id INT)")

            let error = await #expect(throws: DatabaseError.self) {
                try await connection.batchExecute(
                    "INSERT INTO test_batch_execute_recovery (id) VALUES ($1::text::int)",
                    [["1"], ["invalid"], ["3"]]
                )
            }
            expectDatabaseError(error, .invalidTextRepresentation)

            try await retryWhileOperationInProgress {
                try await connection.batchExecute(
                    "INSERT INTO test_batch_execute_recovery (id) VALUES ($1)",
                    [[1], [2], [3]]
                )
            }

            let rows = try await connection.query("SELECT id FROM test_batch_execute_recovery ORDER BY id")
            var results: [Int] = []
            for try await row in rows {
                let value: Int = try row.decode()
                results.append(value)
            }
            #expect(results == [1, 2, 3])
        }
    }

    private func withConnection<T>(
        socketAddress: ConnectionConfigs.SocketAddress,
        _ operation: (Connection) async throws -> T
    ) async throws -> T {
        let connection = Connection()
        try await connection.connect(configs: liveTrustConnectionConfig(socketAddress: socketAddress))
        do {
            let result = try await operation(connection)
            try await connection.close()
            return result
        } catch {
            try? await connection.close()
            throw error
        }
    }

    private func retryWhileOperationInProgress<T>(
        _ operation: () async throws -> T
    ) async throws -> T {
        var lastError: Error?
        for _ in 0..<20 {
            do {
                return try await operation()
            } catch let error as ClientError {
                guard case .concurrencyError = error else {
                    throw error
                }
                lastError = error
                try await Task.sleep(for: .milliseconds(25))
            }
        }
        throw lastError ?? ClientError.operationTimeout
    }
}
