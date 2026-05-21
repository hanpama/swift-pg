import Testing

@testable import SwiftPG

final class PoolLifecycleIntegrationTests {
    @Test func reusesReleasedConnectionForWaitingAcquire() async throws {
        let pool = ConnectionPool(
            configuration: liveTrustConnectionConfig(socketAddress: postgres17HostPort),
            maxConnections: 1
        )

        let first = try await pool.acquire()
        let waiter = Task {
            try await pool.acquire(timeout: .seconds(1))
        }

        try await Task.sleep(for: .milliseconds(50))
        await pool.release(first)

        let second = try await waiter.value
        #expect(ObjectIdentifier(first) == ObjectIdentifier(second))
        await pool.release(second)
    }

    @Test func queryReleasesConnectionAfterRowsAreConsumed() async throws {
        let pool = ConnectionPool(
            configuration: liveTrustConnectionConfig(socketAddress: postgres17HostPort),
            maxConnections: 1
        )

        try await withTimeout(seconds: 2) {
            let rows = try await pool.query("SELECT 1::int4")
            for try await row in rows {
                let value: Int32 = try row.decode()
                #expect(value == 1)
            }

            let nextRows = try await pool.query("SELECT 2::int4")
            for try await row in nextRows {
                let value: Int32 = try row.decode()
                #expect(value == 2)
            }
        }
    }

    @Test func executeReleasesConnectionAfterCompletion() async throws {
        let pool = ConnectionPool(
            configuration: liveTrustConnectionConfig(socketAddress: postgres17HostPort),
            maxConnections: 1
        )

        try await withTimeout(seconds: 2) {
            try await pool.execute("SELECT 1")
            try await pool.execute("SELECT 2")
        }
    }
}
