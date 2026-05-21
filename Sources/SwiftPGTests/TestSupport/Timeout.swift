import Foundation

struct TestTimeoutError: Error {}

func withTimeout<T: Sendable>(
    seconds: Double = 1,
    operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        group.addTask {
            let nanoseconds = UInt64(seconds * 1_000_000_000)
            try await Task.sleep(nanoseconds: nanoseconds)
            throw TestTimeoutError()
        }

        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}
