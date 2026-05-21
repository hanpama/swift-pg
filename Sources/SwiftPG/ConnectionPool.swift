import NIO

public final actor ConnectionPool {
    private let eventLoopGroup: EventLoopGroup
    private let configuration: ConnectionConfigs
    private let maxConnections: Int

    private var connections: [ObjectIdentifier: Connection] = [:]
    private var availables: [Connection] = []
    private var waiters: [Waiter] = []

    public init(
        configuration: ConnectionConfigs,
        maxConnections: Int,
        eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) {
        self.eventLoopGroup = eventLoopGroup
        self.configuration = configuration
        self.maxConnections = maxConnections
    }

    func acquire(timeout: Duration? = nil) async throws -> Connection {
        while let connection = availables.popLast() {
            if connection.isClosed() {
                connections.removeValue(forKey: ObjectIdentifier(connection))
                continue
            }
            return connection
        }

        if connections.count < maxConnections {
            let connection = Connection(eventLoopGroup: eventLoopGroup)
            connections[ObjectIdentifier(connection)] = connection
            try await connection.connect(configs: configuration)
            return connection
        }

        let waiter = Waiter(eventLoopGroup.next(), timeout)
        waiters.append(waiter)

        return try await waiter.get()
    }

    func release(_ connection: Connection) async {
        if connection.isConnected() {
            if let waiter = waiters.popLast() {
                waiter.succeed(connection)
            } else {
                availables.append(connection)
            }
        } else {
            if let waiter = waiters.popLast() {
                do {
                    try await connection.connect(configs: configuration)
                    waiter.succeed(connection)
                } catch {
                    waiter.fail(error: error)
                }
            } else {
                connections.removeValue(forKey: ObjectIdentifier(connection))
            }
        }
    }

    public func query(_ sql: String, _ params: [PostgreSQLEncodable] = []) async throws
        -> PostgreSQLRows
    {
        let connection = try await acquire()
        let rows = try await connection.query(sql, params)
        Task {
            try await connection.waitCurrentTask()
            await release(connection)
        }
        return rows
    }

    public func execute(_ sql: String, _ params: [PostgreSQLEncodable] = []) async throws {
        let connection = try await acquire()
        try await connection.execute(sql, params)
        Task {
            do { try await connection.waitCurrentTask() }
            await release(connection)
        }
    }

    public func batchQuery(_ sql: String, _ params: [[PostgreSQLEncodable]] = []) async throws
        -> PostgreSQLRows
    {
        let connection = try await acquire()
        let rows = try await connection.batchQuery(sql, params)
        Task {
            do { try await connection.waitCurrentTask() }
            await release(connection)
        }
        return rows
    }

    public func batchExecute(_ sql: String, _ params: [[PostgreSQLEncodable]] = []) async throws {
        let connection = try await acquire()
        try await connection.batchExecute(sql, params)
        Task {
            do { try await connection.waitCurrentTask() }
            await release(connection)
        }
    }
}

private final class Waiter: Sendable {
    let promise: EventLoopPromise<Connection>
    let timeout: Task<Void, Swift.Error>?

    init(_ loop: EventLoop, _ timeout: Duration?) {
        let promise = loop.makePromise(of: Connection.self)
        if let timeout = timeout {
            self.timeout = Task {
                try await Task.sleep(for: timeout)
                promise.fail(ClientError.operationTimeout)
            }
        } else {
            self.timeout = nil
        }
        self.promise = promise
    }

    func fail(error: Swift.Error) {
        timeout?.cancel()
        promise.fail(error)
    }

    func succeed(_ connection: Connection) {
        timeout?.cancel()
        promise.succeed(connection)
    }

    func get() async throws -> Connection {
        do {
            return try await promise.futureResult.get()
        } catch {
            timeout?.cancel()
            throw error
        }
    }
}
