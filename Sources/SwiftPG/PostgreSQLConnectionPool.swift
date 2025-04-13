import NIO

public final actor PostgreSQLConnectionPool {
  private let eventLoopGroup: EventLoopGroup
  private let configuration: PostgreSQLConnectionConfigs
  private let maxConnections: Int

  private var connections: [ObjectIdentifier: PostgreSQLConnection] = [:]
  private var availables: [PostgreSQLConnection] = []
  private var waiters: [EventLoopPromise<PostgreSQLConnection>] = []

  init(
    eventLoopGroup: EventLoopGroup,
    configuration: PostgreSQLConnectionConfigs,
    maxConnections: Int
  ) {
    self.eventLoopGroup = eventLoopGroup
    self.configuration = configuration
    self.maxConnections = maxConnections
  }

  func acquire(timeout: Duration? = nil) async throws -> PostgreSQLConnection {
    while let connection = availables.popLast() {
      if connection.isClosed() {
        connections.removeValue(forKey: ObjectIdentifier(connection))
        continue
      }
      return connection
    }

    if connections.count < maxConnections {
      let connection = try await PostgreSQLConnection(
        eventLoopGroup: eventLoopGroup,
        configs: configuration
      )
      connections[ObjectIdentifier(connection)] = connection
      return connection
    }

    let promise = eventLoopGroup.next().makePromise(of: PostgreSQLConnection.self)
    waiters.append(promise)
    if let timeout = timeout {
      Task {
        try await Task.sleep(for: timeout)
        promise.fail(PostgreSQLError.operationTimeout)
      }
    }
    return try await promise.futureResult.get()
  }

  func release(_ connection: PostgreSQLConnection) async {
    if connection.isClosed() {
      connections.removeValue(forKey: ObjectIdentifier(connection))
      return
    }

    if let promise = waiters.popLast() {
      promise.succeed(connection)
    } else {
      availables.append(connection)
    }
  }
}
