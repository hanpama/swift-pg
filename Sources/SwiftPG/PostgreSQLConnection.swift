import AsyncAlgorithms
import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOSSL

public final class PostgreSQLConnection: Sendable {
  private let eventLoopGroup: EventLoopGroup
  private let protocolClientBox: NIOLockedValueBox<PostgreSQLProtocolClient?>
  let defaultDecoderMap: [Int32: PostgreSQLDecodable.Type] = DEFAULT_DECODER_MAP

  public init(eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton) {
    self.eventLoopGroup = eventLoopGroup
    self.protocolClientBox = .init(nil)
  }

  public func connect(configs: PostgreSQLConnectionConfigs) async throws {
    let protocolClient = try await PostgreSQLProtocolClient(
      eventLoop: eventLoopGroup.next(),
      configs: configs
    )
    protocolClientBox.withLockedValue { $0 = protocolClient }
    try await send(.startupMessage(configs.username, configs.database))
    try await authenticate(username: configs.username, password: configs.password)
    loop: while true {
      let message = try await receive()
      switch message {
      case .backendKeyData(_, _):
        break loop
      case .errorResponse(let errorMessage):
        throw DatabaseError.from(errorMessage: errorMessage)
      case .noticeResponse(_), .notificationResponse(_, _, _), .parameterStatus(_, _):
        break
      default:
        throw DriverError("Unexpected message: \(message)")
      }
    }
  }

  public func close() {
    currentTask.withLockedValue { $0?.cancel() }
    if let protocolClient = try? getProtocolClient() {
      Task {
        try await send(.terminate)
        try await protocolClient.close()
      }
    }
  }

  public func isConnected() -> Bool {
    if let protocolClient = try? getProtocolClient() {
      return protocolClient.isConnected()
    }
    return false
  }

  public func isClosed() -> Bool {
    if let protocolClient = try? getProtocolClient() {
      return protocolClient.isClosed()
    }
    return true
  }

  public func query(_ sql: String, _ params: [PostgreSQLEncodable] = []) async throws
    -> PostgreSQLRows
  {
    let promise = eventLoopGroup.next().makePromise(of: PostgreSQLRows.self)
    try withinCurrentTask {
      do {
        let stmt = try await self.parseStmt(name: "", sql: sql)
        try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
        try await self.sync()
        let rows = PostgreSQLRows()
        promise.succeed(rows)
        do {
          while let row = try await self.receiveRow(stmt: stmt, rowLimit: 0) {
            await rows.channel.send(row)
          }
          rows.channel.finish()
        } catch {
          rows.channel.fail(error)
        }
      } catch {
        promise.fail(error)
      }
    }
    return try await promise.futureResult.get()
  }

  public func execute(_ sql: String, _ params: [PostgreSQLEncodable] = []) async throws {
    let promise = eventLoopGroup.next().makePromise(of: Void.self)
    try withinCurrentTask {
      do {
        let stmt = try await self.parseStmt(name: "", sql: sql)
        try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
        try await self.sync()
        try await self.drainUntilCommandComplete()
        promise.succeed(())
      } catch {
        promise.fail(error)
      }
    }
    return try await promise.futureResult.get()
  }

  public func batchQuery(_ sql: String, _ batches: [[PostgreSQLEncodable]]) async throws
    -> PostgreSQLRows
  {
    let promise = eventLoopGroup.next().makePromise(of: PostgreSQLRows.self)
    try withinCurrentTask {
      do {
        let stmt = try await self.parseStmt(name: "", sql: sql)
        for params in batches {
          try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
        }
        try await self.sync()
        let rows = PostgreSQLRows()
        promise.succeed(rows)
        do {
          for _ in batches {
            while let row = try await self.receiveRow(stmt: stmt, rowLimit: 0) {
              await rows.channel.send(row)
            }
          }
          rows.channel.finish()
        } catch {
          rows.channel.fail(error)
        }
      } catch {
        promise.fail(error)
      }
    }
    return try await promise.futureResult.get()
  }

  public func batchExecute(_ sql: String, _ batches: [[PostgreSQLEncodable]]) async throws {
    let promise = eventLoopGroup.next().makePromise(of: Void.self)
    try withinCurrentTask {
      do {
        let stmt = try await self.parseStmt(name: "", sql: sql)
        for params in batches {
          try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
        }
        try await self.sync()
        for _ in batches {
          try await self.drainUntilCommandComplete()
        }
        promise.succeed(())
      } catch {
        promise.fail(error)
      }
    }
    return try await promise.futureResult.get()
  }

  func receiveRow(stmt: PostgreSQLStatement, rowLimit: Int32) async throws -> PostgreSQLRow? {
    loop: while true {
      switch try await receive() {
      case .commandComplete(_):
        return nil
      case .portalSuspended:
        try await send(.execute("", rowLimit))
      case .errorResponse(let errorMessage):
        throw DatabaseError.from(errorMessage: errorMessage)
      case .dataRow(_, let columnData):
        return .init(
          defaultDecoderMap: defaultDecoderMap,
          fieldOids: stmt.fields.map { $0.dataTypeOID },
          columns: columnData
        )
      default:
        break
      }
    }
  }

  private func drainUntilCommandComplete() async throws {
    loop: while true {
      switch try await receive() {
      case .commandComplete(_):
        break loop
      case .errorResponse(let errorMessage):
        throw DatabaseError.from(errorMessage: errorMessage)
      default:
        break
      }
    }
  }

  private func parseStmt(name: String, sql: String) async throws -> PostgreSQLStatement {
    try await send(.parse(name, sql))
    try await send(.describe(variant: 83, name))
    try await send(.flush)

    var fields: [PostgreSQLFieldDescription]?
    var parameterOids: [Int32]?
    loop: while true {
      switch try await receive() {
      case .parseComplete:
        break
      case .rowDescription(let descriptions):
        fields = descriptions
        break loop
      case .parameterDescription(let oids):
        parameterOids = oids
      case .errorResponse(let errorMessage):
        throw DatabaseError.from(errorMessage: errorMessage)
      default:
        break
      }
    }
    return PostgreSQLStatement(name: name, fields: fields!, parameterOids: parameterOids!)
  }

  private func execStmt(
    portalName: String, stmt: PostgreSQLStatement, params: [PostgreSQLEncodable], rowLimit: Int32
  ) async throws {
    var paramsBuffer = ByteBuffer()
    for (oid, param) in zip(stmt.parameterOids, params) {
      try param.encode(typeOid: oid, buffer: &paramsBuffer)
    }
    try await send(
      .bind(
        portalName: portalName,
        statementName: stmt.name,
        parameterFormatCodes: [Int16](repeating: 1, count: params.count),
        parameterValueCount: params.count,
        parameterValues: paramsBuffer,
        resultColumnFormatCodes: [Int16](repeating: 1, count: stmt.fields.count)
      ))
    try await send(.execute(portalName, rowLimit))
  }

  private func sync() async throws {
    try await send(.sync)
  }

  func readyForQuery() async throws {
    loop: while true {
      let message = try await receive()
      switch message {
      case .readyForQuery:
        break loop
      case .errorResponse(let errorMessage):
        throw DatabaseError.from(errorMessage: errorMessage)
      case .noticeResponse(_), .notificationResponse(_, _, _):
        break
      default:
        throw DriverError("Unexpected message: \(message)")
      }
    }
  }

  private func authenticate(username: String, password: String) async throws {
    var scramSha256Authenticator: ScramSha256Authenticator?

    loop: while true {
      switch try await receive() {
      case .authenticationOk:
        break loop

      case .authenticationSasl(let mechanisms):
        if mechanisms.contains("SCRAM-SHA-256") || mechanisms.contains("SCRAM-SHA-256-PLUS") {
          scramSha256Authenticator = try ScramSha256Authenticator(
            username: username, password: password)

          try await send(
            .saslInitialResponse(
              mechanism: "SCRAM-SHA-256",
              initialResponse: scramSha256Authenticator!.formatClientFirstMessage()
            )
          )
        } else {
          throw DriverError("No supported SASL mechanism found. Supported: \(mechanisms)")
        }

      case .authenticationSaslContinue(let challenge):
        try scramSha256Authenticator!.handleServerFirstMessage(challenge)

        try await send(
          .saslResponse(scramSha256Authenticator!.formatClientFinalMessage())
        )

      case .authenticationSaslFinal(let finalMessage):
        try scramSha256Authenticator!.handleServerFinalMessage(finalMessage)

      case .errorResponse(let errorMessage):
        throw DatabaseError.from(errorMessage: errorMessage)

      default:
        break
      }
    }
  }

  private func send(_ message: PostgreSQLFrontendMessage) async throws {
    try await getProtocolClient().send(message)
  }

  private func receive() async throws -> PostgreSQLBackendMessage {
    switch try await getProtocolClient().receive() {
    case .some(let message):
      // print("Received message: \(message)")
      return message
    case .none:
      throw ClientError.connectionError("Connection closed")
    }
  }

  private func getProtocolClient() throws -> PostgreSQLProtocolClient {
    let protocolClient = protocolClientBox.withLockedValue { $0 }
    guard let protocolClient = protocolClient else {
      throw ClientError.connectionError("Connection not established")
    }
    return protocolClient
  }

  private let currentTask: NIOLockedValueBox<Task<Void, Error>?> = .init(nil)

  private func withinCurrentTask(_ body: @Sendable @escaping () async throws -> Void) throws {
    try currentTask.withLockedValue({ currentTask in
      if case .some = currentTask {
        throw ClientError.concurrencyError("Operation already in progress")
      } else {
        currentTask = Task {
          defer { self.currentTask.withLockedValue({ $0 = nil }) }
          try await body()
        }
      }
    })
  }

  func waitCurrentTask() async throws {
    if let task = currentTask.withLockedValue({ $0 }) {
      try await task.result.get()
    }
  }
}

public struct PostgreSQLStatement: Sendable {
  let name: String
  let fields: [PostgreSQLFieldDescription]
  let parameterOids: [Int32]

  init(name: String, fields: [PostgreSQLFieldDescription], parameterOids: [Int32]) {
    self.name = name
    self.fields = fields
    self.parameterOids = parameterOids
  }
}

public struct PostgreSQLRows: AsyncSequence, Sendable {
  public typealias Element = PostgreSQLRow
  public typealias AsyncIterator = AsyncThrowingChannel<Element, Error>.AsyncIterator
  let channel = AsyncThrowingChannel<PostgreSQLRow, Error>()

  public func makeAsyncIterator() -> AsyncIterator {
    return channel.makeAsyncIterator()
  }

  public func close() {
    channel.finish()
  }
}

public struct PostgreSQLRow: Sendable {
  let defaultDecoderMap: [Int32: PostgreSQLDecodable.Type]
  let fieldOids: [Int32]
  let columns: ByteBuffer

  public func decode<each V>() throws -> (repeat each V) {
    var buffer = columns
    var fieldsIterator: IndexingIterator<[Int32]> = fieldOids.makeIterator()
    return (repeat try get((each V).self, fieldsIterator.next()!, &buffer))
  }

  private func get<T>(_ type: T.Type, _ oid: Int32, _ buf: inout ByteBuffer) throws -> T {
    if let type = type as? PostgreSQLDecodable.Type {
      return try type.init(pgTypeOid: oid, buffer: &buf) as! T
    }
    if type == PostgreSQLDecodable.self || type == Any.self {
      if let type = defaultDecoderMap[oid] {
        return try type.init(pgTypeOid: oid, buffer: &buf) as! T
      }
    }
    throw ClientError.codecError("Cannot decode \(type)")
  }
}

public protocol PostgreSQLEncodable: Sendable {
  func encode(typeOid: Int32, buffer: inout ByteBuffer) throws
}

public protocol PostgreSQLDecodable: Sendable {
  init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws
}
