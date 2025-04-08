import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOSSL

public final actor PostgreSQLConnection {
  private let eventLoopGroup: EventLoopGroup
  private let defaultTimeout: Duration?
  private let protocolClient: PostgreSQLProtocolClient
  let defaultDecoderMap: [Int32: PostgreSQLDecodable.Type] = DEFAULT_DECODER_MAP

  public init(
    eventLoopGroup: EventLoopGroup,
    configs: PostgreSQLConnectionConfigs,
    defaultTimeout: Duration? = nil
  ) async throws {
    self.eventLoopGroup = eventLoopGroup
    self.defaultTimeout = defaultTimeout
    self.protocolClient = try await PostgreSQLProtocolClient(eventLoopGroup.next(), configs)
  }

  public func close() async throws {
    if !protocolClient.isClosed() {
      try await protocolClient.send(.terminate)
      try await protocolClient.close()
    }
  }

  public func isClosed() -> Bool {
    return protocolClient.isClosed()
  }

  public func query(
    timeout: Duration? = nil, rowBuffer: Int32 = 0, _ sql: String,
    _ params: [PostgreSQLEncodable] = []
  ) async throws -> PostgreSQLRows {
    let opCtx = try makeOpCtx(timeout: timeout)
    let stmt = try await opCtx.run {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: rowBuffer)
      try await self.sync()
      return stmt
    }
    return PostgreSQLRows(connection: self, stmt: stmt, rowLimit: rowBuffer, opCtx: opCtx)
  }

  public func execute(timeout: Duration? = nil, _ sql: String, _ params: [PostgreSQLEncodable] = [])
    async throws
  {
    let opCtx = try makeOpCtx(timeout: timeout)
    try await opCtx.run {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
      try await self.sync()
      try await self.drainUntilReadyForQuery()
    }
  }

  public func batchQuery(
    timeout: Duration? = nil, _ sql: String, _ batches: [[PostgreSQLEncodable]]
  ) async throws -> PostgreSQLRows {
    let opCtx = try makeOpCtx(timeout: timeout)
    let stmt = try await opCtx.run {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      for params in batches {
        try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
      }
      try await self.sync()
      return stmt
    }
    return PostgreSQLRows(connection: self, stmt: stmt, rowLimit: 0, opCtx: opCtx)
  }

  public func batchExecute(
    timeout: Duration? = nil, _ sql: String, _ batches: [[PostgreSQLEncodable]]
  ) async throws {
    let opCtx = try makeOpCtx(timeout: timeout)
    try await opCtx.run {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      for params in batches {
        try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
      }
      try await self.sync()
      try await self.drainUntilReadyForQuery()
    }
  }

  func receiveRowData(rowLimit: Int32) async throws -> ByteBuffer? {
    loop: while true {
      switch try await receive() {
      case .dataRow(_, let columnData):
        return columnData
      case .portalSuspended:
        try await protocolClient.send(.execute("", rowLimit))
      case .readyForQuery:
        return nil
      case .errorResponse(let message):
        throw PostgreSQLError.databaseError(message.description)
      default:
        break
      }
    }
  }

  func drainUntilReadyForQuery() async throws {
    loop: while true {
      switch try await receive() {
      case .readyForQuery:
        break loop
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields.description)
      default:
        break
      }
    }
  }

  private func parseStmt(name: String, sql: String) async throws -> PostgreSQLStatement {
    try await protocolClient.send(.parse(name, sql))
    try await protocolClient.send(.describe(variant: 83, name))
    try await protocolClient.send(.flush)

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
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields.description)
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
    try await protocolClient.send(
      .bind(
        portalName: portalName,
        statementName: stmt.name,
        parameterFormatCodes: [Int16](repeating: 1, count: params.count),
        parameterValueCount: params.count,
        parameterValues: paramsBuffer,
        resultColumnFormatCodes: [Int16](repeating: 1, count: stmt.fields.count)
      ))
    try await protocolClient.send(.execute(portalName, rowLimit))
  }

  private func sync() async throws {
    try await protocolClient.send(.sync)
  }

  private func receive() async throws -> PostgreSQLBackendMessage {
    switch try await protocolClient.receive() {
    case .some(let message):
      return message
    case .none:
      throw PostgreSQLError.operationTimeout  // TODO: inappropriate error
    }
  }

  private var currentOpCtx: OperationContext? = nil

  private func makeOpCtx(timeout: Duration?) throws -> OperationContext {
    if let currentOpCtx = currentOpCtx {
      if !currentOpCtx.isClosed() {
        throw PostgreSQLError.clientError("Another operation is in progress")
      }
    }
    let opctx = OperationContext(timeout: timeout)
    currentOpCtx = opctx
    return opctx
  }
}

struct OperationContext: Sendable {
  private let cancelCurrent: NIOLockedValueBox<(@Sendable () -> Void)?> = .init(nil)
  private let closeError: NIOLockedValueBox<Error?> = .init(nil)
  private var timeoutTask: Task<(), any Error>?

  init(timeout: Duration?) {
    if let timeout = timeout {
      timeoutTask = Task { [self] in
        try await Task.sleep(for: timeout)
        closeError.withLockedValue { $0 = PostgreSQLError.operationTimeout }
        cancelCurrent.withLockedValue { $0?() }
      }
    }
  }

  func run<T: Sendable>(_ body: @Sendable @escaping () async throws -> T) async throws -> T {
    if let err = closeError.withLockedValue({ $0 }) {
      throw err
    }
    let task = Task { try await body() }
    cancelCurrent.withLockedValue { $0 = task.cancel }
    do {
      return try await task.value
    } catch _ as CancellationError {
      throw closeError.withLockedValue { $0! }
    } catch {
      throw error
    }
  }

  func close() {
    closeError.withLockedValue { $0 = PostgreSQLError.operationClosed }
    cancelCurrent.withLockedValue { $0?() }
    timeoutTask?.cancel()
  }

  func isClosed() -> Bool {
    return closeError.withLockedValue { $0 != nil }
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

public struct PostgreSQLRows: Sendable, AsyncSequence, AsyncIteratorProtocol {
  public typealias Element = PostgreSQLRow
  public typealias AsyncIterator = Self

  public mutating func next() async throws -> PostgreSQLRow? {
    let data = try await opCtx.run { [self] in
      try await self.connection.receiveRowData(rowLimit: rowLimit)
    }
    if let data = data {
      return .init(
        defaultDecoderMap: connection.defaultDecoderMap,
        fields: stmt.fields,
        columns: data
      )
    } else {
      opCtx.close()
      return nil
    }
  }

  public mutating func close() async throws {
    try await connection.drainUntilReadyForQuery()
    opCtx.close()
  }

  public func makeAsyncIterator() -> AsyncIterator {
    return self
  }

  let connection: PostgreSQLConnection
  let stmt: PostgreSQLStatement
  let rowLimit: Int32
  let opCtx: OperationContext
}

public struct PostgreSQLRow: Sendable {
  let defaultDecoderMap: [Int32: PostgreSQLDecodable.Type]
  let fields: [PostgreSQLFieldDescription]
  let columns: ByteBuffer

  public func decode<each V>(_ types: (repeat each V).Type) throws -> (repeat each V) {
    return try decode<each V>()
  }

  public func decode<each V>() throws -> (repeat each V) {
    var buffer = columns
    var fieldsIterator: IndexingIterator<[PostgreSQLFieldDescription]> = fields.makeIterator()
    return (repeat try get((each V).self, fieldsIterator.next()!.dataTypeOID, &buffer))
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
    throw PostgreSQLError.codecError("Cannot decode \(type)")
  }
}

public protocol PostgreSQLEncodable: Sendable {
  func encode(typeOid: Int32, buffer: inout ByteBuffer) throws
}

public protocol PostgreSQLDecodable: Sendable {
  init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws
}

public enum PostgreSQLError: Error, Sendable, Equatable {
  // case transportError(String)
  case databaseError(String)
  case clientError(String)
  case codecError(String)
  case operationTimeout
  case operationClosed
}
