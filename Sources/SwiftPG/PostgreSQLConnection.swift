import Foundation
import Logging
import NIO
import NIOSSL

public final actor PostgreSQLConnection {
  private let eventLoopGroup: EventLoopGroup
  private let defaultTimeout: Duration?
  private let protocolClient: PostgreSQLProtocolClient
  let defaultDecoderMap: [Int32: PostgreSQLDecodable.Type] = DEFAULT_DECODER_MAP
  private var state: State = .initialized

  private enum State {
    case initialized
    case idle
    case busy
    case closed
  }

  public init(eventLoopGroup: EventLoopGroup, defaultTimeout: Duration? = nil) {
    self.eventLoopGroup = eventLoopGroup
    self.defaultTimeout = defaultTimeout
    self.protocolClient = PostgreSQLProtocolClient(eventLoopGroup.next())
  }

  public func connect(configs: PostgreSQLConnectionConfigs) async throws {
    try await protocolClient.connect(configs: configs)
    self.state = .idle
  }

  public func close() async throws {
    guard state != .closed else {
      return
    }
    currentTaskGroup?.cancelAll()
    if !(await protocolClient.isClosed()) {
      try await protocolClient.send(.terminate)
      try await protocolClient.close()
    }
    state = .closed
  }

  public func isClosed() -> Bool {
    return state == .closed
  }

  public func query(
    timeout: Duration? = nil,
    rowBuffer: Int32 = 0,
    _ sql: String,
    _ params: [PostgreSQLEncodable] = []
  )
    async throws -> PostgreSQLRows
  {
    guard state == .idle else {
      throw PostgreSQLError.clientError("Connection is not idle")
    }
    return try await withTask(timeout: timeout) {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: rowBuffer)
      try await self.sync()
      return PostgreSQLRows(connection: self, stmt: stmt, rowLimit: rowBuffer)
    }
  }

  public func execute(timeout: Duration? = nil, _ sql: String, _ params: [PostgreSQLEncodable] = [])
    async throws
  {
    guard state == .idle else {
      throw PostgreSQLError.clientError("Connection is not idle")
    }
    try await withTask(timeout: timeout) {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
      try await self.sync()
      try await self.drainUntilReadyForQuery()
    }
  }

  public func batchQuery(
    timeout: Duration? = nil, _ sql: String, _ batches: [[PostgreSQLEncodable]]
  )
    async throws -> PostgreSQLRows
  {
    guard state == .idle else {
      throw PostgreSQLError.clientError("Connection is not idle")
    }
    return try await withTask(timeout: timeout) {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      for params in batches {
        try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
      }
      try await self.sync()
      return PostgreSQLRows(connection: self, stmt: stmt, rowLimit: 0)
    }
  }

  public func batchExecute(
    timeout: Duration? = nil, _ sql: String, _ batches: [[PostgreSQLEncodable]]
  ) async throws {
    guard state == .idle else {
      throw PostgreSQLError.clientError("Connection is not idle")
    }
    try await withTask(timeout: timeout) {
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
        state = .idle
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
        state = .idle
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
      throw PostgreSQLError.clientTimeout  // TODO: inappropriate error
    }
  }

  // var currentTaskGroup: ThrowingTaskGroup<Sendable?, Error>?
  private var cancelFn: (() -> Void)?

  private func withTask<T: Sendable>(
    timeout: Duration?, _ body: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    self.cancelFn?()
    self.cancelFn = nil
    if let timeout = timeout {
      return try await withThrowingTaskGroup(of: Sendable?.self) { taskGroup in
        self.cancelFn = taskGroup.cancelAll
        taskGroup.addTask {
          return try await body()
        }
        taskGroup.addTask {
          try await Task.sleep(for: timeout)
          try await self.close()
          return nil
        }
        let result = try await taskGroup.next() as! T
        taskGroup.cancelAll()
        return result
      }
    } else {
      return try await body()
    }
  }
}

public final class PostgreSQLStatement: Sendable {
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
    if closed {
      return nil
    }
    if let data = try await connection.receiveRowData(rowLimit: rowLimit) {
      return .init(
        defaultDecoderMap: connection.defaultDecoderMap, fields: stmt.fields, columns: data)
    } else {
      closed = true
      return nil
    }
  }

  public mutating func close() async throws {
    if closed {
      return
    }
    closed = true
    try await connection.drainUntilReadyForQuery()
  }

  public func makeAsyncIterator() -> AsyncIterator {
    return self
  }

  let connection: PostgreSQLConnection
  let stmt: PostgreSQLStatement
  let rowLimit: Int32
  var closed: Bool = false
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

public enum PostgreSQLError: Error, Sendable {
  // case transportError(String)
  case databaseError(String)
  case clientError(String)
  case codecError(String)
  case clientTimeout
}

let DEFAULT_DECODER_MAP: [Int32: PostgreSQLDecodable.Type] = [
  16: Bool.self,
  1000: [Bool].self,
  21: Int16.self,
  1005: [Int16].self,
  23: Int32.self,
  1007: [Int32].self,
  20: Int64.self,
  1016: [Int64].self,
  25: String.self,
  1043: String.self,
  1009: [String].self,
  1015: [String].self,
  700: Float.self,
  1021: [Float].self,
  701: Double.self,
  1022: [Double].self,
  1700: Decimal.self,
  1231: [Decimal].self,
  1114: Date.self,
  1184: Date.self,
  1115: [Date].self,
  1185: [Date].self,
  2950: UUID.self,
  2951: [UUID].self,
]
