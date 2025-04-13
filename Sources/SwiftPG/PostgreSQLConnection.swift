import AsyncAlgorithms
import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOSSL

public final class PostgreSQLConnection: Sendable {
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
    currentTask.withLockedValue { $0?.cancel() }
    if !protocolClient.isClosed() {
      try await protocolClient.send(.terminate)
      try await protocolClient.close()
    }
  }

  public func isClosed() -> Bool {
    return protocolClient.isClosed()
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
        try await protocolClient.send(.execute("", rowLimit))
      case .errorResponse(let message):
        throw PostgreSQLError.databaseError(message.description)
      case .dataRow(_, let columnData):
        return .init(
          defaultDecoderMap: defaultDecoderMap,
          fields: stmt.fields,
          columns: columnData
        )
      default:
        break
      }
    }
  }

  func drainUntilCommandComplete() async throws {
    loop: while true {
      switch try await receive() {
      case .commandComplete(_):
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
      throw PostgreSQLError.clientError("Connection closed")
    }
  }

  private func readyForQuery() async throws {
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

  private let currentTask: NIOLockedValueBox<Task<Void, Error>?> = .init(nil)

  private func withinCurrentTask(_ body: @Sendable @escaping () async throws -> Void) throws {
    try currentTask.withLockedValue({ currentTask in
      if case .some = currentTask {
        throw PostgreSQLError.clientError("Operation already in progress")
      } else {
        currentTask = Task {
          defer { self.currentTask.withLockedValue({ $0 = nil }) }
          try await body()
        }
      }
    })
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
  case databaseError(String)
  case clientError(String)
  case codecError(String)
  case operationTimeout
  case operationClosed
}
