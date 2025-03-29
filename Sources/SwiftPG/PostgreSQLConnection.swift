import Crypto
import Foundation
import Logging
import NIO
import NIOSSL

// import

// MARK: - Types

enum PostgreSQLFrontendMessage: Sendable {
  case bind(
    portalName: String,
    statementName: String,
    parameterFormatCodes: [Int16],
    parameterValueCount: Int,
    parameterValues: ByteBuffer,
    resultColumnFormatCodes: [Int16]
  )
  case close(variant: UInt8, _ name: String)
  case describe(variant: UInt8, _ name: String)
  case execute(_ portalName: String, _ rowLimit: Int32)
  case flush
  case parse(_ statementName: String, _ sql: String)
  case saslInitialResponse(mechanism: String, initialResponse: String?)
  case saslResponse(_ response: String)
  case startupMessage(_ user: String, _ database: String)
  case sync
  case terminate
}

enum PostgreSQLBackendMessage: Sendable {
  case authenticationOk
  case authenticationKerberosV5
  case authenticationCleartextPassword
  case authenticationMD5Password(_ salt: [UInt8])
  case authenticationGSS
  case authenticationGSSContinue
  case authenticationSSPI
  case authenticationSasl(_ mechanisms: [String])
  case authenticationSaslContinue(String)
  case authenticationSaslFinal(String)
  case backendKeyData(_ processID: Int32, _ secretKey: Int32)
  case bindComplete
  case closeComplete
  case commandComplete(_ commandTag: String)
  case copyData(_ data: [UInt8])
  case copyDone
  case copyInResponse(_ format: Int8, _ columnFormats: [Int16])
  case copyOutResponse(_ format: Int8, _ columnFormats: [Int16])
  case copyBothResponse(_ format: Int8, _ columnFormats: [Int16])
  case dataRow(columns: Int16, columnData: ByteBuffer)
  case emptyQueryResponse
  case errorResponse(_ errorFields: [PostgreSQLMessageField])
  case functionCallResponse(_ functionResult: [UInt8])
  case negotiateProtocolVersion(_ newestVersion: Int32, _ notRecognized: [String])
  case noData
  case noticeResponse(_ noticeFields: [PostgreSQLMessageField])
  case notificationResponse(_ processID: Int32, _ channel: String, _ payload: String)
  case parameterDescription(_ parameterOIDs: [Int32])
  case parameterStatus(_ parameter: String, _ value: String)
  case parseComplete
  case portalSuspended
  case readyForQuery(_ transactionStatus: UInt8)
  case rowDescription(_ fields: [PostgreSQLFieldDescription])

  case unknown  // Placeholder for unknown message types
  case unknownAuthentication  // Placeholder for unknown authentication message types
}

public enum PostgreSQLMessageField: Sendable {
  case severity(String)
  case code(String)
  case message(String)
  case detail(String)
  case hint(String)
  case position(String)
  case internalPosition(String)
  case internalQuery(String)
  case where_(String)
  case schemaName(String)
  case tableName(String)
  case columnName(String)
  case dataTypeName(String)
  case constraintName(String)
  case file(String)
  case line(String)
  case routine(String)
}

struct PostgreSQLFieldDescription: Sendable {
  let name: String
  let tableOID: Int32
  let columnAttr: Int16
  let dataTypeOID: Int32
  let dataTypeSize: Int16
  let typeModifier: Int32
  let formatCode: Int16
}

struct PostgreSQLBoundParameter: Sendable {
  let formatCode: Int16
  let value: [UInt8]
}

public enum PostgreSQLError: Error, Sendable {
  case transportError(String)
  case databaseError(fields: [PostgreSQLMessageField])
  case clientError(String)
  case codecError(String)
  case clientTimeout
}

// MARK: - Protocol Client

final actor PostgreSQLProtocolClient {
  private let messages: AsyncStream<PostgreSQLBackendMessage?>
  private let bootstrap: ClientBootstrap
  private var channel: Channel?
  private var state: State = .created
  private var cert: NIOSSLCertificate?

  enum State {
    case created
    case connected
    case disconnected
  }

  enum ConnectOptions {
    case hostPort(_ host: String, _ port: Int)
    case unixDomainSocket(path: String)
  }

  init(_ loop: EventLoop) {
    var continuation: AsyncStream<PostgreSQLBackendMessage?>.Continuation?
    messages = AsyncStream { continuation = $0 }
    bootstrap = ClientBootstrap(group: loop)
      .channelOption(.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
      .channelOption(.autoRead, value: false)
      .channelInitializer { channel in
        channel.pipeline.addHandler(
          PostgreSQLMessageHandler(
            onMessage: { [continuation] message in continuation!.yield(message) },
            onDisconnect: { [continuation] in continuation!.finish() }
          )
        )
      }
  }

  func connect(host: String, port: Int) async throws {
    guard state == .created else {
      throw PostgreSQLError.clientError("ProtocolClient is already open")
    }
    state = .connected
    self.channel = try await bootstrap.connect(host: host, port: port).get()
    channel!.read()
  }

  func connect(unixDomainSocketPath: String) async throws {
    guard state == .created else {
      throw PostgreSQLError.clientError("ProtocolClient is already open")
    }
    state = .connected
    self.channel = try await bootstrap.connect(unixDomainSocketPath: unixDomainSocketPath).get()
    channel!.read()
  }

  func enableTLS(host: String, _ tlsConfiguration: TLSConfiguration) async throws {
    guard let channel = self.channel else {
      throw PostgreSQLError.transportError("ProtocolClient is not connected")
    }
    let sslContext = try NIOSSLContext(configuration: tlsConfiguration)
    let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: host)
    try await channel.pipeline.addHandler(sslHandler, position: .first).get()
  }

  func getTLSCertificate() -> NIOSSLCertificate? {
    return cert
  }

  func send(_ message: PostgreSQLFrontendMessage) async throws {
    // print("Sending message: \(message)")
    let buffer: ByteBuffer = encodeMessage(message: message)
    try await getChannel().writeAndFlush(buffer)
  }

  func close() async throws {
    let channel = try getChannel()
    state = .disconnected
    try await channel.close()
  }

  func isClosed() -> Bool {
    return state == .disconnected
  }

  func receive() async throws -> PostgreSQLBackendMessage? {
    for try await message in messages {
      if let message = message {
        return message
      } else {
        try getChannel().read()
      }
    }
    return nil
  }

  private func getChannel() throws -> Channel {
    guard let channel = self.channel else {
      throw PostgreSQLError.transportError("ProtocolClient is not connected")
    }
    return channel
  }

  private func encodeMessage(message: PostgreSQLFrontendMessage) -> ByteBuffer {
    var buffer = ByteBuffer()

    switch message {
    case .bind(
      let portalName,
      let statementName,
      let parameterFormatCodes,
      let parameterValueCount,
      var parameterValues,
      let resultColumnFormatCodes
    ):
      var body = ByteBuffer()
      body.writeString(portalName)
      body.writeInteger(0, as: UInt8.self)
      body.writeString(statementName)
      body.writeInteger(0, as: UInt8.self)
      body.writeInteger(Int16(parameterFormatCodes.count))
      for code in parameterFormatCodes {
        body.writeInteger(code)
      }
      body.writeInteger(Int16(parameterValueCount))
      body.writeBuffer(&parameterValues)
      body.writeInteger(Int16(resultColumnFormatCodes.count))
      for code in resultColumnFormatCodes {
        body.writeInteger(code)
      }

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "B"))
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .close(let variant, let name):
      var body = ByteBuffer()
      body.writeInteger(variant)
      body.writeString(name)
      body.writeInteger(0, as: UInt8.self)

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "C"))
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .describe(let variant, let name):
      var body = ByteBuffer()
      body.writeInteger(variant)
      body.writeString(name)
      body.writeInteger(0, as: UInt8.self)

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "D"))
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .execute(let portalName, let rowLimit):
      var body = ByteBuffer()
      if portalName.isEmpty {
        body.writeInteger(0, as: UInt8.self)
      } else {
        body.writeString(portalName)
        body.writeInteger(0, as: UInt8.self)
      }
      body.writeInteger(rowLimit)  // No row limit

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "E"))
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .flush:
      buffer.writeInteger(UInt8(ascii: "H"))
      buffer.writeInteger(Int32(4))

    case .parse(let statementName, let sql):
      var body = ByteBuffer()
      body.writeString(statementName)
      body.writeInteger(0, as: UInt8.self)
      body.writeString(sql)
      body.writeInteger(0, as: UInt8.self)
      body.writeInteger(Int16(0))  // No parameter types

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "P"))
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .saslInitialResponse(let mechanism, let initialResponse):
      var body = ByteBuffer()
      body.writeString(mechanism)
      body.writeInteger(0, as: UInt8.self)
      if let initialResponse = initialResponse {
        body.writeInteger(Int32(initialResponse.utf8.count))
        body.writeString(initialResponse)
      } else {
        body.writeInteger(-1)
      }

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "p"))
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .saslResponse(let response):
      buffer.writeInteger(UInt8(ascii: "p"))

      let messageLength = Int32(response.utf8.count + 4)
      buffer.writeInteger(messageLength)
      buffer.writeString(response)

    case .startupMessage(let user, let database):
      var body = ByteBuffer()
      body.writeInteger(Int32(196608))  // Protocol version number (3.0)
      body.writeString("user")
      body.writeInteger(0, as: UInt8.self)
      body.writeString(user)
      body.writeInteger(0, as: UInt8.self)
      body.writeString("database")
      body.writeInteger(0, as: UInt8.self)
      body.writeString(database)
      body.writeInteger(0, as: UInt8.self)
      body.writeInteger(0, as: UInt8.self)  // End of parameters

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(messageLength)
      buffer.writeBuffer(&body)

    case .sync:
      buffer.writeInteger(UInt8(ascii: "S"))
      buffer.writeInteger(Int32(4))

    case .terminate:
      buffer.writeInteger(UInt8(ascii: "X"))
      buffer.writeInteger(Int32(4))
    }
    return buffer
  }

  private static func decodeMessage(_ type: UInt8, _ length: Int, _ buffer: inout ByteBuffer)
    -> PostgreSQLBackendMessage
  {
    let message: PostgreSQLBackendMessage
    // print("Receiving message type: \(messageType)")

    switch type {
    case 82:  // 'R'
      let authMessageType = buffer.readInteger(as: Int32.self)!
      switch authMessageType {
      case 0:
        message = .authenticationOk
      case 10:
        message = .authenticationSasl(
          buffer.readNullTerminatedString()!.split(separator: ",").map { String($0) })
      case 11:
        message = .authenticationSaslContinue(buffer.readString(length: length - 4)!)
      case 12:
        message = .authenticationSaslFinal(buffer.readString(length: length - 4)!)
      default:
        message = .unknownAuthentication
      }
    case 67:  // 'C'
      message = .commandComplete(buffer.readNullTerminatedString()!)
    case 69:  // 'E'
      var errorFields: [PostgreSQLMessageField] = []
      while let fieldType = buffer.readInteger(as: UInt8.self), fieldType != 0 {
        guard let fieldValue = buffer.readNullTerminatedString() else {
          continue
        }
        let field: PostgreSQLMessageField
        switch fieldType {
        case 0x53: field = .severity(fieldValue)  // 'S'
        case 0x56: field = .severity(fieldValue)  // 'V'
        case 0x43: field = .code(fieldValue)  // 'C'
        case 0x4D: field = .message(fieldValue)  // 'M'
        case 0x44: field = .detail(fieldValue)  // 'D'
        case 0x48: field = .hint(fieldValue)  // 'H'
        case 0x50: field = .position(fieldValue)  // 'P'
        case 0x70: field = .internalPosition(fieldValue)  // 'p'
        case 0x71: field = .internalQuery(fieldValue)  // 'q'
        case 0x57: field = .where_(fieldValue)  // 'W'
        case 0x73: field = .schemaName(fieldValue)  // 's'
        case 0x74: field = .tableName(fieldValue)  // 't'
        case 0x63: field = .columnName(fieldValue)  // 'c'
        case 0x64: field = .dataTypeName(fieldValue)  // 'd'
        case 0x6E: field = .constraintName(fieldValue)  // 'n'
        case 0x46: field = .file(fieldValue)  // 'F'
        case 0x4C: field = .line(fieldValue)  // 'L'
        case 0x52: field = .routine(fieldValue)  // 'R'
        default: continue
        }
        errorFields.append(field)
      }
      message = .errorResponse(errorFields)
    case 90:  // 'Z'
      message = .readyForQuery(buffer.readInteger(as: UInt8.self)!)
    case 84:  // 'T'
      message = .rowDescription(
        (0..<buffer.readInteger(as: Int16.self)!).map { _ in
          .init(
            name: buffer.readNullTerminatedString()!,
            tableOID: buffer.readInteger(as: Int32.self)!,
            columnAttr: buffer.readInteger(as: Int16.self)!,
            dataTypeOID: buffer.readInteger(as: Int32.self)!,
            dataTypeSize: buffer.readInteger(as: Int16.self)!,
            typeModifier: buffer.readInteger(as: Int32.self)!,
            formatCode: buffer.readInteger(as: Int16.self)!
          )
        }
      )
    case 116:  // 't'
      message = .parameterDescription(
        (0..<buffer.readInteger(as: Int16.self)!).map { _ in
          buffer.readInteger(as: Int32.self)!
        }
      )
    case 68:  // 'D'
      message = .dataRow(columns: buffer.readInteger(as: Int16.self)!, columnData: buffer)
    case 49:  // '1'
      message = .parseComplete
    case 50:  // '2'
      message = .bindComplete
    default:
      message = .unknown
    }
    // print("Received message: \(message)")
    return message
  }

  private final class PostgreSQLMessageHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer
    typealias OnMessage = @Sendable (PostgreSQLBackendMessage?) -> Void
    typealias OnDisconnect = @Sendable () -> Void
    private let onMessage: OnMessage
    private let onDisconnect: OnDisconnect

    init(onMessage: @escaping OnMessage, onDisconnect: @escaping OnDisconnect) {
      self.onMessage = onMessage
      self.onDisconnect = onDisconnect
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
      var buffer = unwrapInboundIn(data)
      while buffer.readableBytes >= 5 {  // Process all complete messages in the buffer
        guard let messageType = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self),
          let messageLength = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self)
        else {
          break
        }
        let dataLength = Int(messageLength) - 4  // 4 bytes for the length itself
        if buffer.readableBytes < 5 + dataLength {  // Not enough data yet
          break
        }
        buffer.moveReaderIndex(forwardBy: 5)  // Consume the message type and length
        var dataBuffer = buffer.readSlice(length: dataLength)!

        let message = decodeMessage(messageType, dataLength, &dataBuffer)
        onMessage(message)
      }
      onMessage(nil)
    }
  }
}

// MARK: - Connection

public final actor PostgreSQLConnection {
  private let eventLoopGroup: EventLoopGroup
  private let defaultTimeout: Duration?
  private let protocolClient: PostgreSQLProtocolClient
  private var state: State = .initialized

  private enum State {
    case initialized
    case ready
    case busy
    case closed
  }

  public init(eventLoopGroup: EventLoopGroup, defaultTimeout: Duration? = nil) {
    self.eventLoopGroup = eventLoopGroup
    self.defaultTimeout = defaultTimeout
    self.protocolClient = PostgreSQLProtocolClient(eventLoopGroup.next())
  }

  public func connect(configs: PostgreSQLConnectionConfigs) async throws {
    guard state == .initialized else {
      throw PostgreSQLError.clientError("Connection has already been opened")
    }
    switch configs.socketAddress {
    case .hostPort(let host, let port):
      try await protocolClient.connect(host: host, port: port)
    case .unixDomainSocket(let path):
      try await protocolClient.connect(unixDomainSocketPath: path)
    }

    if let tls = configs.tls {
      if case let .hostPort(host: host, _) = configs.socketAddress {
        try await protocolClient.enableTLS(host: host, tls)
      } else {
        throw PostgreSQLError.clientError("TLS is not supported for Unix domain sockets")
      }
    }

    try await protocolClient.send(.startupMessage(configs.username, configs.database))

    var scramSha256Authenticator: ScramSha256Authenticator?

    loop: while true {
      switch try await receive() {
      case .authenticationOk:
        break

      case .authenticationSasl(let mechanisms):
        if mechanisms.contains("SCRAM-SHA-256") || mechanisms.contains("SCRAM-SHA-256-PLUS") {
          scramSha256Authenticator = try ScramSha256Authenticator(
            username: configs.username, password: configs.password)

          try await protocolClient.send(
            .saslInitialResponse(
              mechanism: "SCRAM-SHA-256",
              initialResponse: scramSha256Authenticator!.formatClientFirstMessage()
            )
          )
        } else {
          throw PostgreSQLError.clientError(
            "No supported SASL mechanism found. Supported: \(mechanisms)")
        }

      case .authenticationSaslContinue(let challenge):
        guard let scramSha256Authenticator = scramSha256Authenticator else {
          throw PostgreSQLError.clientError("Unexpected SASL continue message")
        }
        try scramSha256Authenticator.handleServerFirstMessage(challenge)

        try await protocolClient.send(
          .saslResponse(scramSha256Authenticator.formatClientFinalMessage())
        )

      case .authenticationSaslFinal(let finalMessage):
        guard let scramSha256Authenticator = scramSha256Authenticator else {
          throw PostgreSQLError.clientError("Unexpected SASL final message")
        }
        try scramSha256Authenticator.handleServerFinalMessage(finalMessage)

      case .readyForQuery:
        state = .ready
        break loop
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields: fields)
      default:
        break
      }
    }
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
    guard state == .ready else {
      throw PostgreSQLError.clientError("Connection is not ready")
    }
    return try await withTask(timeout: timeout) {
      let stmt = try await self.parseStmt(name: "", sql: sql)
      try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
      try await self.sync()
      return PostgreSQLRows(connection: self, stmt: stmt, rowLimit: 0)
    }
  }

  public func execute(timeout: Duration? = nil, _ sql: String, _ params: [PostgreSQLEncodable] = [])
    async throws
  {
    guard state == .ready else {
      throw PostgreSQLError.clientError("Connection is not ready")
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
    guard state == .ready else {
      throw PostgreSQLError.clientError("Connection is not ready")
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
    guard state == .ready else {
      throw PostgreSQLError.clientError("Connection is not ready")
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
      // return .init(fields: stmt.fields, columns: columnData)
      case .portalSuspended:
        try await protocolClient.send(.execute("", rowLimit))
      case .readyForQuery:
        state = .ready
        return nil
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields: fields)
      default:
        break
      }
    }
  }

  func drainUntilReadyForQuery() async throws {
    loop: while true {
      switch try await receive() {
      case .readyForQuery:
        state = .ready
        break loop
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields: fields)
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
        throw PostgreSQLError.databaseError(fields: fields)
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
      throw PostgreSQLError.clientTimeout
    }
  }

  var currentTaskGroup: ThrowingTaskGroup<Sendable?, Error>?

  private func withTask<T: Sendable>(
    timeout: Duration?, _ body: @Sendable @escaping () async throws -> T
  )
    async throws
    -> T
  {
    return try await withThrowingTaskGroup(of: Sendable?.self) { taskGroup in
      if currentTaskGroup != nil {
        throw PostgreSQLError.clientError("Connection is busy")
      } else {
        self.currentTaskGroup = taskGroup
      }
      defer { self.currentTaskGroup = nil }
      taskGroup.addTask {
        return try await body()
      }
      if let timeout = timeout {
        taskGroup.addTask {
          try await Task.sleep(for: timeout)
          try await self.close()
          return nil
        }
      }
      let result = try await taskGroup.next() as! T
      taskGroup.cancelAll()
      return result
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

public struct PostgreSQLParameters {
  let oids: [Int]
  let values: [PostgreSQLEncodable]
}

public struct PostgreSQLRows: Sendable, AsyncSequence, AsyncIteratorProtocol {
  public typealias Element = PostgreSQLRow
  public typealias AsyncIterator = Self

  public func next() async throws -> PostgreSQLRow? {
    if let data = try await connection.receiveRowData(rowLimit: rowLimit) {
      return .init(fields: stmt.fields, columns: data)
    } else {
      return nil
    }
  }

  public func close() async throws {
    try await connection.drainUntilReadyForQuery()
  }

  public func makeAsyncIterator() -> AsyncIterator {
    return self
  }

  let connection: PostgreSQLConnection
  let stmt: PostgreSQLStatement
  let rowLimit: Int32
}

public struct PostgreSQLRow: Sendable {
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
      switch oid {
      case 16: return try Bool?(pgTypeOid: oid, buffer: &buf) as! T
      case 1000: return try [Bool]?(pgTypeOid: oid, buffer: &buf) as! T
      case 21: return try Int16?(pgTypeOid: oid, buffer: &buf) as! T
      case 1005: return try [Int16]?(pgTypeOid: oid, buffer: &buf) as! T
      case 23: return try Int32?(pgTypeOid: oid, buffer: &buf) as! T
      case 1007: return try [Int32]?(pgTypeOid: oid, buffer: &buf) as! T
      case 20: return try Int64?(pgTypeOid: oid, buffer: &buf) as! T
      case 1016: return try [Int64]?(pgTypeOid: oid, buffer: &buf) as! T
      case 25: return try String?(pgTypeOid: oid, buffer: &buf) as! T
      case 1043: return try String?(pgTypeOid: oid, buffer: &buf) as! T
      case 1009: return try [String]?(pgTypeOid: oid, buffer: &buf) as! T
      case 1015: return try [String]?(pgTypeOid: oid, buffer: &buf) as! T
      case 700: return try Float?(pgTypeOid: oid, buffer: &buf) as! T
      case 1021: return try [Float]?(pgTypeOid: oid, buffer: &buf) as! T
      case 701: return try Double?(pgTypeOid: oid, buffer: &buf) as! T
      case 1022: return try [Double]?(pgTypeOid: oid, buffer: &buf) as! T
      case 1700: return try Decimal?(pgTypeOid: oid, buffer: &buf) as! T
      case 1231: return try [Decimal]?(pgTypeOid: oid, buffer: &buf) as! T
      case 1114: return try Date?(pgTypeOid: oid, buffer: &buf) as! T
      case 1184: return try Date?(pgTypeOid: oid, buffer: &buf) as! T
      case 1115: return try [Date]?(pgTypeOid: oid, buffer: &buf) as! T
      case 1185: return try [Date]?(pgTypeOid: oid, buffer: &buf) as! T
      case 2950: return try UUID?(pgTypeOid: oid, buffer: &buf) as! T
      case 2951: return try [UUID]?(pgTypeOid: oid, buffer: &buf) as! T
      default: throw PostgreSQLError.codecError("Cannot decode Any from \(oid)")
      }
    }
    throw PostgreSQLError.codecError("Cannot decode \(type)")
  }
}

public struct PostgreSQLConnectionConfigs: Sendable {

  let socketAddress: SocketAddress
  let username: String
  let password: String
  let database: String
  let tls: TLSConfiguration?

  enum SocketAddress {
    case hostPort(host: String, port: Int)
    case unixDomainSocket(path: String)
  }
}

// MARK: - Codec

public protocol PostgreSQLEncodable: Sendable {
  func encode(typeOid: Int32, buffer: inout ByteBuffer) throws
}

public protocol PostgreSQLDecodable: Sendable {
  init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws
}

public protocol PostgreSQLCodable: PostgreSQLEncodable & PostgreSQLDecodable {}

public protocol PostgreSQLCodableArrayElement {
  static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32
}

extension Bool: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 1 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 16 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Bool as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 16 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Bool")
      }
      self = buffer.readInteger(as: UInt8.self) == 1
    } else {
      throw PostgreSQLError.codecError("Cannot decode Bool from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1000 {
      return 16
    }
    throw PostgreSQLError.codecError("Cannot get Bool element type oid from \(pgArrayTypeOid)")
  }
}

extension Int16: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 2 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 21 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self, as: Int16.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int16 as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 21 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int16")
      }
      guard let value = buffer.readInteger(as: Int16.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int16")
      }
      self = value
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int16 from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1005 {
      return 21
    }
    throw PostgreSQLError.codecError("Cannot get Int16 element type oid from \(pgArrayTypeOid)")
  }
}

extension Int32: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 4 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 23 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self, as: Int32.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int32 as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 23 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int32")
      }
      guard let value = buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int32")
      }
      self = value
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int32 from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1007 {
      return 23
    }
    throw PostgreSQLError.codecError("Cannot get Int32 element type oid from \(pgArrayTypeOid)")
  }
}

extension Int64: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 8 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 20 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self, as: Int64.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int64 as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 20 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int64")
      }
      guard let value = buffer.readInteger(as: Int64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int64")
      }
      self = value
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int64 from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1016 {
      return 20
    }
    throw PostgreSQLError.codecError("Cannot get Int64 element type oid from \(pgArrayTypeOid)")
  }
}

extension Int: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 20 {
      try Int64(self).encode(typeOid: 20, buffer: &buffer)
    } else if typeOid == 23 {
      guard self >= Int32.min && self <= Int32.max else {
        throw PostgreSQLError.codecError("Integer \(self) out of bounds for int4")
      }
      try Int32(self).encode(typeOid: 23, buffer: &buffer)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 20 {
      guard let length = buffer.readInteger(as: Int32.self), length == 8 else {
        throw PostgreSQLError.codecError("Invalid data for Int")
      }
      guard let value = buffer.readInteger(as: Int64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int")
      }
      guard value >= Int.min && value <= Int.max else {
        throw PostgreSQLError.codecError("bigint \(value) out of bounds for Int")
      }
      self = Int(value)
    } else if pgTypeOid == 23 {
      let value = try Int32(pgTypeOid: 23, buffer: &buffer)
      self = Int(value)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1016 {
      return 20
    } else if pgArrayTypeOid == 1007 {
      return 23
    }
    throw PostgreSQLError.codecError("Cannot get Int element type oid from \(pgArrayTypeOid)")
  }
}

extension String: PostgreSQLCodable, PostgreSQLCodableArrayElement {

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 25 || typeOid == 1043 {
      buffer.writeInteger(Int32(utf8.count), as: Int32.self)
      buffer.writeBytes(utf8)
    } else {
      throw PostgreSQLError.codecError("Cannot encode String as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 25 || pgTypeOid == 1043 {
      guard let length = buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for String")
      }
      guard let string = buffer.readString(length: Int(length)) else {
        throw PostgreSQLError.codecError("Invalid data for String")
      }
      self = string
    } else {
      throw PostgreSQLError.codecError("Cannot decode String from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1009 {
      return 25
    } else if pgArrayTypeOid == 1015 {
      return 1043
    }
    throw PostgreSQLError.codecError("Cannot get String element type oid from \(pgArrayTypeOid)")
  }
}

extension Float: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 4 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 700 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self.bitPattern, as: UInt32.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Float as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 700 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Float")
      }
      guard let bitPattern = buffer.readInteger(as: UInt32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Float")
      }
      self = Float(bitPattern: bitPattern)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Float from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1021 {
      return 700
    }
    throw PostgreSQLError.codecError("Cannot get Float element type oid from \(pgArrayTypeOid)")
  }
}

extension Double: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 8 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 701 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self.bitPattern, as: UInt64.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Double as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 701 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Double")
      }
      guard let bitPattern = buffer.readInteger(as: UInt64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Double")
      }
      self = Double(bitPattern: bitPattern)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Double from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1022 {
      return 701
    }
    throw PostgreSQLError.codecError("Cannot get Double element type oid from \(pgArrayTypeOid)")
  }
}

extension Decimal: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private enum Sign: Int16 {
    case positive = 0
    case negative = 16384
    case nan = -16384
    case posInfinity = -12288
    case negInfinity = -4096
  }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 1700 {
      let ndigits: Int16
      let weight: Int16
      let sign: Sign
      var dscale: Int16
      var digits: [UInt16] = []

      if self.isNaN {
        (ndigits, weight, sign, dscale) = (0, 0, .nan, 0)
      } else if self.isZero {
        (ndigits, weight, sign, dscale) = (0, 0, .positive, 0)
      } else {
        let parts = String(describing: self).split(separator: ".")
        let signedIntegerPart = parts[0]
        let exp =
          signedIntegerPart.hasPrefix("-") ? signedIntegerPart.dropFirst() : signedIntegerPart
        let frac = parts.count > 1 ? parts[1] : ""

        let expDigits = stride(from: 0, to: exp.count, by: 4).map { i in
          let end = exp.index(exp.endIndex, offsetBy: -i)
          let start = exp.index(end, offsetBy: -4, limitedBy: exp.startIndex) ?? exp.startIndex
          return String(exp[start..<end])
        }.reversed()
        let fracDigits = stride(from: 0, to: frac.count, by: 4).map { i in
          let start = frac.index(frac.startIndex, offsetBy: i)
          let end = frac.index(start, offsetBy: 4, limitedBy: frac.endIndex) ?? frac.endIndex
          let digit = String(frac[start..<end])
          return digit.padding(toLength: 4, withPad: "0", startingAt: 0)
        }

        digits = (expDigits + fracDigits).map { UInt16($0)! }
        ndigits = Int16(digits.count)
        weight = Int16(expDigits.count - 1)
        sign = self.isSignMinus ? .negative : .positive
        dscale = Int16(frac.count)
      }
      buffer.writeInteger(Int32(ndigits) * 2 + 8, as: Int32.self)
      buffer.writeInteger(ndigits, as: Int16.self)
      buffer.writeInteger(weight, as: Int16.self)
      buffer.writeInteger(sign.rawValue, as: Int16.self)
      buffer.writeInteger(dscale, as: Int16.self)
      for digit in digits {
        buffer.writeInteger(digit, as: UInt16.self)
      }

    } else {
      throw PostgreSQLError.codecError("Cannot encode Decimal as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 1700 {
      guard buffer.readInteger(as: Int32.self) != nil else {
        throw PostgreSQLError.codecError("Invalid data for Decimal")
      }
      guard let ndigits = buffer.readInteger(as: Int16.self),
        let weight = buffer.readInteger(as: Int16.self),
        let sign = buffer.readInteger(as: Int16.self),
        let sign = Sign(rawValue: sign),
        buffer.readInteger(as: Int16.self) != nil  // dscale
      else {
        throw PostgreSQLError.codecError("Invalid data for Decimal")
      }
      var digits = [UInt16]()
      for _ in 0..<ndigits {
        guard let digit = buffer.readInteger(as: UInt16.self) else {
          throw PostgreSQLError.codecError("Invalid data for Decimal")
        }
        digits.append(digit)
      }
      if case .posInfinity = sign, case .negInfinity = sign {
        throw PostgreSQLError.codecError("Decimal infinity unsupported")
      } else if case .nan = sign {
        self = Decimal.nan
      } else if ndigits == 0 {
        self = Decimal.zero
      } else {
        let unsignedString = digits.enumerated().map {
          String(format: "%04d", $1) + ($0 == weight ? "." : "")
        }.joined()
        guard let unsigned = Decimal(string: unsignedString) else {
          throw PostgreSQLError.codecError("Invalid data for Decimal")
        }
        if sign == .negative {
          self = -unsigned
        } else {
          self = unsigned
        }
      }
    } else {
      throw PostgreSQLError.codecError("Cannot decode Decimal from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1231 {
      return 1700
    }
    throw PostgreSQLError.codecError("Cannot get Decimal element type oid from \(pgArrayTypeOid)")
  }
}

extension Date: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 8 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 1114 || typeOid == 1184 {
      let epochDifference: TimeInterval = 946_684_800  // PostgreSQL epoch offset for timestamp without time zone
      let microseconds = Int64((timeIntervalSince1970 - epochDifference) * 1_000_000)

      buffer.writeInteger(Self.pgDataLength)
      buffer.writeInteger(microseconds, as: Int64.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Date as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 1114 || pgTypeOid == 1184 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Date")
      }
      guard let microseconds = buffer.readInteger(as: Int64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Date")
      }
      let epochDifference: TimeInterval = 946_684_800
      self = Date(
        timeIntervalSince1970: TimeInterval(microseconds) / 1_000_000 + epochDifference)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Date from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1185 {
      return 1184
    } else if pgArrayTypeOid == 1115 {
      return 1114
    }
    throw PostgreSQLError.codecError("Cannot get Date element type oid from \(pgArrayTypeOid)")
  }
}

extension UUID: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 16 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 2950 {
      let b = uuid
      buffer.writeInteger(Self.pgDataLength)
      buffer.writeBytes([
        b.0, b.1, b.2, b.3, b.4, b.5, b.6, b.7,
        b.8, b.9, b.10, b.11, b.12, b.13, b.14, b.15,
      ])
    } else {
      throw PostgreSQLError.codecError("Cannot encode UUID as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 2950 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for UUID")
      }
      guard let b = buffer.readBytes(length: 16) else {
        throw PostgreSQLError.codecError("Invalid data for UUID")
      }
      self = .init(
        uuid: (
          b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
          b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
        ))
    } else {
      throw PostgreSQLError.codecError("Cannot decode UUID from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 2951 {
      return 2950
    }
    throw PostgreSQLError.codecError("Cannot get UUID element type oid from \(pgArrayTypeOid)")
  }
}

extension Optional: PostgreSQLEncodable where Wrapped: PostgreSQLEncodable {
  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if let value = self {
      try value.encode(typeOid: typeOid, buffer: &buffer)
    } else {
      buffer.writeInteger(-1, as: Int32.self)
    }
  }
}

extension Optional: PostgreSQLDecodable where Wrapped: PostgreSQLDecodable {
  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    guard let length = buffer.getInteger(at: buffer.readerIndex, as: Int32.self) else {
      throw PostgreSQLError.codecError("Invalid data for Optional")
    }
    if length == -1 {
      buffer.moveReaderIndex(forwardBy: 4)
      self = nil
    } else {
      self = try Wrapped(pgTypeOid: pgTypeOid, buffer: &buffer)
    }
  }
}

extension Optional: PostgreSQLCodableArrayElement where Wrapped: PostgreSQLCodableArrayElement {
  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    return try Wrapped.pgArrayElemTypeOid(pgArrayTypeOid: pgArrayTypeOid)
  }
}

extension Array: PostgreSQLEncodable
where Element: PostgreSQLCodableArrayElement, Element: PostgreSQLEncodable {
  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    let elementTypeOid = try Element.pgArrayElemTypeOid(pgArrayTypeOid: typeOid)
    let arrayShape = [Int32(self.count)]
    let arrayNdim = Int32(arrayShape.count)
    let elementHasNull = self.contains { val in
      return if case .none = val as Any? { true } else { false }
    }

    var body = ByteBuffer()
    body.writeInteger(arrayNdim, as: Int32.self)
    body.writeInteger(elementHasNull ? 1 : 0, as: Int32.self)
    body.writeInteger(elementTypeOid, as: Int32.self)
    for dim in arrayShape {
      body.writeInteger(dim, as: Int32.self)
      body.writeInteger(1, as: Int32.self)
    }
    for element in self {
      try element.encodeElem(pgArrayTypeOid: typeOid, buffer: &body)
    }

    buffer.writeInteger(Int32(body.readableBytes), as: Int32.self)
    buffer.writeBuffer(&body)
  }
}

extension Array: PostgreSQLDecodable
where Element: PostgreSQLCodableArrayElement, Element: PostgreSQLDecodable {
  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    guard buffer.readInteger(as: Int32.self) != nil,  // size
      let ndim: Int32 = buffer.readInteger(as: Int32.self),
      buffer.readInteger(as: Int32.self) != nil,  // frags
      buffer.readInteger(as: Int32.self) != nil,  // elemTypeOid
      let count = buffer.readInteger(as: Int32.self),
      let lbound = buffer.readInteger(as: Int32.self)
    else {
      throw PostgreSQLError.codecError("Invalid array data")
    }
    guard ndim == 1, lbound == 1 else {
      throw PostgreSQLError.codecError("Invalid 1dim array data")
    }
    let elements = try (0..<count).map { _ in
      try Element?(pgArrayTypeOid: pgTypeOid, buffer: &buffer)
    }
    self = elements as! [Element]
  }
}

extension PostgreSQLCodableArrayElement where Self: PostgreSQLEncodable {
  public func encodeElem(pgArrayTypeOid: Int32, buffer: inout ByteBuffer) throws {
    let elemTypeOid = try Self.pgArrayElemTypeOid(pgArrayTypeOid: pgArrayTypeOid)
    try self.encode(typeOid: elemTypeOid, buffer: &buffer)
  }
}

extension PostgreSQLCodableArrayElement where Self: PostgreSQLDecodable {
  public init(pgArrayTypeOid: Int32, buffer: inout ByteBuffer) throws {
    let elemTypeOid = try Self.pgArrayElemTypeOid(pgArrayTypeOid: pgArrayTypeOid)
    self = try .init(pgTypeOid: elemTypeOid, buffer: &buffer)
  }
}

// MARK: - SCRAM-SHA-256

class ScramSha256Authenticator {
  let username: String
  let passwordData: SymmetricKey
  let clientNonce: String

  var saltedPasswordKey: SymmetricKey?
  var authMessage: String?
  var combinedNonce: String?

  init(username: String, password: String) throws {
    let randomBytes: [UInt8] = SymmetricKey(size: .bits256).withUnsafeBytes { Array($0) }

    guard let passwordData = password.data(using: .utf8) else {
      throw ScramSha256AuthenticatorError(message: "Failed to encode password to UTF-8 Data")
    }

    self.username = username
    self.passwordData = SymmetricKey(data: passwordData)
    self.clientNonce = Data(randomBytes).base64EncodedString()
  }

  func formatClientFirstMessage() -> String {
    let saslUsername = escapeSaslString(username)
    let clientFirstMessageBare = "n=\(saslUsername),r=\(clientNonce)"
    return "n,,\(clientFirstMessageBare)"
  }

  func handleServerFirstMessage(_ serverFirstMessage: String) throws {
    let parts = serverFirstMessage.split(separator: ",")
    var serverParams: [String: String] = [:]

    for part in parts {
      let keyValue = part.split(separator: "=", maxSplits: 1)
      if keyValue.count == 2 {
        serverParams[String(keyValue[0])] = String(keyValue[1])
      }
    }

    guard let combinedNonce = serverParams["r"] else {
      throw ScramSha256AuthenticatorError(
        message: "Server nonce (r) missing in server-first-message")
    }
    guard let saltBase64 = serverParams["s"] else {
      throw ScramSha256AuthenticatorError(
        message: "Salt (s) missing in server-first-message")
    }
    guard let iterationsStr = serverParams["i"], let iterations = Int(iterationsStr) else {
      throw ScramSha256AuthenticatorError(
        message: "Iterations (i) missing in server-first-message or not an integer")
    }
    guard iterations > 0 else {
      throw ScramSha256AuthenticatorError(
        message: "Iterations (i) must be a positive integer")
    }
    guard combinedNonce.starts(with: clientNonce) else {
      throw ScramSha256AuthenticatorError(
        message: "Server nonce (r) does not start with client nonce")
    }

    let saslUsername = escapeSaslString(username)
    let clientFirstMessageBare = "n=\(saslUsername),r=\(clientNonce)"
    let clientFinalWithoutProof = "c=biws,r=\(combinedNonce)"
    let authMessage = "\(clientFirstMessageBare),\(serverFirstMessage),\(clientFinalWithoutProof)"

    guard let saltData = Data(base64Encoded: saltBase64) else {
      throw ScramSha256AuthenticatorError(
        message: "Invalid base64 encoding for salt")
    }

    let derivedKey = pbkdf2SHA256(
      pass: passwordData,
      salt: saltData,
      iterations: iterations,
      outLen: SHA256.byteCount
    )

    self.saltedPasswordKey = SymmetricKey(data: derivedKey)
    self.authMessage = authMessage
    self.combinedNonce = combinedNonce
  }

  func formatClientFinalMessage() throws -> String {
    guard
      let saltedPasswordKey = saltedPasswordKey,
      let authMessage = authMessage,
      let combinedNonce = combinedNonce
    else {
      throw ScramSha256AuthenticatorError(
        message:
          "Cannot format client final message: state is incomplete (call handleServerFirstMessage first)."
      )
    }

    let clientKeyMessage = "Client Key".data(using: .utf8)!
    let clientKeyMac = HMAC<SHA256>.authenticationCode(
      for: clientKeyMessage, using: saltedPasswordKey)

    let clientKeyData = Data(clientKeyMac)
    let storedKeyDigest = SHA256.hash(data: clientKeyData)
    let storedKey = SymmetricKey(data: storedKeyDigest)

    guard let authMessageData = authMessage.data(using: .utf8) else {
      throw ScramSha256AuthenticatorError(message: "Failed to encode authMessage to UTF-8 Data")
    }
    let clientSignatureMac = HMAC<SHA256>.authenticationCode(for: authMessageData, using: storedKey)
    let clientSignatureData = Data(clientSignatureMac)

    let clientProofData = xor(clientKeyData, clientSignatureData)
    let clientProofBase64 = clientProofData.base64EncodedString()

    let clientFinalWithoutProof = "c=biws,r=\(combinedNonce)"
    return "\(clientFinalWithoutProof),p=\(clientProofBase64)"
  }

  func handleServerFinalMessage(_ serverFinalMessage: String) throws {
    guard
      let saltedPasswordKey = saltedPasswordKey,
      let authMessage = authMessage
    else {
      throw ScramSha256AuthenticatorError(
        message: "Cannot handle server final message: state is incomplete.")
    }

    if serverFinalMessage.starts(with: "e=") {
      let errorMessage = serverFinalMessage.dropFirst(2)
      throw ScramSha256AuthenticatorError(
        message: "SCRAM Authentication failed on server: \(errorMessage)")
    }

    let parts = serverFinalMessage.split(separator: ",", maxSplits: 1)
    guard parts.first?.starts(with: "v=") ?? false else {
      throw ScramSha256AuthenticatorError(
        message: "Server signature (v=) missing or invalid in server-final-message")
    }

    let serverSignatureBase64 = String(parts[0].dropFirst(2))

    guard let serverSignatureData = Data(base64Encoded: serverSignatureBase64) else {
      throw ScramSha256AuthenticatorError(
        message: "Invalid base64 encoding for server signature (v)")
    }

    let serverKeyMessage = "Server Key".data(using: .utf8)!
    let serverKeyMac = HMAC<SHA256>.authenticationCode(
      for: serverKeyMessage, using: saltedPasswordKey)
    let serverKey = SymmetricKey(data: serverKeyMac)

    guard let authMessageData = authMessage.data(using: .utf8) else {
      throw ScramSha256AuthenticatorError(message: "Failed to encode authMessage to UTF-8 Data")
    }
    let expectedServerSignatureMac = HMAC<SHA256>.authenticationCode(
      for: authMessageData, using: serverKey)
    let expectedServerSignatureData = Data(expectedServerSignatureMac)

    guard serverSignatureData == expectedServerSignatureData else {
      throw ScramSha256AuthenticatorError(
        message: "Server signature verification failed. Server proof is incorrect.")
    }
  }

  private func escapeSaslString(_ string: String) -> String {
    return string.replacingOccurrences(of: "=", with: "=3D")
      .replacingOccurrences(of: ",", with: "=2C")
  }

  private func xor(_ data1: Data, _ data2: Data) -> Data {
    guard data1.count == data2.count else {
      fatalError("Data lengths do not match for XOR operation")
    }
    var result = Data(count: data1.count)
    for i in 0..<data1.count {
      result[i] = data1[i] ^ data2[i]
    }
    return result
  }

  private func pbkdf2SHA256(pass: SymmetricKey, salt: Data, iterations: Int, outLen: Int) -> Data {
    let hashLength = 32
    let blocks = (outLen + hashLength - 1) / hashLength
    var derivedKey = Data()

    for block in 1...blocks {
      var saltAndCounter = salt
      var counter = UInt32(block).bigEndian
      withUnsafeBytes(of: &counter) { counterBytes in
        saltAndCounter.append(contentsOf: counterBytes)
      }

      var U = Data(HMAC<SHA256>.authenticationCode(for: saltAndCounter, using: pass))
      var T = Data(U)

      for _ in 1..<iterations {
        U = Data(HMAC<SHA256>.authenticationCode(for: U, using: pass))
        T = xor(T, U)
      }
      derivedKey.append(contentsOf: T)
    }
    return derivedKey.prefix(outLen)
  }
}

public struct ScramSha256AuthenticatorError: Error {
  let message: String
}

// MARK: - Pool

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
      if await connection.isClosed() {
        connections.removeValue(forKey: ObjectIdentifier(connection))
        continue
      }
      return connection
    }

    if connections.count < maxConnections {
      let connection = PostgreSQLConnection(eventLoopGroup: eventLoopGroup)
      connections[ObjectIdentifier(connection)] = connection

      try await connection.connect(configs: configuration)
      return connection
    }

    let promise = eventLoopGroup.next().makePromise(of: PostgreSQLConnection.self)
    waiters.append(promise)
    if let timeout = timeout {
      Task {
        try await Task.sleep(for: timeout)
        promise.fail(PostgreSQLError.clientTimeout)
      }
    }
    return try await promise.futureResult.get()
  }

  func release(_ connection: PostgreSQLConnection) async {
    if await connection.isClosed() {
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
