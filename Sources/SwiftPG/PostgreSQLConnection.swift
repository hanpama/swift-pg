import CommonCrypto
import Crypto
import Foundation
import Logging
import NIO

// MARK: - Types

enum PostgreSQLFrontendMessage {
  case startupMessage(_ user: String, _ database: String)
  case saslInitialResponse(mechanism: String, initialResponse: String?)
  case saslResponse(_ response: String)
  case parse(_ statementName: String, _ sql: String)
  case bind(
    portalName: String,
    statementName: String,
    parameterFormatCodes: [Int16],
    parameterValues: [[UInt8]?],
    resultColumnFormatCodes: [Int16]
  )
  case describe(variant: UInt8, _ name: String)
  case execute(_ portalName: String)
  case close(variant: UInt8, _ name: String)
  case flush
  case sync
  case terminate
}

enum PostgreSQLBackendMessage {
  case authenticationOk
  case authenticationSasl(_ mechanisms: [String])
  case authenticationSaslContinue(String)
  case authenticationSaslFinal(String)
  case commandComplete(_ commandTag: String)
  case dataRow(_ columns: [[UInt8]])
  case errorResponse(_ errorFields: [PostgreSQLMessageField])
  case readyForQuery(_ transactionStatus: UInt8)
  case rowDescription(_ fields: [PostgreSQLFieldDescription])
  case parameterDescription(_ parameterOIDs: [Int32])
  case parseComplete
  case bindComplete
}

enum PostgreSQLMessageField {
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

struct PostgreSQLFieldDescription {
  let name: String
  let tableOID: Int32
  let columnAttr: Int16
  let dataTypeOID: Int32
  let dataTypeSize: Int16
  let typeModifier: Int32
  let formatCode: Int16
}

struct PostgreSQLBoundParameter {
  let formatCode: Int16
  let value: [UInt8]
}

extension ByteBuffer {
  mutating func readNullTerminatedString() -> String? {
    let startIndex = readerIndex
    var length = 0

    while let byte = getInteger(at: startIndex + length, as: UInt8.self) {
      if byte == 0 {
        break
      }
      length += 1
    }
    guard let string = getString(at: startIndex, length: length) else {
      return nil
    }
    moveReaderIndex(forwardBy: length + 1)  // Move reader index past the null terminator

    return string
  }
}

// MARK: - Protocol Client

final class PostgreSQLProtocolClient: Sendable {
  let messages: AsyncThrowingStream<PostgreSQLBackendMessage, Error>

  private let channel: Channel

  enum ConnectOptions {
    case hostPort(_ host: String, _ port: Int)
    case unixDomainSocket(path: String)
  }

  init(_ loop: EventLoop, _ connectOpts: ConnectOptions) async throws {
    var continuation: AsyncThrowingStream<PostgreSQLBackendMessage, Error>.Continuation?
    self.messages = AsyncThrowingStream { continuation = $0 }

    let bootstrap = ClientBootstrap(group: loop)
      .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
      .channelInitializer { channel in
        channel.pipeline.addHandler(PostgreSQLMessageHandler(continuation!))
      }

    switch connectOpts {
    case .hostPort(let host, let port):
      self.channel = try await bootstrap.connect(host: host, port: port).get()
    case .unixDomainSocket(let path):
      self.channel = try await bootstrap.connect(unixDomainSocketPath: path).get()
    }
  }

  func close() async throws {
    try await channel.close()
  }

  func send(_ message: PostgreSQLFrontendMessage) async throws {
    print("Sending message: \(message)")
    var buffer = ByteBuffer()

    switch message {
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

    case .bind(
      let portalName,
      let statementName,
      let parameterFormatCodes,
      let parameterValues,
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
      body.writeInteger(Int16(parameterValues.count))
      for value in parameterValues {
        if let value = value {
          body.writeInteger(Int32(value.count))
          body.writeBytes(value)
        } else {
          body.writeInteger(Int32(-1))
        }
      }
      body.writeInteger(Int16(resultColumnFormatCodes.count))
      for code in resultColumnFormatCodes {
        body.writeInteger(code)
      }

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "B"))
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

    case .execute(let portalName):
      var body = ByteBuffer()
      if portalName.isEmpty {
        body.writeInteger(0, as: UInt8.self)
      } else {
        body.writeString(portalName)
        body.writeInteger(0, as: UInt8.self)
      }
      body.writeInteger(Int32(0))  // No row limit

      let messageLength = Int32(body.readableBytes + 4)
      buffer.writeInteger(UInt8(ascii: "E"))
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

    case .flush:
      buffer.writeInteger(UInt8(ascii: "H"))
      buffer.writeInteger(Int32(4))

    case .sync:
      buffer.writeInteger(UInt8(ascii: "S"))
      buffer.writeInteger(Int32(4))

    case .terminate:
      buffer.writeInteger(UInt8(ascii: "X"))
      buffer.writeInteger(Int32(4))
    }

    try await channel.writeAndFlush(buffer)
    // print("Sent")
  }

  private final class PostgreSQLMessageHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer
    private let continuation: AsyncThrowingStream<PostgreSQLBackendMessage, Error>.Continuation

    init(_ continuation: AsyncThrowingStream<PostgreSQLBackendMessage, Error>.Continuation) {
      self.continuation = continuation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
      var buffer: ByteBuffer = unwrapInboundIn(data)
      while buffer.readableBytes >= 5 {  // Process all complete messages in the buffer
        if let message = receiveBackendMessage(&buffer) {
          // print("Received message: \(message)")
          continuation.yield(message)
        }
      }
    }
    private func receiveBackendMessage(_ buffer: inout ByteBuffer) -> PostgreSQLBackendMessage? {
      guard let messageType = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self),
        let messageLength = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self)
      else {
        return nil
      }

      let dataLength = Int(messageLength) - 4  // 4 bytes for the length itself
      if buffer.readableBytes < 5 + dataLength {  // Not enough data yet
        return nil
      }

      buffer.moveReaderIndex(forwardBy: 5)  // Consume the message type and length

      let message: PostgreSQLBackendMessage?
      print("Receiving message type: \(messageType)")

      switch messageType {
      case 0x52:  // 'R'
        let authMessageType = buffer.readInteger(as: Int32.self)!
        switch authMessageType {
        case 0:
          message = .authenticationOk
        case 10:
          message = .authenticationSasl(
            buffer.readNullTerminatedString()!.split(separator: ",").map { String($0) })
        case 11:
          message = .authenticationSaslContinue(buffer.readString(length: dataLength - 4)!)
        case 12:
          message = .authenticationSaslFinal(buffer.readString(length: dataLength - 4)!)
        default:
          buffer.moveReaderIndex(forwardBy: dataLength - 4)  // TODO: should throw an error
          message = nil
        }
      case 0x43:  // 'C'
        message = .commandComplete(buffer.readNullTerminatedString()!)
      case 0x45:  // 'E'
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
      case 0x5A:  // 'Z'
        message = .readyForQuery(buffer.readInteger(as: UInt8.self)!)
      case 0x54:  // 'T'
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
      case 0x74:  // 't'
        message = .parameterDescription(
          (0..<buffer.readInteger(as: Int16.self)!).map { _ in
            buffer.readInteger(as: Int32.self)!
          }
        )
      case 0x44:  // 'D'
        message = .dataRow(
          (0..<buffer.readInteger(as: Int16.self)!).map { _ in
            let length = buffer.readInteger(as: Int32.self)!
            if length == -1 {
              return []
            } else {
              return buffer.readBytes(length: Int(length))!.map { $0 }
            }
          }
        )
      case 0x31:  // '1'
        message = .parseComplete
      case 0x32:  // '2'
        message = .bindComplete
      default:
        // print("Unknown message type: \(messageType)")
        buffer.moveReaderIndex(forwardBy: dataLength)
        message = nil
      }
      return message
    }
  }
}

// MARK: - Connection

final actor PostgreSQLConnection {
  let protocolClient: PostgreSQLProtocolClient

  private init(protocolClient: PostgreSQLProtocolClient) {
    self.protocolClient = protocolClient
  }

  static func connect(
    eventLoopGroup: EventLoopGroup,
    configuration: PostgreSQLConnectionConfiguration
  ) async throws -> PostgreSQLConnection {
    let eventLoop = eventLoopGroup.next()

    let protocolClient = try await PostgreSQLProtocolClient(
      eventLoop, .hostPort(configuration.host, configuration.port)
    )

    try await protocolClient.send(.startupMessage(configuration.username, configuration.database))

    var scramSha256Authenticator: ScramSha256Authenticator?

    loop: for try await message in protocolClient.messages {
      switch message {
      case .authenticationOk:
        break

      case .authenticationSasl(let mechanisms):
        guard mechanisms.contains("SCRAM-SHA-256") else {
          throw PostgreSQLError.codecError("No supported SASL mechanism found")
        }
        scramSha256Authenticator = ScramSha256Authenticator(
          username: configuration.username, password: configuration.password)

        try await protocolClient.send(
          .saslInitialResponse(
            mechanism: "SCRAM-SHA-256",
            initialResponse: scramSha256Authenticator!.formatClientFirstMessage()
          )
        )

      case .authenticationSaslContinue(let challenge):
        guard let scramSha256Authenticator = scramSha256Authenticator else {
          throw PostgreSQLError.codecError("Unexpected SASL continue message")
        }
        try scramSha256Authenticator.handleServerFirstMessage(challenge)

        try await protocolClient.send(
          .saslResponse(scramSha256Authenticator.formatClientFinalMessage())
        )

      case .authenticationSaslFinal(let finalMessage):
        guard let scramSha256Authenticator = scramSha256Authenticator else {
          throw PostgreSQLError.codecError("Unexpected SASL final message")
        }
        try scramSha256Authenticator.handleServerFinalMessage(finalMessage)

      case .readyForQuery:
        break loop
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields: fields)
      default:
        break
      }
    }

    return PostgreSQLConnection(protocolClient: protocolClient)
  }

  func close() async throws {
    try await protocolClient.send(.terminate)
    try await protocolClient.close()
  }

  func query(_ sql: String, _ parameters: [PostgreSQLEncodable] = []) async throws
    -> [PostgreSQLRow]
  {
    let (_, rows) = try await query_(sql, [parameters])
    return rows
  }

  @discardableResult
  func execute(_ sql: String, _ parameters: [PostgreSQLEncodable] = []) async throws -> String {
    let (result, _) = try await query_(sql, [parameters])
    return result[0]
  }

  func batchQuery(_ sql: String, _ parameter_lists: [[PostgreSQLEncodable]]) async throws
    -> [PostgreSQLRow]
  {
    let (_, rows) = try await query_(sql, parameter_lists)
    return rows
  }

  @discardableResult
  func batchExecute(_ sql: String, _ parameter_lists: [[PostgreSQLEncodable]]) async throws
    -> [String]
  {
    let (result, _) = try await query_(sql, parameter_lists)
    return result
  }

  private func query_(_ sql: String, _ parameter_lists: [[PostgreSQLEncodable]]) async throws
    -> ([String], [PostgreSQLRow])
  {
    try await protocolClient.send(.parse("", sql))
    try await protocolClient.send(.describe(variant: 83, ""))
    try await protocolClient.send(.flush)

    var fields: [PostgreSQLFieldDescription]?
    var parameterOids: [Int32]?
    loop: for try await message in protocolClient.messages {
      switch message {
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
    for parameters in parameter_lists {
      try await protocolClient.send(
        .bind(
          portalName: "",
          statementName: "",
          parameterFormatCodes: [Int16](repeating: 1, count: parameters.count),
          parameterValues: zip(parameters, parameterOids ?? []).map {
            try? encodeParameter($0.0, typeOid: $0.1)
          },
          resultColumnFormatCodes: [Int16](repeating: 1, count: fields?.count ?? 0)
        ))
      try await protocolClient.send(.execute(""))
      try await protocolClient.send(.sync)
    }

    var commendTags: [String] = []
    var rows: [PostgreSQLRow] = []

    loop: for try await message in protocolClient.messages {
      switch message {
      case .dataRow(let columns):
        rows.append(.init(fields: fields!, columns: columns))
      case .commandComplete(let commandTag):
        commendTags.append(commandTag)
      case .readyForQuery:
        break loop
      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields: fields)
      default: break
      }
    }

    return (commendTags, rows)
  }

  private func encodeParameter(_ parameter: PostgreSQLEncodable, typeOid: Int32) throws -> [UInt8]?
  {
    return try parameter.encodeForPostgreSQL(postgreSQLTypeOid: typeOid)
  }
}

typealias PostgreSQLRows = AsyncThrowingStream<PostgreSQLRow, Error>

struct PostgreSQLRow {
  let fields: [PostgreSQLFieldDescription]
  let columns: [[UInt8]]
  // let columnLocs: [String: Int]

  func get<T>(at index: Int) throws -> T {
    return try get(T.self, at: index)
  }

  func get<T>(_ type: T.Type, at index: Int) throws -> T {
    if type == Any.self {
      throw PostgreSQLError.codecError("Cannot decode Any")
    }
    guard let type = type as? PostgreSQLDecodable.Type else {
      throw PostgreSQLError.codecError("Cannot decode \(type)")
    }
    return try type.init(
      postgreSQLTypeOid: fields[index].dataTypeOID, fromPostgreSQLData: columns[index]) as! T
  }
}

enum PostgreSQLError: Error {
  case transportError(String)
  case databaseError(fields: [PostgreSQLMessageField])
  case codecError(String)
}

struct PostgreSQLConnectionConfiguration {
  let host: String
  let port: Int
  let username: String
  let password: String
  let database: String
}

// MARK: - Codec

protocol PostgreSQLEncodable {
  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]?
}

protocol PostgreSQLDecodable {
  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData: [UInt8]) throws
}

protocol PostgreSQLEncodableArrayElement: PostgreSQLEncodable {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { get }
}

protocol PostgreSQLDecodableArrayElement: PostgreSQLDecodable {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { get }
}

typealias PostgreSQLCodable = PostgreSQLEncodable & PostgreSQLDecodable
typealias PostgreSQLCodableArrayElement = PostgreSQLEncodableArrayElement
  & PostgreSQLDecodableArrayElement

extension Bool: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1000: 16] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 16 {
      return [UInt8(self ? 1 : 0)]
    }
    throw PostgreSQLError.codecError("Cannot encode Bool as \(postgreSQLTypeOid)")
  }
  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData: [UInt8]) throws {
    if postgreSQLTypeOid == 16 {
      self = fromPostgreSQLData[0] != 0
    } else {
      throw PostgreSQLError.codecError("Cannot decode Bool from \(postgreSQLTypeOid)")
    }
  }
}

extension Int16: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1005: 21] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 21 {
      return withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode Int16 as \(postgreSQLTypeOid)")
  }

  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 21 {
      guard data.count == MemoryLayout<Self>.size else {
        throw PostgreSQLError.codecError("Invalid byte count for \(Self.self)")
      }
      self = data.withUnsafeBytes { $0.load(as: Self.self) }.bigEndian
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int16 from \(postgreSQLTypeOid)")
    }
  }
}

extension Int32: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1007: 23] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 23 {
      return withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode Int32 as \(postgreSQLTypeOid)")
  }

  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 23 {
      guard data.count == MemoryLayout<Self>.size else {
        throw PostgreSQLError.codecError("Invalid byte count for \(Self.self)")
      }
      self = data.withUnsafeBytes { $0.load(as: Self.self) }.bigEndian
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int32 from \(postgreSQLTypeOid)")
    }
  }
}

extension Int64: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1016: 20] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 20 {
      return withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode Int64 as \(postgreSQLTypeOid)")
  }

  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 20 {
      guard data.count == MemoryLayout<Self>.size else {
        throw PostgreSQLError.codecError("Invalid byte count for \(Self.self)")
      }
      self = data.withUnsafeBytes { $0.load(as: Self.self) }.bigEndian
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int64 from \(postgreSQLTypeOid)")
    }
  }
}

extension String: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1009: 25, 1015: 1043] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 25 || postgreSQLTypeOid == 1043 {
      return utf8.map { UInt8($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode String as \(postgreSQLTypeOid)")
  }
  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData: [UInt8]) throws {
    if postgreSQLTypeOid == 25 || postgreSQLTypeOid == 1043 {
      self = String(decoding: fromPostgreSQLData, as: UTF8.self)
    } else {
      throw PostgreSQLError.codecError("Cannot decode String from \(postgreSQLTypeOid)")
    }
  }
}

extension Float: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1021: 700] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 700 {
      return withUnsafeBytes(of: self.bitPattern.bigEndian) { Array($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode Float as \(postgreSQLTypeOid)")
  }

  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 700 {
      guard data.count == MemoryLayout<UInt32>.size else {
        throw PostgreSQLError.codecError("Invalid byte count for \(Self.self)")
      }
      let bitPattern = data.withUnsafeBytes { $0.load(as: UInt32.self) }.bigEndian
      self = Float(bitPattern: bitPattern)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Float from \(postgreSQLTypeOid)")
    }
  }
}

extension Double: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1022: 701] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 701 {
      return withUnsafeBytes(of: self.bitPattern.bigEndian) { Array($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode Double as \(postgreSQLTypeOid)")
  }

  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 701 {
      guard data.count == MemoryLayout<UInt64>.size else {
        throw PostgreSQLError.codecError("Invalid byte count for \(Self.self)")
      }
      let bitPattern = data.withUnsafeBytes { $0.load(as: UInt64.self) }.bigEndian
      self = Double(bitPattern: bitPattern)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Double from \(postgreSQLTypeOid)")
    }
  }
}

extension Date: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [1185: 1184, 1115: 1114] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 1114 || postgreSQLTypeOid == 1184 {
      let epochDifference: TimeInterval = 946_684_800  // PostgreSQL epoch offset for timestamp without time zone
      let microseconds = Int64((timeIntervalSince1970 - epochDifference) * 1_000_000)
      return withUnsafeBytes(of: microseconds.bigEndian) { Array($0) }
    }
    throw PostgreSQLError.codecError("Cannot encode Date as \(postgreSQLTypeOid)")
  }

  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 1114 || postgreSQLTypeOid == 1184 {
      guard data.count == MemoryLayout<Int64>.size else {
        throw PostgreSQLError.codecError("Invalid byte count for \(Self.self)")
      }
      let microseconds = data.withUnsafeBytes { $0.load(as: Int64.self) }.bigEndian

      let epochDifference: TimeInterval = 946_684_800
      self = Date(timeIntervalSince1970: TimeInterval(microseconds) / 1_000_000 + epochDifference)
      return
    }
    throw PostgreSQLError.codecError("Cannot decode Date from \(postgreSQLTypeOid)")
  }
}

extension UUID: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  static var postgreSQLArrayElementTypes: [Int32: Int32] { [2951: 2950] }

  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if postgreSQLTypeOid == 2950 {
      let b = uuid
      return [
        b.0, b.1, b.2, b.3, b.4, b.5, b.6, b.7,
        b.8, b.9, b.10, b.11, b.12, b.13, b.14, b.15,
      ]
    }
    throw PostgreSQLError.codecError("Cannot encode UUID as \(postgreSQLTypeOid)")
  }
  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if postgreSQLTypeOid == 2950 {
      let b = (
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]
      )
      self.init(uuid: b)
    } else {
      throw PostgreSQLError.codecError("Cannot decode UUID from \(postgreSQLTypeOid)")
    }
  }
}

extension Optional: PostgreSQLEncodable where Wrapped: PostgreSQLEncodable {
  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    switch self {
    case .none:
      return nil
    case .some(let wrapped):
      return try wrapped.encodeForPostgreSQL(postgreSQLTypeOid: postgreSQLTypeOid)
    }
  }
}

extension Optional: PostgreSQLDecodable where Wrapped: PostgreSQLDecodable {
  init?(postgreSQLTypeOid: Int32, fromPostgreSQLData data: [UInt8]) throws {
    if data.isEmpty {
      self = .none
    } else {
      self = try Wrapped(postgreSQLTypeOid: postgreSQLTypeOid, fromPostgreSQLData: data)
    }
  }
}

extension Array: PostgreSQLEncodable where Element: PostgreSQLEncodableArrayElement {
  func encodeForPostgreSQL(postgreSQLTypeOid: Int32) throws -> [UInt8]? {
    if let elementTypeOid = Element.postgreSQLArrayElementTypes[postgreSQLTypeOid] {
      var buffer = ByteBufferAllocator().buffer(capacity: 0)
      buffer.writeInteger(Int32(1))  // ndim
      buffer.writeInteger(Int32(0))  // flags
      buffer.writeInteger(elementTypeOid)
      buffer.writeInteger(Int32(self.count))
      buffer.writeInteger(Int32(1))

      for element in self {
        if let data = try element.encodeForPostgreSQL(postgreSQLTypeOid: elementTypeOid) {
          buffer.writeInteger(Int32(data.count))
          buffer.writeBytes(data)
        } else {
          buffer.writeInteger(Int32(-1))
        }
      }

      return buffer.readBytes(length: buffer.readableBytes)!.map { $0 }
    }
    throw PostgreSQLError.codecError("Cannot encode [\(Element.self)] as \(postgreSQLTypeOid)")
  }
}

extension Array: PostgreSQLDecodable where Element: PostgreSQLDecodableArrayElement {
  init(postgreSQLTypeOid: Int32, fromPostgreSQLData: [UInt8]) throws {
    if let elementTypeOid = Element.postgreSQLArrayElementTypes[postgreSQLTypeOid] {
      var buffer = ByteBufferAllocator().buffer(capacity: fromPostgreSQLData.count)
      buffer.writeBytes(fromPostgreSQLData)

      guard let ndim = buffer.readInteger(as: Int32.self),
        let flags = buffer.readInteger(as: Int32.self),
        let elementOid = buffer.readInteger(as: Int32.self),
        let count = buffer.readInteger(as: Int32.self),
        let lbound = buffer.readInteger(as: Int32.self)
      else {
        throw PostgreSQLError.codecError("Invalid array data")
      }
      guard ndim == 1, flags == 0, elementOid == elementTypeOid, lbound == 1 else {
        throw PostgreSQLError.codecError("Invalid array data")
      }

      var elements: [Element?] = []
      for _ in 0..<count {
        guard let length = buffer.readInteger(as: Int32.self) else {
          throw PostgreSQLError.codecError("Invalid array data")
        }
        if length == -1 {
          elements.append(nil as Element?)
        } else {
          let data = buffer.readBytes(length: Int(length))!.map { $0 }
          let element = try Element.init(
            postgreSQLTypeOid: elementTypeOid, fromPostgreSQLData: data)
          elements.append(element)
        }
      }
      self = elements as! [Element]
    } else {
      throw PostgreSQLError.codecError("Cannot decode [\(Element.self)] from \(postgreSQLTypeOid)")
    }
  }
}

// MARK: - SCRAM-SHA-256

class ScramSha256Authenticator {
  let username: String
  let password: String
  let clientNonce: String

  var saltedPassword: [UInt8]?
  var authMessage: String?
  var combinedNonce: String?

  init(username: String, password: String) {
    var randomBytes = [UInt8](repeating: 0, count: 24)
    let res = CCRandomGenerateBytes(&randomBytes, randomBytes.count)

    precondition(res == kCCSuccess, "Failed to generate random bytes")

    self.username = username
    self.password = password
    self.clientNonce = Data(randomBytes).base64EncodedString()
  }

  func formatClientFirstMessage() -> String {
    let clientFirstMessageBare = "n=\(username),r=\(clientNonce)"
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
        message: "Iterations (i) missing in server-first-message")
    }
    guard combinedNonce.starts(with: clientNonce) else {
      throw ScramSha256AuthenticatorError(
        message: "Server nonce (r) does not start with client nonce")
    }

    let clientFirstMessageBare = "n=\(username),r=\(clientNonce)"
    let clientFinalWithoutProof = "c=biws,r=\(combinedNonce)"
    let authMessage = "\(clientFirstMessageBare),\(serverFirstMessage),\(clientFinalWithoutProof)"

    guard let saltData = Data(base64Encoded: saltBase64) else {
      throw ScramSha256AuthenticatorError(
        message: "Invalid base64 encoding for salt")
    }

    let passwordData = password.data(using: .utf8)!
    var saltedPassword = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))

    let result = CCKeyDerivationPBKDF(
      CCPBKDFAlgorithm(kCCPBKDF2),
      password, passwordData.count,
      [UInt8](saltData), saltData.count,
      CCPBKDFAlgorithm(kCCPRFHmacAlgSHA256),
      UInt32(iterations),
      &saltedPassword, saltedPassword.count
    )
    guard result == kCCSuccess else {
      throw ScramSha256AuthenticatorError(
        message: "PBKDF2 computation failed")
    }

    self.saltedPassword = saltedPassword
    self.authMessage = authMessage
    self.combinedNonce = combinedNonce
  }

  func formatClientFinalMessage() throws -> String {
    guard
      let saltedPassword = saltedPassword,
      let authMessage = authMessage,
      let combinedNonce = combinedNonce
    else {
      throw ScramSha256AuthenticatorError(
        message: "Salted password, auth message, or combined nonce missing")
    }

    // Compute client key
    let clientKeyMessage = "Client Key".data(using: .utf8)!
    var clientKey = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
    CCHmac(
      CCHmacAlgorithm(kCCHmacAlgSHA256), saltedPassword, saltedPassword.count,
      [UInt8](clientKeyMessage), clientKeyMessage.count, &clientKey)

    // Compute stored key
    var storedKey = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
    CC_SHA256(clientKey, CC_LONG(clientKey.count), &storedKey)

    // Compute client signature
    let authMessageData = authMessage.data(using: .utf8)!
    var clientSignature = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
    CCHmac(
      CCHmacAlgorithm(kCCHmacAlgSHA256), storedKey, storedKey.count, [UInt8](authMessageData),
      authMessageData.count, &clientSignature)

    // Compute client proof
    var clientProof = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
    for i in 0..<clientProof.count {
      clientProof[i] = clientKey[i] ^ clientSignature[i]
    }
    let clientProofBase64 = Data(clientProof).base64EncodedString()

    return "c=biws,r=\(combinedNonce),p=\(clientProofBase64)"
  }

  func handleServerFinalMessage(_ serverFinalMessage: String) throws {
    if serverFinalMessage.starts(with: "e=") {
      throw ScramSha256AuthenticatorError(
        message: "SASL authentication error: \(serverFinalMessage)")
    }

    // Extract server signature
    let parts = serverFinalMessage.split(separator: ",")
    var serverParams: [String: String] = [:]

    for part in parts {
      if part.contains("=") {
        let keyValue = part.split(separator: "=", maxSplits: 1)
        if keyValue.count == 2 {
          serverParams[String(keyValue[0])] = String(keyValue[1])
        }
      }
    }

    guard let serverSignatureBase64 = serverParams["v"] else {
      throw ScramSha256AuthenticatorError(
        message: "Server signature (v) missing in server-final-message")
    }

    // Compute server key
    let serverKeyMessage = "Server Key".data(using: .utf8)!
    var serverKey = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
    CCHmac(
      CCHmacAlgorithm(kCCHmacAlgSHA256), saltedPassword!, saltedPassword!.count,
      [UInt8](serverKeyMessage), serverKeyMessage.count, &serverKey)

    // Compute expected server signature
    let authMessageData = authMessage!.data(using: .utf8)!
    var expectedServerSignature = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
    CCHmac(
      CCHmacAlgorithm(kCCHmacAlgSHA256), serverKey, serverKey.count, [UInt8](authMessageData),
      authMessageData.count, &expectedServerSignature)

    let expectedServerSignatureBase64 = Data(expectedServerSignature).base64EncodedString()

    // Verify signatures match
    guard serverSignatureBase64 == expectedServerSignatureBase64 else {
      throw ScramSha256AuthenticatorError(
        message: "Server signature does not match expected server signature")
    }
  }
}

struct ScramSha256AuthenticatorError: Error {
  let message: String
}
