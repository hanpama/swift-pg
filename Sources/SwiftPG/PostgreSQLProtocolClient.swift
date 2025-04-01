import NIO
import NIOSSL

final actor PostgreSQLProtocolClient {
  private let messages: AsyncThrowingStream<PostgreSQLBackendMessage?, Error>
  private let bootstrap: ClientBootstrap
  private var channel: Channel?
  private var state: State = .created

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
    var continuation: AsyncThrowingStream<PostgreSQLBackendMessage?, Error>.Continuation?
    let messages = AsyncThrowingStream { continuation = $0 }
    let messageHandler = PostgreSQLMessageHandler(continuation: continuation!)

    self.messages = messages
    self.bootstrap = ClientBootstrap(group: loop)
      .channelOption(.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
      .channelOption(.autoRead, value: false)
      .channelInitializer { channel in
        channel.pipeline.addHandler(messageHandler)
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
    let sslContext = try NIOSSLContext(configuration: tlsConfiguration)
    let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: host)
    try await channel!.pipeline.addHandler(sslHandler, position: .first).get()
  }

  func send(_ message: PostgreSQLFrontendMessage) async throws {
    // print("Sending message: \(message)")
    let buffer: ByteBuffer = encodeMessage(message: message)
    try await channel!.writeAndFlush(buffer)
  }

  func close() async throws {
    state = .disconnected
    try await channel!.close()
  }

  func isClosed() -> Bool {
    return state == .disconnected
  }

  func receive() async throws -> PostgreSQLBackendMessage? {
    for try await message in messages {
      if let message = message {
        return message
      } else {
        channel!.read()
      }
    }
    return nil
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
      message = .errorResponse(decodeServerErrorNoticeMessage(buffer: &buffer))
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

  private static func decodeServerErrorNoticeMessage(buffer: inout ByteBuffer)
    -> PostgreSQLErrorNoticeMessage
  {
    var errorFields: PostgreSQLErrorNoticeMessage = []
    while let fieldType = buffer.readInteger(as: UInt8.self), fieldType != 0 {
      guard let fieldValue = buffer.readNullTerminatedString() else {
        continue
      }
      let field: PostgreSQLErrorNoticeMessageField
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
    return errorFields
  }

  private final class PostgreSQLMessageHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let continuation: AsyncThrowingStream<PostgreSQLBackendMessage?, Error>.Continuation

    init(continuation: AsyncThrowingStream<PostgreSQLBackendMessage?, Error>.Continuation) {
      self.continuation = continuation
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
        continuation.yield(message)
      }
      continuation.yield(nil)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
      continuation.finish(throwing: error)
    }

    func channelInactive(context: ChannelHandlerContext) {
      continuation.finish()
    }
  }
}
