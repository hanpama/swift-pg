import AsyncAlgorithms
@preconcurrency import NIO
import NIOSSL

final class PostgreSQLProtocolClient: Sendable {
  private let channel: Channel
  private let messageStream: AsyncThrowingChannel<PostgreSQLBackendMessage?, Error>
  private let outbound: NIOAsyncChannelOutboundWriter<PostgreSQLFrontendMessage>

  init(_ loop: EventLoop, _ configs: PostgreSQLConnectionConfigs) async throws {
    let messageStream = AsyncThrowingChannel<PostgreSQLBackendMessage?, Error>()

    let socketAddress: SocketAddress =
      switch configs.socketAddress {
      case .hostPort(let host, let port):
        try .makeAddressResolvingHost(host, port: port)
      case .unixDomainSocket(let path):
        try .init(unixDomainSocketPath: path)
      }

    let messageCodec = PostgreSQLMessageCodec()
    var handlers: [ChannelHandler] = [
      ByteToMessageHandler(messageCodec),
      MessageToByteHandler(messageCodec),
    ]
    if let sslHandler = try Self.getTLSHandler(configs: configs) {
      handlers.insert(sslHandler, at: 0)
    }

    let bootstrap = ClientBootstrap(group: loop)
      .channelInitializer({ $0.pipeline.addHandlers(handlers) })

    let channel = try await bootstrap.connect(to: socketAddress).get()
    let asyncChannel = try await channel.eventLoop.submit {
      try NIOAsyncChannel<PostgreSQLBackendMessage, PostgreSQLFrontendMessage>(
        wrappingChannelSynchronously: channel
      )
    }.get()

    let outboundPromise = loop.makePromise(
      of: NIOAsyncChannelOutboundWriter<PostgreSQLFrontendMessage>.self)

    Task {
      do {
        try await asyncChannel.executeThenClose { inbound, outbound in
          outboundPromise.succeed(outbound)
          for try await message in inbound {
            await messageStream.send(message)
          }
          messageStream.finish()
        }
      } catch {
        messageStream.fail(error)
        outboundPromise.fail(error)
      }
    }

    self.messageStream = messageStream
    self.outbound = try await outboundPromise.futureResult.get()
    self.channel = channel

    try await send(.startupMessage(configs.username, configs.database))
    try await authenticate(configs: configs)
  }

  func send(_ message: PostgreSQLFrontendMessage) async throws {
    try await outbound.write(message)
  }

  func close() async throws {
    try await channel.close()
  }

  func isClosed() -> Bool {
    return !channel.isActive
  }

  func receive() async throws -> PostgreSQLBackendMessage? {
    for try await message in messageStream {
      return message
    }
    return nil
  }

  private static func getTLSHandler(configs: PostgreSQLConnectionConfigs) throws
    -> NIOSSLClientHandler?
  {
    guard configs.sslmode != .disable else {
      return nil
    }
    var tlsConfig = TLSConfiguration.makeClientConfiguration()
    tlsConfig.applicationProtocols = ["postgresql"]

    if case .require = configs.sslmode {
      tlsConfig.certificateVerification = .none
    } else if case .verifyCA = configs.sslmode {
      tlsConfig.certificateVerification = .noHostnameVerification
    } else if case .verifyFull = configs.sslmode {
      tlsConfig.certificateVerification = .fullVerification
    }

    if let sslcert = configs.sslcert {
      let certs = try NIOSSLCertificate.fromPEMFile(sslcert)
      tlsConfig.certificateChain = certs.map { .certificate($0) }
    }
    if let sslkey = configs.sslkey {
      let privateKey = try NIOSSLPrivateKey(file: sslkey, format: .pem)
      tlsConfig.privateKey = .privateKey(privateKey)
    }
    if let sslrootcert = configs.sslrootcert {
      tlsConfig.additionalTrustRoots = [.file(sslrootcert)]
    }
    guard case let .hostPort(host, _) = configs.socketAddress else {
      throw PostgreSQLError.clientError("Host is required for TLS connections")
    }
    let sslContext = try NIOSSLContext(configuration: tlsConfig)
    let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: host)
    return sslHandler
  }

  private func authenticate(configs: PostgreSQLConnectionConfigs) async throws {
    var scramSha256Authenticator: ScramSha256Authenticator?

    loop: while true {
      switch try await receive() {
      case .authenticationOk:
        break loop

      case .authenticationSasl(let mechanisms):
        if mechanisms.contains("SCRAM-SHA-256") || mechanisms.contains("SCRAM-SHA-256-PLUS") {
          scramSha256Authenticator = try ScramSha256Authenticator(
            username: configs.username, password: configs.password)

          try await send(
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

        try await send(
          .saslResponse(scramSha256Authenticator.formatClientFinalMessage())
        )

      case .authenticationSaslFinal(let finalMessage):
        guard let scramSha256Authenticator = scramSha256Authenticator else {
          throw PostgreSQLError.clientError("Unexpected SASL final message")
        }
        try scramSha256Authenticator.handleServerFinalMessage(finalMessage)

      case .errorResponse(let fields):
        throw PostgreSQLError.databaseError(fields.description)

      default:
        break
      }
    }
  }
}

private final class PostgreSQLMessageCodec: ByteToMessageDecoder, MessageToByteEncoder, Sendable {
  typealias OutboundIn = PostgreSQLFrontendMessage
  typealias InboundOut = PostgreSQLBackendMessage

  func encode(data: PostgreSQLFrontendMessage, out: inout NIOCore.ByteBuffer) throws {
    var buffer = Self.encodeMessage(message: data)
    out.writeBuffer(&buffer)
  }

  func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
    guard buffer.readableBytes >= 5 else {
      return .needMoreData
    }
    let messageType = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self)!
    let messageLength = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self)!

    let dataLength = Int(messageLength) - 4  // 4 bytes for the length itself
    guard buffer.readableBytes >= dataLength + 5 else {
      return .needMoreData
    }

    buffer.moveReaderIndex(forwardBy: 5)  // Move past the message type and length
    var dataBuffer = buffer.readSlice(length: dataLength)!
    let message = Self.decodeMessage(messageType, dataLength, &dataBuffer)
    context.fireChannelRead(wrapInboundOut(message))
    return .continue
  }

  private static func decodeMessage(_ type: UInt8, _ length: Int, _ buffer: inout ByteBuffer)
    -> PostgreSQLBackendMessage
  {
    let message: PostgreSQLBackendMessage
    // print("Receiving message type: \(type)")

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

  private static func encodeMessage(message: PostgreSQLFrontendMessage) -> ByteBuffer {
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
}
