import NIO
import NIOSSL
import NIOTLS

final class ProtocolClient: @unchecked Sendable {
    private let channel: Channel
    private let outbound: Outbound
    private var inbound: Inbound.AsyncIterator
    private typealias Outbound = NIOAsyncChannelOutboundWriter<PostgreSQLFrontendMessage>
    private typealias Inbound = NIOAsyncChannelInboundStream<PostgreSQLBackendMessage>
    private let messageCodec = MessageCodec()

    init(eventLoop loop: EventLoop, configs: ConnectionConfigs) async throws {

        let socketAddress: SocketAddress =
            switch configs.socketAddress {
            case .hostPort(let host, let port):
                try .makeAddressResolvingHost(host, port: port)
            case .unixDomainSocket(let directory, let port):
                try .init(unixDomainSocketPath: "\(directory)/.s.PGSQL.\(port)")
            }

        let channel: any Channel
        let channelReady = loop.makePromise(of: Void.self)
        var handlers: [ChannelHandler] = [
            ByteToMessageHandler(messageCodec),
            MessageToByteHandler(messageCodec),
        ]
        if let sslHandler = try Self.getTLSHandler(configs: configs) {
            handlers.insert(sslHandler, at: 0)
        }
        handlers.append(ReadyForStartupHandler(promise: channelReady, tls: configs.sslmode != .disable))
        do {
            channel = try await ClientBootstrap(group: loop)
                .channelInitializer { channel in
                    channel.pipeline.addHandlers(handlers)
                }
                .connect(to: socketAddress).get()
        } catch {
            channelReady.fail(error)
            throw error
        }

        try await channelReady.futureResult.get()

        let asyncChannel = try await channel.eventLoop.submit {
            try NIOAsyncChannel<PostgreSQLBackendMessage, PostgreSQLFrontendMessage>(
                wrappingChannelSynchronously: channel
            )
        }.get()

        let ioPromise = loop.makePromise(of: (Inbound, Outbound).self)

        Task {
            do {
                try await asyncChannel.executeThenClose { inbound, outbound in
                    ioPromise.succeed((inbound, outbound))
                    try await channel.closeFuture.get()
                }
            } catch {
                ioPromise.fail(error)
            }
        }
        let (inbound, outbound) = try await ioPromise.futureResult.get()
        self.inbound = inbound.makeAsyncIterator()
        self.outbound = outbound
        self.channel = channel
    }

    func send(_ message: PostgreSQLFrontendMessage) async throws {
        try await outbound.write(message)
    }

    func close() async throws {
        if channel.isActive {
            try await channel.close()
        }
    }

    func isConnected() -> Bool {
        return channel.isActive
    }

    func isClosed() -> Bool {
        return !channel.isActive
    }

    func receive() async throws -> PostgreSQLBackendMessage? {
        return try await inbound.next()  // TODO: make it thread-safe
    }

    private static func getTLSHandler(configs: ConnectionConfigs) throws
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
            tlsConfig.certificateChain = [.certificate(sslcert)]
        }
        if let sslkey = configs.sslkey {
            tlsConfig.privateKey = .privateKey(sslkey)
        }
        if let sslrootcert = configs.sslrootcert {
            tlsConfig.additionalTrustRoots = [sslrootcert]
        }
        guard case let .hostPort(host, _) = configs.socketAddress else {
            throw ClientError.configurationError("Host is required for TLS connections")
        }
        let sslContext = try NIOSSLContext(configuration: tlsConfig)
        let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: host)
        return sslHandler
    }

    private final class MessageCodec: ByteToMessageDecoder, MessageToByteEncoder, Sendable {
        typealias OutboundIn = PostgreSQLFrontendMessage
        typealias InboundOut = PostgreSQLBackendMessage

        func encode(data: PostgreSQLFrontendMessage, out: inout NIOCore.ByteBuffer) throws {
            var buffer = Self.encodeMessage(message: data)
            out.writeBuffer(&buffer)
        }

        func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws
            -> DecodingState
        {
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
            let message = try Self.decodeMessage(messageType, &dataBuffer)
            context.fireChannelRead(wrapInboundOut(message))
            return .continue
        }

        private static func decodeMessage(_ type: UInt8, _ buffer: inout ByteBuffer) throws
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
                case 2:
                    message = .authenticationKerberosV5
                case 3:
                    message = .authenticationCleartextPassword
                case 5:
                    let salt = buffer.readBytes(length: 4)!
                    message = .authenticationMD5Password(salt)
                case 7:
                    message = .authenticationGSS
                case 8:
                    message = .authenticationGSSContinue
                case 9:
                    message = .authenticationSSPI
                case 10:
                    message = .authenticationSasl(
                        buffer.readNullTerminatedString()!.split(separator: ",").map { String($0) })
                case 11:
                    message = .authenticationSaslContinue(
                        buffer.readString(length: buffer.readableBytes)!)
                case 12:
                    message = .authenticationSaslFinal(
                        buffer.readString(length: buffer.readableBytes)!)
                default:
                    throw DriverError("Unknown authentication message type: \(authMessageType)")
                }
            case 75:  // 'K'
                let processID = buffer.readInteger(as: Int32.self)!
                let secretKey = buffer.readInteger(as: Int32.self)!
                message = .backendKeyData(processID, secretKey)
            case 50:  // '2'
                message = .bindComplete
            case 51:  // '3'
                message = .closeComplete
            case 67:  // 'C'
                message = .commandComplete(buffer.readNullTerminatedString()!)
            case 100:  // 'd'
                message = .copyData(buffer.readBytes(length: buffer.readableBytes)!)
            case 99:  // 'c'
                message = .copyDone
            case 71:  // 'G'
                message = .copyInResponse(
                    buffer.readInteger(as: Int8.self)!,
                    (0..<buffer.readInteger(as: Int16.self)!).map { _ in
                        buffer.readInteger(as: Int16.self)!
                    }
                )
            case 72:  // 'H'
                message = .copyOutResponse(
                    buffer.readInteger(as: Int8.self)!,
                    (0..<buffer.readInteger(as: Int16.self)!).map { _ in
                        buffer.readInteger(as: Int16.self)!
                    }
                )
            case 87:  // 'W'
                message = .copyBothResponse(
                    buffer.readInteger(as: Int8.self)!,
                    (0..<buffer.readInteger(as: Int16.self)!).map { _ in
                        buffer.readInteger(as: Int16.self)!
                    }
                )
            case 68:  // 'D'
                message = .dataRow(columns: buffer.readInteger(as: Int16.self)!, columnData: buffer)
            case 73:  // 'I'
                message = .emptyQueryResponse
            case 69:  // 'E'
                message = .errorResponse(decodeServerErrorNoticeMessage(buffer: &buffer))
            case 86:  // 'V'
                message = .functionCallResponse(buffer.readBytes(length: buffer.readableBytes)!)
            case 118:  // 'v'
                message = .negotiateProtocolVersion(
                    buffer.readInteger(as: Int32.self)!,
                    (0..<buffer.readInteger(as: Int16.self)!).map { _ in
                        buffer.readNullTerminatedString()!
                    }
                )
            case 110:  // 'n'
                message = .noData
            case 78:  // 'N'
                message = .noticeResponse(decodeServerErrorNoticeMessage(buffer: &buffer))
            case 65:  // 'A'
                let processID = buffer.readInteger(as: Int32.self)!
                let channel = buffer.readNullTerminatedString()!
                let payload = buffer.readNullTerminatedString()!
                message = .notificationResponse(processID, channel, payload)
            case 116:  // 't'
                message = .parameterDescription(
                    (0..<buffer.readInteger(as: Int16.self)!).map { _ in
                        buffer.readInteger(as: Int32.self)!
                    }
                )
            case 83:  // 'S'
                let parameter = buffer.readNullTerminatedString()!
                let value = buffer.readNullTerminatedString()!
                message = .parameterStatus(parameter, value)
            case 49:  // '1'
                message = .parseComplete
            case 115:  // 's'
                message = .portalSuspended
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
                case 83: field = .severity(fieldValue)  // 'S'
                case 86: field = .severity(fieldValue)  // 'V'
                case 67: field = .code(fieldValue)  // 'C'
                case 77: field = .message(fieldValue)  // 'M'
                case 68: field = .detail(fieldValue)  // 'D'
                case 72: field = .hint(fieldValue)  // 'H'
                case 80: field = .position(fieldValue)  // 'P'
                case 112: field = .internalPosition(fieldValue)  // 'p'
                case 113: field = .internalQuery(fieldValue)  // 'q'
                case 87: field = .where(fieldValue)  // 'W'
                case 115: field = .schemaName(fieldValue)  // 's'
                case 116: field = .tableName(fieldValue)  // 't'
                case 99: field = .columnName(fieldValue)  // 'c'
                case 100: field = .dataTypeName(fieldValue)  // 'd'
                case 110: field = .constraintName(fieldValue)  // 'n'
                case 70: field = .file(fieldValue)  // 'F'
                case 76: field = .line(fieldValue)  // 'L'
                case 82: field = .routine(fieldValue)  // 'R'
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

    private final class ReadyForStartupHandler: ChannelInboundHandler, Sendable {
        typealias InboundIn = PostgreSQLBackendMessage

        private let promise: EventLoopPromise<Void>
        private let tls: Bool
        init(promise: EventLoopPromise<Void>, tls: Bool) {
            self.promise = promise
            self.tls = tls
        }

        func channelActive(context: ChannelHandlerContext) {
            context.fireChannelActive()
            if !tls {
                promise.succeed(())
            }
        }

        func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
            context.fireUserInboundEventTriggered(event)
            if tls {
                if case TLSUserEvent.handshakeCompleted = event {
                    promise.succeed(())
                }
            }
        }

        func errorCaught(context: ChannelHandlerContext, error: Error) {
            context.fireErrorCaught(error)
            promise.fail(error)
        }
    }
}
