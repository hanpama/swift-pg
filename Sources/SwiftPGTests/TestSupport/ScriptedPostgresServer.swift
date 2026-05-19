import NIO
import NIOConcurrencyHelpers

@testable import SwiftPG

enum ScriptedPostgresStep {
    case readStartup(write: [ByteBuffer] = [])
    case readMessage(UInt8, write: [ByteBuffer] = [])
    case write([ByteBuffer])
    case close
}

enum ScriptedFrontendMessage: Equatable {
    case startup
    case message(UInt8)

    var type: UInt8? {
        guard case .message(let type) = self else {
            return nil
        }
        return type
    }
}

final class ScriptedPostgresServer: @unchecked Sendable {
    private let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    private let steps: [ScriptedPostgresStep]
    private let receivedMessagesBox: NIOLockedValueBox<[ScriptedFrontendMessage]> = .init([])
    private let failuresBox: NIOLockedValueBox<[String]> = .init([])
    private var channel: (any Channel)?

    init(steps: [ScriptedPostgresStep]) {
        self.steps = steps
    }

    var socketAddress: ConnectionConfigs.SocketAddress {
        guard let port = channel?.localAddress?.port else {
            preconditionFailure("ScriptedPostgresServer has not started")
        }
        return .hostPort(host: "127.0.0.1", port: port)
    }

    var receivedMessages: [ScriptedFrontendMessage] {
        receivedMessagesBox.withLockedValue { $0 }
    }

    var failures: [String] {
        failuresBox.withLockedValue { $0 }
    }

    func start() async throws {
        let receivedMessagesBox = receivedMessagesBox
        let failuresBox = failuresBox
        let steps = steps
        channel = try await ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.addHandler(
                    Handler(
                        steps: steps,
                        receivedMessagesBox: receivedMessagesBox,
                        failuresBox: failuresBox
                    )
                )
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
    }

    func stop() async {
        if let channel {
            try? await channel.close()
        }
        try? await group.shutdownGracefully()
    }

    private final class Handler: ChannelInboundHandler, @unchecked Sendable {
        typealias InboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer

        private var steps: [ScriptedPostgresStep]
        private let receivedMessagesBox: NIOLockedValueBox<[ScriptedFrontendMessage]>
        private let failuresBox: NIOLockedValueBox<[String]>
        private var inboundBuffer = ByteBuffer()
        private var expectingStartup = true

        init(
            steps: [ScriptedPostgresStep],
            receivedMessagesBox: NIOLockedValueBox<[ScriptedFrontendMessage]>,
            failuresBox: NIOLockedValueBox<[String]>
        ) {
            self.steps = steps
            self.receivedMessagesBox = receivedMessagesBox
            self.failuresBox = failuresBox
        }

        func channelActive(context: ChannelHandlerContext) {
            processReadySteps(context: context)
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            var data = unwrapInboundIn(data)
            inboundBuffer.writeBuffer(&data)
            processInbound(context: context)
        }

        func channelInactive(context: ChannelHandlerContext) {
            if !steps.isEmpty {
                recordFailure("Connection closed before script completed: \(steps)")
            }
            context.fireChannelInactive()
        }

        private func processInbound(context: ChannelHandlerContext) {
            while true {
                if expectingStartup {
                    guard let startupLength = readableStartupLength() else {
                        return
                    }
                    inboundBuffer.moveReaderIndex(forwardBy: startupLength)
                    expectingStartup = false
                    handle(.startup, context: context)
                    continue
                }

                guard let message = readableFrontendMessage() else {
                    return
                }
                handle(message, context: context)
            }
        }

        private func readableStartupLength() -> Int? {
            guard inboundBuffer.readableBytes >= 4 else {
                return nil
            }
            guard
                let length = inboundBuffer.getInteger(
                    at: inboundBuffer.readerIndex, as: Int32.self),
                length >= 4
            else {
                recordFailure("Invalid startup packet length")
                return nil
            }
            let totalLength = Int(length)
            guard inboundBuffer.readableBytes >= totalLength else {
                return nil
            }
            return totalLength
        }

        private func readableFrontendMessage() -> ScriptedFrontendMessage? {
            guard inboundBuffer.readableBytes >= 5 else {
                return nil
            }
            guard
                let type = inboundBuffer.getInteger(
                    at: inboundBuffer.readerIndex, as: UInt8.self),
                let length = inboundBuffer.getInteger(
                    at: inboundBuffer.readerIndex + 1, as: Int32.self),
                length >= 4
            else {
                recordFailure("Invalid frontend message header")
                return nil
            }

            let totalLength = Int(length) + 1
            guard inboundBuffer.readableBytes >= totalLength else {
                return nil
            }
            inboundBuffer.moveReaderIndex(forwardBy: totalLength)
            return .message(type)
        }

        private func handle(_ message: ScriptedFrontendMessage, context: ChannelHandlerContext) {
            receivedMessagesBox.withLockedValue { $0.append(message) }

            guard !steps.isEmpty else {
                if message.type == UInt8(ascii: "X") {
                    context.close(promise: nil)
                    return
                }
                recordFailure("Unexpected frontend message: \(message)")
                context.close(promise: nil)
                return
            }

            switch (message, steps.removeFirst()) {
            case (.startup, .readStartup(let frames)):
                write(frames, context: context)
                processReadySteps(context: context)

            case (.message(let got), .readMessage(let expected, let frames)) where got == expected:
                write(frames, context: context)
                processReadySteps(context: context)

            case (_, let step):
                recordFailure("Unexpected frontend message: \(message), expected: \(step)")
                context.close(promise: nil)
            }
        }

        private func processReadySteps(context: ChannelHandlerContext) {
            while let step = steps.first {
                switch step {
                case .write(let frames):
                    steps.removeFirst()
                    write(frames, context: context)
                case .close:
                    steps.removeFirst()
                    context.close(promise: nil)
                case .readStartup, .readMessage:
                    return
                }
            }
        }

        private func write(_ frames: [ByteBuffer], context: ChannelHandlerContext) {
            guard !frames.isEmpty else {
                return
            }

            var buffer = context.channel.allocator.buffer(capacity: 0)
            for var frame in frames {
                buffer.writeBuffer(&frame)
            }
            context.writeAndFlush(wrapOutboundOut(buffer), promise: nil)
        }

        private func recordFailure(_ failure: String) {
            failuresBox.withLockedValue { $0.append(failure) }
        }
    }
}

func withScriptedPostgresServer<T>(
    steps: [ScriptedPostgresStep],
    _ body: (ScriptedPostgresServer) async throws -> T
) async throws -> T {
    let server = ScriptedPostgresServer(steps: steps)
    try await server.start()
    do {
        let result = try await body(server)
        await server.stop()
        return result
    } catch {
        await server.stop()
        throw error
    }
}

func startupSuccessFrames() -> [ByteBuffer] {
    [
        PostgresWire.authenticationOk(),
        PostgresWire.backendKeyData(),
        PostgresWire.readyForQuery(),
    ]
}

func testConnectionConfigs(
    socketAddress: ConnectionConfigs.SocketAddress,
    username: String = "user",
    password: String = "password",
    database: String = "postgres"
) -> ConnectionConfigs {
    .init(
        socketAddress: socketAddress,
        username: username,
        password: password,
        database: database,
        sslmode: .disable
    )
}
