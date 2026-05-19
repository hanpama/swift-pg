import AsyncAlgorithms
import Foundation
import Logging
import NIO
import NIOConcurrencyHelpers
import NIOSSL

public final class Connection: Sendable {
    private let eventLoopGroup: EventLoopGroup
    let defaultDecoderMap: [Int32: PostgreSQLDecodable.Type] = DEFAULT_DECODER_MAP
    private let protocolCiientBox: NIOLockedValueBox<ProtocolClient?> = .init(nil)
    private let currentTaskBox: NIOLockedValueBox<Task<Void, Swift.Error>?> = .init(nil)
    private let connectionErrorBox: NIOLockedValueBox<Swift.Error?> = .init(nil)
    // private let query

    public init(eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton) {
        self.eventLoopGroup = eventLoopGroup
    }

    public func connect(configs: ConnectionConfigs) async throws {
        let protocolClient = try await ProtocolClient(
            eventLoop: eventLoopGroup.next(),
            configs: configs
        )
        protocolCiientBox.withLockedValue { $0 = protocolClient }

        try await send(.startupMessage(configs.username, configs.database))
        do {
            try await authenticate(username: configs.username, password: configs.password)
            try await waitKeyData()
            try await readyForQuery()
        } catch {
            try await protocolClient.close()
            protocolCiientBox.withLockedValue { $0 = nil }
            throw error
        }
    }

    public func close() async throws {
        cancelCurrentTask()
        let protocolClient = protocolCiientBox.withLockedValue { $0 }
        try await protocolClient?.send(.terminate)
        try await protocolClient?.close()
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

    public func query(_ sql: String, _ params: [PostgreSQLEncodable] = []) async throws -> PostgreSQLRows {
        let promise = eventLoopGroup.next().makePromise(of: PostgreSQLRows.self)
        try withCurrentTask {
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
                    throw error
                }
            } catch {
                promise.fail(error)
                throw error
            }
        }
        return try await withTaskCancellationHandler {
            try await promise.futureResult.get()
        } onCancel: {
            cancelCurrentTask()
        }
    }

    public func execute(_ sql: String, _ params: [PostgreSQLEncodable] = []) async throws {
        let promise = eventLoopGroup.next().makePromise(of: Void.self)
        try withCurrentTask {
            do {
                let stmt = try await self.parseStmt(name: "", sql: sql)
                try await self.execStmt(portalName: "", stmt: stmt, params: params, rowLimit: 0)
                try await self.sync()
                try await self.drainUntilCommandComplete()
                promise.succeed(())
            } catch {
                promise.fail(error)
                throw error
            }
        }
        try await withTaskCancellationHandler {
            try await promise.futureResult.get()
        } onCancel: {
            cancelCurrentTask()
        }
    }

    public func batchQuery(_ sql: String, _ batches: [[PostgreSQLEncodable]]) async throws -> PostgreSQLRows {
        let promise = eventLoopGroup.next().makePromise(of: PostgreSQLRows.self)
        try withCurrentTask {
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
                    throw error
                }
            } catch {
                promise.fail(error)
                throw error
            }
        }
        return try await withTaskCancellationHandler {
            try await promise.futureResult.get()
        } onCancel: {
            cancelCurrentTask()
        }
    }

    public func batchExecute(_ sql: String, _ batches: [[PostgreSQLEncodable]]) async throws {
        let promise = eventLoopGroup.next().makePromise(of: Void.self)
        try withCurrentTask {
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
                throw error
            }
        }
        try await withTaskCancellationHandler {
            try await promise.futureResult.get()
        } onCancel: {
            cancelCurrentTask()
        }
    }

    private func receiveRow(stmt: PostgreSQLStatement, rowLimit: Int32) async throws -> PostgreSQLRow? {
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
            case .noData:
                fields = []
                break loop
            case .parameterDescription(let oids):
                parameterOids = oids
            case .errorResponse(let errorMessage):
                throw DatabaseError.from(errorMessage: errorMessage)
            default:
                break
            }
        }
        guard let fields else {
            throw DriverError("Missing row description")
        }
        guard let parameterOids else {
            throw DriverError("Missing parameter description")
        }
        return PostgreSQLStatement(name: name, fields: fields, parameterOids: parameterOids)
    }

    private func execStmt(
        portalName: String, stmt: PostgreSQLStatement, params: [PostgreSQLEncodable],
        rowLimit: Int32
    ) async throws {
        guard params.count == stmt.parameterOids.count else {
            throw ClientError.codecError(
                "Parameter count mismatch: expected \(stmt.parameterOids.count), got \(params.count)"
            )
        }
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

    private func waitKeyData() async throws {
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

    private func readyForQuery() async throws {
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
            let message = try await receive()
            switch message {
            case .authenticationOk:
                break loop

            case .authenticationSasl(let mechanisms):
                if mechanisms.contains("SCRAM-SHA-256") || mechanisms.contains("SCRAM-SHA-256-PLUS") {
                    let authenticator = try ScramSha256Authenticator(
                        username: username, password: password)
                    scramSha256Authenticator = authenticator

                    try await send(
                        .saslInitialResponse(
                            mechanism: "SCRAM-SHA-256",
                            initialResponse: authenticator.formatClientFirstMessage()
                        )
                    )
                } else {
                    throw DriverError("No supported SASL mechanism found. Supported: \(mechanisms)")
                }

            case .authenticationSaslContinue(let challenge):
                guard let scramSha256Authenticator else {
                    throw DriverError("Received SASL challenge before SASL authentication started")
                }
                try scramSha256Authenticator.handleServerFirstMessage(challenge)

                try await send(
                    .saslResponse(scramSha256Authenticator.formatClientFinalMessage())
                )

            case .authenticationSaslFinal(let finalMessage):
                guard let scramSha256Authenticator else {
                    throw DriverError("Received SASL final message before SASL authentication started")
                }
                try scramSha256Authenticator.handleServerFinalMessage(finalMessage)

            case .authenticationCleartextPassword:
                throw DriverError("Unsupported authentication method: cleartext password")

            case .authenticationMD5Password:
                throw DriverError("Unsupported authentication method: MD5 password")

            case .authenticationKerberosV5:
                throw DriverError("Unsupported authentication method: Kerberos V5")

            case .authenticationGSS:
                throw DriverError("Unsupported authentication method: GSS")

            case .authenticationGSSContinue:
                throw DriverError("Unsupported authentication method: GSS continuation")

            case .authenticationSSPI:
                throw DriverError("Unsupported authentication method: SSPI")

            case .errorResponse(let errorMessage):
                throw DatabaseError.from(errorMessage: errorMessage)

            default:
                throw DriverError("Unexpected authentication message: \(message)")
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

    private func getProtocolClient() throws -> ProtocolClient {
        let protocolClient = protocolCiientBox.withLockedValue { $0 }
        guard let protocolClient = protocolClient else {
            throw ClientError.connectionError("Connection not established")
        }
        return protocolClient
    }

    private func withCurrentTask(_ op: @Sendable @escaping @isolated(any) () async throws -> Void) throws {
        let task = Task(operation: op)
        try currentTaskBox.withLockedValue({ currentTask in
            guard case .none = currentTask else {
                throw ClientError.concurrencyError("Operation already in progress")
            }
            currentTask = task
        })
        Task {
            do { try await task.value }
            currentTaskBox.withLockedValue { $0 = nil }

            do {
                try await readyForQuery()
            } catch {
                // print("withCurrentTask readyForQuery error: \(error)")
                try await close()
            }
        }
    }

    private func cancelCurrentTask() {
        currentTaskBox.withLockedValue {
            $0?.cancel()
            $0 = nil
        }
    }

    func waitCurrentTask() async throws {
        try await currentTaskBox.withLockedValue { $0 }?.value
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
    public typealias AsyncIterator = AsyncThrowingChannel<Element, Swift.Error>.AsyncIterator
    let channel = AsyncThrowingChannel<PostgreSQLRow, Swift.Error>()

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
        return (repeat try get((each V).self, nextOid(&fieldsIterator), &buffer))
    }

    private func nextOid(_ fieldsIterator: inout IndexingIterator<[Int32]>) throws -> Int32 {
        guard let oid = fieldsIterator.next() else {
            throw ClientError.codecError("Requested more values than row contains")
        }
        return oid
    }

    private func get<T>(_ type: T.Type, _ oid: Int32, _ buf: inout ByteBuffer) throws -> T {
        if let type = type as? PostgreSQLDecodable.Type {
            let value = try type.init(pgTypeOid: oid, buffer: &buf)
            guard let typedValue = value as? T else {
                throw ClientError.codecError("Decoded value cannot be cast to \(T.self)")
            }
            return typedValue
        }
        if type == PostgreSQLDecodable.self || type == Any.self {
            if let type = defaultDecoderMap[oid] {
                let value = try type.init(pgTypeOid: oid, buffer: &buf)
                guard let typedValue = value as? T else {
                    throw ClientError.codecError("Decoded value cannot be cast to \(T.self)")
                }
                return typedValue
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
