import Testing

@testable import SwiftPG

final class ExtendedQueryProtocolTests {
    @Test func executeCompletesForStatementWithoutRows() async throws {
        try await withScriptedPostgresServer(
            steps: startupSuccessScript()
                + describeStatementScript(parameterOids: [], result: .noData)
                + [
                    .readMessage(UInt8(ascii: "B")),
                    .readMessage(UInt8(ascii: "E")),
                    .readMessage(
                        UInt8(ascii: "S"),
                        write: [
                            PostgresWire.bindComplete(),
                            PostgresWire.commandComplete("CREATE TABLE"),
                            PostgresWire.readyForQuery(),
                        ]
                    ),
                ]
        ) { server in
            let conn = Connection()
            try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))

            try await withTimeout {
                try await conn.execute("CREATE TABLE example(id int4)")
            }

            try await conn.close()
            #expect(server.failures.isEmpty, "\(server.failures)")
        }
    }

    @Test func executeRejectsTooFewParametersBeforeBind() async throws {
        let server = ScriptedPostgresServer(
            steps: startupSuccessScript()
                + describeStatementScript(parameterOids: [23], result: .noData)
        )
        try await server.start()
        let conn = Connection()
        try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))

        let error = await #expect(throws: ClientError.self) {
            try await withTimeout {
                try await conn.execute("SELECT $1::int4")
            }
        }

        guard case .codecError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            try await conn.close()
            await server.stop()
            return
        }
        #expect(server.receivedMessages.contains(.message(UInt8(ascii: "B"))) == false)
        try await conn.close()
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test func executeRejectsTooManyParametersBeforeBind() async throws {
        let server = ScriptedPostgresServer(
            steps: startupSuccessScript()
                + describeStatementScript(parameterOids: [23], result: .noData)
        )
        try await server.start()
        let conn = Connection()
        try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))

        let error = await #expect(throws: ClientError.self) {
            try await withTimeout {
                try await conn.execute("SELECT $1::int4", [Int32(1), Int32(2)])
            }
        }

        guard case .codecError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            try await conn.close()
            await server.stop()
            return
        }
        #expect(server.receivedMessages.contains(.message(UInt8(ascii: "B"))) == false)
        try await conn.close()
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test func queryRejectsParameterCountMismatchBeforeBind() async throws {
        let server = ScriptedPostgresServer(
            steps: startupSuccessScript()
                + describeStatementScript(parameterOids: [23], result: .noData)
        )
        try await server.start()
        let conn = Connection()
        try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))

        let error = await #expect(throws: ClientError.self) {
            try await withTimeout {
                _ = try await conn.query("SELECT $1::int4")
            }
        }

        guard case .codecError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            try await conn.close()
            await server.stop()
            return
        }
        #expect(server.receivedMessages.contains(.message(UInt8(ascii: "B"))) == false)
        try await conn.close()
        #expect(server.failures.isEmpty, "\(server.failures)")
        await server.stop()
    }

    @Test func batchExecuteDrainsOneCommandCompletePerBatch() async throws {
        try await withScriptedPostgresServer(
            steps: startupSuccessScript()
                + describeStatementScript(parameterOids: [23], result: .noData)
                + [
                    .readMessage(UInt8(ascii: "B")),
                    .readMessage(UInt8(ascii: "E")),
                    .readMessage(UInt8(ascii: "B")),
                    .readMessage(UInt8(ascii: "E")),
                    .readMessage(
                        UInt8(ascii: "S"),
                        write: [
                            PostgresWire.bindComplete(),
                            PostgresWire.commandComplete("INSERT 0 1"),
                            PostgresWire.bindComplete(),
                            PostgresWire.commandComplete("INSERT 0 1"),
                            PostgresWire.readyForQuery(),
                        ]
                    ),
                ]
        ) { server in
            let conn = Connection()
            try await conn.connect(configs: testConnectionConfigs(socketAddress: server.socketAddress))

            try await withTimeout {
                try await conn.batchExecute(
                    "INSERT INTO example(id) VALUES ($1)",
                    [[Int32(1)], [Int32(2)]]
                )
            }

            try await conn.close()
            #expect(server.failures.isEmpty, "\(server.failures)")
        }
    }
}

enum DescribeResult {
    case noData
    case malformedRowDescription
}

func startupSuccessScript() -> [ScriptedPostgresStep] {
    [.readStartup(write: startupSuccessFrames())]
}

func describeStatementScript(parameterOids: [Int32], result: DescribeResult)
    -> [ScriptedPostgresStep]
{
    let resultFrame =
        switch result {
        case .noData:
            PostgresWire.noData()
        case .malformedRowDescription:
            PostgresWire.malformedRowDescription()
        }

    return [
        .readMessage(UInt8(ascii: "P")),
        .readMessage(UInt8(ascii: "D")),
        .readMessage(
            UInt8(ascii: "H"),
            write: [
                PostgresWire.parseComplete(),
                PostgresWire.parameterDescription(parameterOids),
                resultFrame,
            ]
        ),
    ]
}
