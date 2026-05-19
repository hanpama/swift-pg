import NIO
import Testing

@testable import SwiftPG

final class RowDecodingContractTests {
    @Test func rowDecodeThrowsWhenRequestingMoreValuesThanRowContains() throws {
        let row = PostgreSQLRow(
            defaultDecoderMap: DEFAULT_DECODER_MAP,
            fieldOids: [23],
            columns: encodedInt32Column(1)
        )

        let error = #expect(throws: ClientError.self) {
            let _: (Int32, Int32) = try row.decode()
        }

        guard case .codecError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            return
        }
    }

    @Test func rowDecodeThrowsForUnknownAnyOID() throws {
        let row = PostgreSQLRow(
            defaultDecoderMap: DEFAULT_DECODER_MAP,
            fieldOids: [999_999, 23],
            columns: encodedInt32Column(1)
        )

        let error = #expect(throws: ClientError.self) {
            let _: (Any, Int32) = try row.decode()
        }

        guard case .codecError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            return
        }
    }

    @Test func rowDecodeThrowsForMalformedColumnBuffer() throws {
        var columns = ByteBuffer()
        columns.writeInteger(Int32(4))
        let row = PostgreSQLRow(
            defaultDecoderMap: DEFAULT_DECODER_MAP,
            fieldOids: [23],
            columns: columns
        )

        let error = #expect(throws: ClientError.self) {
            let _: Int32 = try row.decode()
        }

        guard case .codecError = error else {
            Issue.record("Unexpected error: \(String(describing: error))")
            return
        }
    }
}

private func encodedInt32Column(_ value: Int32) -> ByteBuffer {
    var buffer = ByteBuffer()
    buffer.writeInteger(Int32(4))
    buffer.writeInteger(value)
    return buffer
}
