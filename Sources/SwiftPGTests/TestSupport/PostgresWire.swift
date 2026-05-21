import NIO

enum PostgresWire {
    static func sslNotSupported() -> ByteBuffer {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "N"))
        return buffer
    }

    static func authenticationOk() -> ByteBuffer {
        typedMessage("R") { body in
            body.writeInteger(Int32(0))
        }
    }

    static func authenticationCleartextPassword() -> ByteBuffer {
        authentication(type: 3)
    }

    static func authenticationMD5Password() -> ByteBuffer {
        typedMessage("R") { body in
            body.writeInteger(Int32(5))
            body.writeBytes([1, 2, 3, 4])
        }
    }

    static func authenticationKerberosV5() -> ByteBuffer {
        authentication(type: 2)
    }

    static func authenticationGSS() -> ByteBuffer {
        authentication(type: 7)
    }

    static func authenticationGSSContinue() -> ByteBuffer {
        authentication(type: 8)
    }

    static func authenticationSSPI() -> ByteBuffer {
        authentication(type: 9)
    }

    static func backendKeyData(processID: Int32 = 1, secretKey: Int32 = 2) -> ByteBuffer {
        typedMessage("K") { body in
            body.writeInteger(processID)
            body.writeInteger(secretKey)
        }
    }

    static func readyForQuery() -> ByteBuffer {
        typedMessage("Z") { body in
            body.writeInteger(UInt8(ascii: "I"))
        }
    }

    static func errorResponse(code: String, message: String, severity: String = "FATAL")
        -> ByteBuffer
    {
        typedMessage("E") { body in
            body.writeInteger(UInt8(ascii: "S"))
            body.writeNullTerminatedString(severity)
            body.writeInteger(UInt8(ascii: "C"))
            body.writeNullTerminatedString(code)
            body.writeInteger(UInt8(ascii: "M"))
            body.writeNullTerminatedString(message)
            body.writeInteger(UInt8(0))
        }
    }

    static func parseComplete() -> ByteBuffer {
        typedMessage("1")
    }

    static func parameterDescription(_ oids: [Int32]) -> ByteBuffer {
        typedMessage("t") { body in
            body.writeInteger(Int16(oids.count))
            for oid in oids {
                body.writeInteger(oid)
            }
        }
    }

    static func noData() -> ByteBuffer {
        typedMessage("n")
    }

    static func bindComplete() -> ByteBuffer {
        typedMessage("2")
    }

    static func commandComplete(_ tag: String = "SELECT 0") -> ByteBuffer {
        typedMessage("C") { body in
            body.writeNullTerminatedString(tag)
        }
    }

    static func malformedRowDescription() -> ByteBuffer {
        typedMessage("T") { body in
            body.writeInteger(Int16(1))
        }
    }

    static func invalidMessageLength() -> ByteBuffer {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "R"))
        buffer.writeInteger(Int32(3))
        return buffer
    }

    static func truncatedAuthenticationMessage() -> ByteBuffer {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "R"))
        buffer.writeInteger(Int32(6))
        buffer.writeInteger(Int16(0))
        return buffer
    }

    private static func authentication(type: Int32) -> ByteBuffer {
        typedMessage("R") { body in
            body.writeInteger(type)
        }
    }

    private static func typedMessage(
        _ ascii: Unicode.Scalar,
        _ writeBody: (inout ByteBuffer) -> Void = { _ in }
    ) -> ByteBuffer {
        var body = ByteBuffer()
        writeBody(&body)

        var message = ByteBuffer()
        message.writeInteger(UInt8(ascii: ascii))
        message.writeInteger(Int32(body.readableBytes + 4))
        message.writeBuffer(&body)
        return message
    }
}
