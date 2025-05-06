import Foundation
import NIO
import Testing

@testable import SwiftPG

final class CodecTests {

    // MARK: - Bool
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingBool(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::bool = true,
              $2::bool = false,
              $3::bool is null,
              $4::bool[] = ARRAY[true, false]::bool[],
              $5::bool[] = ARRAY[true, false, null]::bool[]
            """,
            [
                true,
                false,
                nil as Bool?,
                [true, false],
                [true, false, nil] as [Bool?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4, got5): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
            #expect(got5)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingBool(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              true,
              false,
              null::bool,
              array[true, false]::bool[],
              array[true, false, null]::bool[]
            """
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool?, [Bool], [Bool?])
            let (got1, got2, got3, got4, got5): Row = try row.decode()

            #expect(got1 == true)
            #expect(got2 == false)
            #expect(got3 == nil)
            #expect(got4 == [true, false])
            #expect(got5 == [true, false, nil])
        }
    }

    // MARK: - Int16
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingInt16(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::int2 = 42,
              $2::int2 is null,
              $3::int2[] = ARRAY[42, 43]::int2[],
              $4::int2[] = ARRAY[42, 43, null]::int2[]
            """,
            [
                Int16(42),
                nil as Int16?,
                [Int16(42), Int16(43)],
                [Int16(42), Int16(43), nil] as [Int16?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingInt16(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::int2,
              null::int2,
              array[42, 43]::int2[],
              array[42, 43, null]::int2[]
            """
        )

        for try await row in rows {
            typealias Row = (Int16, Int16?, [Int16], [Int16?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    // MARK: - Int32

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingInt32(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::int4 = 42,
              $2::int4 is null,
              $3::int4[] = ARRAY[42, 43]::int4[],
              $4::int4[] = ARRAY[42, 43, null]::int4[]
            """,
            [
                Int32(42),
                nil as Int32?,
                [Int32(42), Int32(43)],
                [Int32(42), Int32(43), nil] as [Int32?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingInt32(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::int4,
              null::int4,
              array[42, 43]::int4[],
              array[42, 43, null]::int4[]
            """
        )

        for try await row in rows {
            typealias Row = (Int32, Int32?, [Int32], [Int32?])
            let (got1, got2, got3, got4): Row = try row.decode()
            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    // MARK: - Int64
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingInt64(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::int8 = 42,
              $2::int8 is null,
              $3::int8[] = ARRAY[42, 43]::int8[],
              $4::int8[] = ARRAY[42, 43, null]::int8[]
            """,
            [
                Int64(42),
                nil as Int64?,
                [Int64(42), Int64(43)],
                [Int64(42), Int64(43), nil] as [Int64?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingInt64(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::int8,
              null::int8,
              array[42, 43]::int8[],
              array[42, 43, null]::int8[]
            """
        )

        for try await row in rows {
            typealias Row = (Int64, Int64?, [Int64], [Int64?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    // MARK: - Int
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingIntFromInt4(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::int4 = 42,
              $2::int4 is null,
              $3::int4[] = ARRAY[42, 43]::int4[],
              $4::int4[] = ARRAY[42, 43, null]::int4[]
            """,
            [
                Int(42),
                nil as Int?,
                [Int(42), Int(43)],
                [Int(42), Int(43), nil] as [Int?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingIntToInt4(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::int4,
              null::int4,
              array[42, 43]::int4[],
              array[42, 43, null]::int4[]
            """
        )

        for try await row in rows {
            typealias Row = (Int, Int?, [Int], [Int?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingIntFromInt8(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::int8 = 42,
              $2::int8 is null,
              $3::int8[] = ARRAY[42, 43]::int8[],
              $4::int8[] = ARRAY[42, 43, null]::int8[]
            """,
            [
                Int(42),
                nil as Int?,
                [Int(42), Int(43)],
                [Int(42), Int(43), nil] as [Int?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingIntToInt8(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::int8,
              null::int8,
              array[42, 43]::int8[],
              array[42, 43, null]::int8[]
            """
        )

        for try await row in rows {
            typealias Row = (Int, Int?, [Int], [Int?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    // MARK: - String
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingStringFromText(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::text = 'Hello, world!',
              $2::text is null,
              $3::text[] = ARRAY['Hello', 'World']::text[],
              $4::text[] = ARRAY['Hello', 'World', null]::text[]
            """,
            [
                "Hello, world!",
                nil as String?,
                ["Hello", "World"],
                ["Hello", "World", nil] as [String?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingStringFromText(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              'Hello, world!'::text,
              null::text,
              array['Hello', 'World']::text[],
              array['Hello', 'World', null]::text[]
            """
        )

        for try await row in rows {
            typealias Row = (String, String?, [String], [String?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == "Hello, world!")
            #expect(got2 == nil)
            #expect(got3 == ["Hello", "World"])
            #expect(got4 == ["Hello", "World", nil])
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingStringFromVarchar(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::varchar = 'Hello, world!',
              $2::varchar is null,
              $3::varchar[] = ARRAY['Hello', 'World']::varchar[],
              $4::varchar[] = ARRAY['Hello', 'World', null]::varchar[]
            """,
            [
                "Hello, world!",
                nil as String?,
                ["Hello", "World"],
                ["Hello", "World", nil] as [String?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingStringFromVarchar(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              'Hello, world!'::varchar,
              null::varchar,
              array['Hello', 'World']::varchar[],
              array['Hello', 'World', null]::varchar[]
            """
        )

        for try await row in rows {
            typealias Row = (String, String?, [String], [String?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == "Hello, world!")
            #expect(got2 == nil)
            #expect(got3 == ["Hello", "World"])
            #expect(got4 == ["Hello", "World", nil])
        }
    }

    // MARK: - Float
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingFloat(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::float4 = 42,
              $2::float4 is null,
              $3::float4[] = ARRAY[42, 43]::float4[],
              $4::float4[] = ARRAY[42, 43, null]::float4[]
            """,
            [
                Float(42),
                nil as Float?,
                [Float(42), Float(43)],
                [Float(42), Float(43), nil] as [Float?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingFloat(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::float4,
              null::float4,
              array[42, 43]::float4[],
              array[42, 43, null]::float4[]
            """
        )

        for try await row in rows {
            typealias Row = (Float, Float?, [Float], [Float?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    // MARK: - Double
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingDouble(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::float8 = 42,
              $2::float8 is null,
              $3::float8[] = ARRAY[42, 43]::float8[],
              $4::float8[] = ARRAY[42, 43, null]::float8[]
            """,
            [
                Double(42),
                nil as Double?,
                [Double(42), Double(43)],
                [Double(42), Double(43), nil] as [Double?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingDouble(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::float8,
              null::float8,
              array[42, 43]::float8[],
              array[42, 43, null]::float8[]
            """
        )

        for try await row in rows {
            typealias Row = (Double, Double?, [Double], [Double?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == 42)
            #expect(got2 == nil)
            #expect(got3 == [42, 43])
            #expect(got4 == [42, 43, nil])
        }
    }

    // MARK: - Decimal
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingDecimal(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::numeric = 42,
              $2::numeric = 3.141592,
              $3::numeric = 987654321.123456789,
              $4::numeric = -1.414213,
              $5::numeric = -123456789.987654321,
              $6::numeric = 0::numeric,
              $7::numeric = 'NaN'::numeric,
              $8::numeric[] = ARRAY[3.141592, -1.414213]::numeric[],
              $9::numeric[] = ARRAY[3.141592, -1.414213, null]::numeric[]
            """,
            [
                Decimal(string: "42")!,
                Decimal(string: "3.141592")!,
                Decimal(string: "987654321.123456789")!,
                Decimal(string: "-1.414213")!,
                Decimal(string: "-123456789.987654321")!,
                Decimal(string: "0")!,
                Decimal.nan,
                [
                    Decimal(string: "3.141592")!,
                    Decimal(string: "-1.414213")!,
                ],
                [
                    Decimal(string: "3.141592")!,
                    Decimal(string: "-1.414213")!,
                    nil,
                ] as [Decimal?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool, Bool, Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4, got5, got6, got7, got8, got9): Row =
                try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
            #expect(got5)
            #expect(got6)
            #expect(got7)
            #expect(got8)
            #expect(got9)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingDecimal(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              42::numeric,
              3.141592::numeric,
              987654321.123456789::numeric,
              -1.414213::numeric,
              -123456789.987654321::numeric,
              0::numeric,
              'NaN'::numeric,
              array[3.141592, -1.414213]::numeric[],
              array[3.141592, -1.414213, null]::numeric[]
            """
        )

        for try await row in rows {
            typealias Row = (
                Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, [Decimal], [Decimal?]
            )
            let (got1, got2, got3, got4, got5, got6, got7, got8, got9): Row = try row.decode()

            #expect(got1 == Decimal(string: "42")!)
            #expect(got2 == Decimal(string: "3.141592")!)
            #expect(got3 == Decimal(string: "987654321.123456789")!)
            #expect(got4 == Decimal(string: "-1.414213")!)
            #expect(got5 == Decimal(string: "-123456789.987654321")!)
            #expect(got6 == Decimal(string: "0")!)
            #expect(got7.isNaN)
            #expect(
                got8 == [
                    Decimal(string: "3.141592")!,
                    Decimal(string: "-1.414213")!,
                ])
            #expect(
                got9 == [
                    Decimal(string: "3.141592")!,
                    Decimal(string: "-1.414213")!,
                    nil,
                ])
        }
    }

    // MARK: - Date
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingDate(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::timestamp = '1970-01-01 00:00:42',
              $2::timestamp is null,
              $3::timestamp[] = ARRAY['1970-01-01 00:00:42', '1970-01-01 00:00:43']::timestamp[],
              $4::timestamp[] = ARRAY['1970-01-01 00:00:42', '1970-01-01 00:00:43', null]::timestamp[]
            """,
            [
                Date(timeIntervalSince1970: 42),
                nil as Date?,
                [
                    Date(timeIntervalSince1970: 42),
                    Date(timeIntervalSince1970: 43),
                ],
                [
                    Date(timeIntervalSince1970: 42),
                    Date(timeIntervalSince1970: 43),
                    nil,
                ] as [Date?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingDate(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              '1970-01-01 00:00:42'::timestamp,
              null::timestamp,
              array['1970-01-01 00:00:42', '1970-01-01 00:00:43']::timestamp[],
              array['1970-01-01 00:00:42', '1970-01-01 00:00:43', null]::timestamp[]
            """
        )

        for try await row in rows {
            typealias Row = (Date, Date?, [Date], [Date?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == Date(timeIntervalSince1970: 42))
            #expect(got2 == nil)
            #expect(
                got3 == [
                    Date(timeIntervalSince1970: 42),
                    Date(timeIntervalSince1970: 43),
                ])
            #expect(
                got4 == [
                    Date(timeIntervalSince1970: 42),
                    Date(timeIntervalSince1970: 43),
                    nil,
                ])
        }
    }

    // MARK: - UUID
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodingUUID(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              $1::uuid = '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a',
              $2::uuid is null,
              $3::uuid[] = ARRAY['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[],
              $4::uuid[] = ARRAY['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b', null]::uuid[]
            """,
            [
                UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
                nil as UUID?,
                [
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
                ],
                [
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
                    nil,
                ] as [UUID?],
            ]
        )

        for try await row in rows {
            typealias Row = (Bool, Bool, Bool, Bool)
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1)
            #expect(got2)
            #expect(got3)
            #expect(got4)
        }
    }

    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testDecodingUUID(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a'::uuid,
              null::uuid,
              array['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[],
              array['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b', null]::uuid[]
            """
        )

        for try await row in rows {
            typealias Row = (UUID, UUID?, [UUID], [UUID?])
            let (got1, got2, got3, got4): Row = try row.decode()

            #expect(got1 == UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!)
            #expect(got2 == nil)
            #expect(
                got3 == [
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
                ])
            #expect(
                got4 == [
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
                    nil,
                ])
        }
    }

    // MARK: - Any
    @Test(arguments: [postgres17HostPort, postgres16HostPort])
    func testEncodeAny(socketAddress: ConnectionConfigs.SocketAddress) async throws {
        let connection = try await createTestConnection(socketAddress: socketAddress)

        let rows = try await connection.query(
            """
            SELECT
              true::bool,
              array[true, false]::bool[],
              42::int2,
              array[42, 43]::int2[],
              42::int4,
              array[42, 43]::int4[],
              42::int8,
              array[42, 43]::int8[],
              'Hello, world!'::text,
              'Hello, world!'::varchar,
              array['Hello', 'World']::text[],
              array['Hello', 'World']::varchar[],
              42::float4,
              array[42, 43]::float4[],
              42::float8,
              array[42, 43]::float8[],
              42::numeric,
              array[42, 43]::numeric[],
              '1970-01-01 00:00:42'::timestamp,
              array['1970-01-01 00:00:42', '1970-01-01 00:00:43']::timestamp[],
              '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a'::uuid,
              array['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[]
            """
        )

        for try await row in rows {
            typealias Row = (
                PostgreSQLDecodable, Any, Any, Any, Any,
                Any, Any, Any, Any, Any,
                Any, Any, Any, Any, Any,
                Any, Any, Any, Any, Any,
                Any, Any
            )
            let (
                got1, got2, got3, got4, got5,
                got6, got7, got8, got9, got10,
                got11, got12, got13, got14, got15,
                got16, got17, got18, got19, got20,
                got21, got22
            ): Row = try row.decode()

            #expect(got1 as! Bool == true)
            #expect(got2 as! [Bool] == [true, false])
            #expect(got3 as! Int16 == 42)
            #expect(got4 as! [Int16] == [42, 43])
            #expect(got5 as! Int32 == 42)
            #expect(got6 as! [Int32] == [42, 43])
            #expect(got7 as! Int64 == 42)
            #expect(got8 as! [Int64] == [42, 43])
            #expect(got9 as! String == "Hello, world!")
            #expect(got10 as! String == "Hello, world!")
            #expect(got11 as! [String] == ["Hello", "World"])
            #expect(got12 as! [String] == ["Hello", "World"])
            #expect(got13 as! Float == 42)
            #expect(got14 as! [Float] == [42, 43])
            #expect(got15 as! Double == 42)
            #expect(got16 as! [Double] == [42, 43])
            #expect(got17 as! Decimal == Decimal(string: "42")!)
            #expect(got18 as! [Decimal] == [Decimal(string: "42")!, Decimal(string: "43")!])
            #expect(got19 as! Date == Date(timeIntervalSince1970: 42))
            #expect(
                got20 as! [Date] == [
                    Date(timeIntervalSince1970: 42),
                    Date(timeIntervalSince1970: 43),
                ])
            #expect(
                got21 as! UUID == UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!)
            #expect(
                got22 as! [UUID] == [
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
                    UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
                ])
        }
    }

    private func createTestConnection(socketAddress: ConnectionConfigs.SocketAddress) async throws -> Connection {
        let conn = Connection()
        let configs = ConnectionConfigs(
            socketAddress: socketAddress,
            username: "user_trust",
            sslmode: .disable,
        )
        try await conn.connect(configs: configs)
        return conn
    }
}
