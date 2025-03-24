import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLCodecTest {

  @Test func testEncoding() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query(
      """
      SELECT
        $1::bool = true,
        $2::int8 = 42,
        $3::int2 = 42,
        $4::int4 = 42,
        $5::text = 'Hello, world!',
        $6::float4 = 42,
        $7::float8 = 42,
        $8::varchar = 'Hello, world!',
        $9::timestamp = '1970-01-01 00:00:42',
        $10::uuid = '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a',
        $11::int4 = 42,
        $12::int8 = 42
      """,
      [
        true,
        Int64(42),
        Int16(42),
        Int32(42),
        "Hello, world!",
        Float(42),
        Double(42),
        "Hello, world!",
        Date(timeIntervalSince1970: 42),
        UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
        Int(42),
        Int(42),
      ]
    )

    for try await row in rows {
      #expect(try row.get(Bool.self, at: 0))
      #expect(try row.get(Bool.self, at: 1))
      #expect(try row.get(Bool.self, at: 2))
      #expect(try row.get(Bool.self, at: 3))
      #expect(try row.get(Bool.self, at: 4))
      #expect(try row.get(Bool.self, at: 5))
      #expect(try row.get(Bool.self, at: 6))
      #expect(try row.get(Bool.self, at: 7))
      #expect(try row.get(Bool.self, at: 8))
      #expect(try row.get(Bool.self, at: 9))
      #expect(try row.get(Bool.self, at: 10))
      #expect(try row.get(Bool.self, at: 11))
    }
  }

  @Test func testEncodingNulls() async throws {
    let connection: PostgreSQLConnection = try await createConnectionSASL()
    let rows = try await connection.query(
      """
      SELECT
        $1::bool is null,
        $2::int8 is null,
        $3::int2 is null,
        $4::int4 is null,
        $5::text is null,
        $6::float4 is null,
        $7::float8 is null,
        $8::varchar is null,
        $9::timestamp is null,
        $10::uuid is null,
        $11::int4 is null,
        $12::int8 is null
      """,
      [
        nil as Bool?,
        nil as Int64?,
        nil as Int16?,
        nil as Int32?,
        nil as String?,
        nil as Float?,
        nil as Double?,
        nil as String?,
        nil as Date?,
        nil as UUID?,
        nil as Int?,
        nil as Int?,
      ]
    )

    for try await row in rows {
      #expect(try row.get(Bool.self, at: 0))
      #expect(try row.get(Bool.self, at: 1))
      #expect(try row.get(Bool.self, at: 2))
      #expect(try row.get(Bool.self, at: 3))
      #expect(try row.get(Bool.self, at: 4))
      #expect(try row.get(Bool.self, at: 5))
      #expect(try row.get(Bool.self, at: 6))
      #expect(try row.get(Bool.self, at: 7))
      #expect(try row.get(Bool.self, at: 8))
      #expect(try row.get(Bool.self, at: 9))
      #expect(try row.get(Bool.self, at: 10))
      #expect(try row.get(Bool.self, at: 11))
    }
  }

  @Test func testEncodingArrays() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query(
      """
      SELECT
        $1::bool[] = ARRAY[true, false]::bool[],
        $2::int8[] = ARRAY[42, 43]::int8[],
        $3::int2[] = ARRAY[42, 43]::int2[],
        $4::int4[] = ARRAY[42, 43]::int4[],
        $5::text[] = ARRAY['Hello', 'World']::text[],
        $6::float4[] = ARRAY[42, 43]::float4[],
        $7::float8[] = ARRAY[42, 43]::float8[],
        $8::varchar[] = ARRAY['Hello', 'World']::varchar[],
        $9::timestamp[] = ARRAY['1970-01-01 00:00:42', '1970-01-01 00:00:43']::timestamp[],
        $10::uuid[] = ARRAY['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[],
        $11::int4[] = ARRAY[42, 43]::int4[],
        $12::int8[] = ARRAY[42, 43]::int8[]
      """,
      [
        [true, false],
        [Int64(42), Int64(43)],
        [Int16(42), Int16(43)],
        [Int32(42), Int32(43)],
        ["Hello", "World"],
        [Float(42), Float(43)],
        [Double(42), Double(43)],
        ["Hello", "World"],
        [
          Date(timeIntervalSince1970: 42),
          Date(timeIntervalSince1970: 43),
        ],
        [
          UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
          UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
        ],
        [Int(42), Int(43)],
        [Int(42), Int(43)],
      ]
    )

    for try await row in rows {
      #expect(try row.get(Bool.self, at: 0))
      #expect(try row.get(Bool.self, at: 1))
      #expect(try row.get(Bool.self, at: 2))
      #expect(try row.get(Bool.self, at: 3))
      #expect(try row.get(Bool.self, at: 4))
      #expect(try row.get(Bool.self, at: 5))
      #expect(try row.get(Bool.self, at: 6))
      #expect(try row.get(Bool.self, at: 7))
      #expect(try row.get(Bool.self, at: 8))
      #expect(try row.get(Bool.self, at: 9))
      #expect(try row.get(Bool.self, at: 10))
      #expect(try row.get(Bool.self, at: 11))
    }
  }

  @Test func testDecoding() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query(
      """
      SELECT
        true,
        42::int8,
        42::int2,
        42::int4,
        'Hello, world!'::text,
        42::float4,
        42::float8,
        'Hello, world!'::varchar,
        '1970-01-01 00:00:42'::timestamp,
        '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a'::uuid,
        42::int4,
        42::int8
      """
    )

    for try await row: PostgreSQLRow in rows {
      #expect(try row.get(Bool.self, at: 0) == true)
      #expect(try row.get(Int64.self, at: 1) == 42)
      #expect(try row.get(Int16.self, at: 2) == 42)
      #expect(try row.get(Int32.self, at: 3) == 42)
      #expect(try row.get(String.self, at: 4) == "Hello, world!")
      #expect(try row.get(Float.self, at: 5) == 42)
      #expect(try row.get(Double.self, at: 6) == 42)
      #expect(try row.get(String.self, at: 7) == "Hello, world!")
      #expect(try row.get(Date.self, at: 8) == Date(timeIntervalSince1970: 42))
      #expect(
        try row.get(UUID.self, at: 9) == UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")
      )
      #expect(try row.get(Int.self, at: 10) == 42)
      #expect(try row.get(Int.self, at: 11) == 42)
    }
  }

  @Test func testDecodingNulls() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query(
      """
      SELECT
        NULL::bool,
        NULL::int8,
        NULL::int2,
        NULL::int4,
        NULL::text,
        NULL::float4,
        NULL::float8,
        NULL::varchar,
        NULL::timestamp,
        NULL::uuid,
        NULL::int4,
        NULL::int8
      """
    )

    for try await row: PostgreSQLRow in rows {
      #expect(try row.get(Bool?.self, at: 0) == nil)
      #expect(try row.get(Int64?.self, at: 1) == nil)
      #expect(try row.get(Int16?.self, at: 2) == nil)
      #expect(try row.get(Int32?.self, at: 3) == nil)
      #expect(try row.get(String?.self, at: 4) == nil)
      #expect(try row.get(Float?.self, at: 5) == nil)
      #expect(try row.get(Double?.self, at: 6) == nil)
      #expect(try row.get(String?.self, at: 7) == nil)
      #expect(try row.get(Date?.self, at: 8) == nil)
      #expect(try row.get(UUID?.self, at: 9) == nil)
      #expect(try row.get(Int?.self, at: 10) == nil)
      #expect(try row.get(Int?.self, at: 11) == nil)
    }
  }

  @Test func testDecodingArrays() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query(
      """
      SELECT
        ARRAY[true, false],
        ARRAY[42, 43]::int8[],
        ARRAY[42, 43]::int2[],
        ARRAY[42, 43]::int4[],
        ARRAY['Hello', 'World'],
        ARRAY[42, 43]::float4[],
        ARRAY[42, 43]::float8[],
        ARRAY['Hello', 'World']::varchar[],
        ARRAY['1970-01-01 00:00:42', '1970-01-01 00:00:43']::timestamp[],
        ARRAY['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[],
        ARRAY[42, 43]::int4[],
        ARRAY[42, 43]::int8[]
      """
    )

    for try await row in rows {
      #expect(try row.get([Bool].self, at: 0) == [true, false])
      #expect(try row.get([Int64].self, at: 1) == [42, 43])
      #expect(try row.get([Int16].self, at: 2) == [42, 43])
      #expect(try row.get([Int32].self, at: 3) == [42, 43])
      #expect(try row.get([String].self, at: 4) == ["Hello", "World"])
      #expect(try row.get([Float].self, at: 5) == [42, 43])
      #expect(try row.get([Double].self, at: 6) == [42, 43])
      #expect(try row.get([String].self, at: 7) == ["Hello", "World"])
      #expect(
        try row.get([Date].self, at: 8) == [
          Date(timeIntervalSince1970: 42),
          Date(timeIntervalSince1970: 43),
        ]
      )
      #expect(
        try row.get([UUID].self, at: 9) == [
          UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")!,
          UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789b")!,
        ]
      )
      #expect(try row.get([Int].self, at: 10) == [42, 43])
      #expect(try row.get([Int].self, at: 11) == [42, 43])
    }
  }

  @Test func testDecodingAny() async throws {
    let connection = try await createConnectionSASL()

    let rows = try await connection.query(
      """
      SELECT
        true,
        42::int8,
        42::int2,
        42::int4,
        'Hello, world!'::text,
        42::float4,
        42::float8,
        'Hello, world!'::varchar,
        '1970-01-01 00:00:42'::timestamp,
        '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a'::uuid
      """
    )

    for try await row: PostgreSQLRow in rows {
      let v1: Any = try row.get(at: 0)
      let v2: Any = try row.get(at: 1)
      let v3: Any = try row.get(at: 2)
      let v4: Any = try row.get(at: 3)
      let v5: Any = try row.get(at: 4)
      let v6: Any = try row.get(at: 5)
      let v7: Any = try row.get(at: 6)
      let v8: Any = try row.get(at: 7)
      let v9: Any = try row.get(at: 8)
      let v10: Any = try row.get(at: 9)

      #expect(v1 as! Bool == true)
      #expect(v2 as! Int64 == 42)
      #expect(v3 as! Int16 == 42)
      #expect(v4 as! Int32 == 42)
      #expect(v5 as! String == "Hello, world!")
      #expect(v6 as! Float == 42)
      #expect(v7 as! Double == 42)
      #expect(v8 as! String == "Hello, world!")
      #expect(v9 as! Date == Date(timeIntervalSince1970: 42))
      #expect(
        v10 as! UUID == UUID(uuidString: "6f41c8bc-38aa-481d-9b1d-1d2d6c81789a")
      )
    }
  }
}
