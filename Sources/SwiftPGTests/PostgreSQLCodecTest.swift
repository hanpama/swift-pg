import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PostgreSQLCodecTest {

  @Test func testEncoding() async throws {
    let connection = try await createConnection()

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
        $10::uuid = '6f41c8bc-38aa-481d-9b1d-1d2d6c81789a'
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
      ]
    )

    for row in rows {
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
    }
  }

  @Test func testEncodingNulls() async throws {
    let connection: PostgreSQLConnection = try await createConnection()
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
        $10::uuid is null
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
      ]
    )

    for row in rows {
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
    }
  }

  @Test func testEncodingArrays() async throws {
    let connection = try await createConnection()

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
        $10::uuid[] = ARRAY['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[]
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
      ]
    )

    for row in rows {
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
    }
  }

  @Test func testDecoding() async throws {
    let connection = try await createConnection()

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

    for row: PostgreSQLRow in rows {
      let val: String = try row.get(at: 4)
      print(val)

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
    }
  }

  @Test func testDecodingNulls() async throws {
    let connection = try await createConnection()

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
        NULL::uuid
      """
    )

    for row: PostgreSQLRow in rows {
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
    }
  }

  @Test func testDecodingArrays() async throws {
    let connection = try await createConnection()

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
        ARRAY['6f41c8bc-38aa-481d-9b1d-1d2d6c81789a', '6f41c8bc-38aa-481d-9b1d-1d2d6c81789b']::uuid[]
      """
    )

    for row in rows {
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
    }
  }

  private func createConnection() async throws -> PostgreSQLConnection {
    let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    return try await PostgreSQLConnection.connect(
      eventLoopGroup: loopGroup,
      configuration: .init(
        host: "localhost",
        port: 6451,
        username: "postgres",
        password: "postgres",
        database: "postgres"
      ))
  }
}
