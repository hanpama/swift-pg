# SwiftPG

SwiftPG is an async Swift PostgreSQL client built on SwiftNIO.

The package currently focuses on the core PostgreSQL protocol path:

- host/port and Unix domain socket connections
- SCRAM-SHA-256 authentication
- TLS negotiation with `disable`, `require`, `verify-ca`, and `verify-full`
- single query and execute operations
- batch query and execute operations
- binary encoding and decoding for common Swift/PostgreSQL types
- a small async connection pool

## Requirements

- Swift 6.1
- macOS 15 or Linux with the Swift 6.1 toolchain
- Docker, for the integration test suite

## Installation

Add SwiftPG to a Swift Package Manager package:

```swift
.package(url: "https://github.com/hanpama/swift-pg.git", branch: "main")
```

Then add the product to your target:

```swift
.product(name: "SwiftPG", package: "swift-pg")
```

## Usage

```swift
import SwiftPG

let config = ConnectionConfigs(
    socketAddress: .hostPort(host: "localhost", port: 5432),
    username: "postgres",
    password: "postgres",
    database: "postgres",
    sslmode: .disable
)

let connection = Connection()
try await connection.connect(configs: config)

let rows = try await connection.query("SELECT $1::int4", [Int32(42)])
for try await row in rows {
    let value: Int32 = try row.decode()
    print(value)
}

try await connection.execute("CREATE TEMP TABLE example(id int4)")
try await connection.execute("INSERT INTO example(id) VALUES ($1)", [Int32(1)])

try await connection.close()
```

Database URLs can also be parsed:

```swift
let config = try ConnectionConfigs.fromDatabaseURL(
    "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
)
```

## Connection Pool

```swift
let pool = ConnectionPool(configuration: config, maxConnections: 4)

let rows = try await pool.query("SELECT $1::text", ["hello"])
for try await row in rows {
    let value: String = try row.decode()
    print(value)
}
```

## Supported Value Types

SwiftPG includes binary codecs for:

- `Bool`
- `Int16`, `Int32`, `Int64`, `Int`
- `String`
- `Float`, `Double`, `Decimal`
- `Date`
- `UUID`
- `Optional`
- arrays whose elements conform to the PostgreSQL array codec contracts

## Testing

Run unit and scripted protocol tests locally:

```sh
swift test
```

Run the full integration suite with PostgreSQL 16 and 17:

```sh
docker compose up --quiet-pull --build -d postgres17 postgres16
docker compose run --quiet-pull test
```

If TLS tests fail after certificate or container changes, reset the generated test volumes and rerun:

```sh
docker compose down -v --remove-orphans
docker compose up --quiet-pull --build -d postgres17 postgres16
docker compose run --quiet-pull test
```

## Test Structure

The test suite is organized around distinct contract slices:

- `Contract`: protocol, codec, startup, row decoding, and public behavior that can be tested with scripted servers
- `Integration`: live PostgreSQL behavior across transport, authentication, TLS, query lifecycle, type mapping, and pooling
- `SwiftPGPublicAPITests`: compile-time and runtime checks for the intended public API surface

This structure is intended to keep protocol contracts explicit while reserving live database tests for behavior that requires PostgreSQL itself.
