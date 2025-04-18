public protocol SwiftPGError: Error {}

public enum PostgreSQLError: Error, Sendable, Equatable {
  case databaseError(String)
  case clientError(String)
  case codecError(String)
  case operationTimeout
  case operationClosed
}

public struct DriverError: SwiftPGError {
  let description: String

  init(_ description: String) {
    self.description = description
  }
}

public enum ClientError: SwiftPGError {
  case codecError(String)
  case operationTimeout
  case operationClosed
}

extension DatabaseError: SwiftPGError {}
