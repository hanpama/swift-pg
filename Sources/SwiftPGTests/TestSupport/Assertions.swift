import Testing

@testable import SwiftPG

enum ExpectedDatabaseError {
    case invalidAuthorizationSpecification
    case invalidCatalogName
    case invalidTextRepresentation
    case invalidPassword
    case undefinedTable
}

func expectDatabaseError(
    _ error: DatabaseError?,
    _ expected: ExpectedDatabaseError
) {
    guard let error else {
        Issue.record("Expected \(expected), but no DatabaseError was thrown")
        return
    }

    switch (expected, error) {
    case (.invalidAuthorizationSpecification, .invalidAuthorizationSpecification),
        (.invalidCatalogName, .invalidCatalogName),
        (.invalidTextRepresentation, .invalidTextRepresentation),
        (.invalidPassword, .invalidPassword),
        (.undefinedTable, .undefinedTable):
        break
    default:
        Issue.record("Expected \(expected), got \(error)")
    }
}
