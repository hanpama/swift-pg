public protocol SwiftPGError: Error {}

public struct DriverError: SwiftPGError {
    let description: String

    init(_ description: String) {
        self.description = description
    }
}

public enum ClientError: SwiftPGError {
    case codecError(String)
    case configurationError(String)
    case connectionError(String)
    case concurrencyError(String)
    case operationTimeout
}

extension DatabaseError: SwiftPGError {
    static func from(errorMessage: PostgreSQLErrorNoticeMessage) -> Self {
        var severity: String?
        var code: String?
        var message: String?
        var detail: String?
        var hint: String?
        var position: Int?
        var internalPosition: Int?
        var internalQuery: String?
        var `where`: String?
        var schemaName: String?
        var tableName: String?
        var columnName: String?
        var dataTypeName: String?
        var constraintName: String?
        var file: String?
        var line: Int?
        var routine: String?
        for field in errorMessage {
            switch field {
            case .severity(let value): severity = value
            case .code(let value): code = value
            case .message(let value): message = value
            case .detail(let value): detail = value
            case .hint(let value): hint = value
            case .position(let value): position = Int(value)
            case .internalPosition(let value): internalPosition = Int(value)
            case .internalQuery(let value): internalQuery = value
            case .`where`(let value): `where` = value
            case .schemaName(let value): schemaName = value
            case .tableName(let value): tableName = value
            case .columnName(let value): columnName = value
            case .dataTypeName(let value): dataTypeName = value
            case .constraintName(let value): constraintName = value
            case .file(let value): file = value
            case .line(let value): line = Int(value)
            case .routine(let value): routine = value
            }
        }
        let details = DatabaseErrorDetails(
            severity: severity!,
            code: code!,
            message: message!,
            detail: detail,
            hint: hint,
            position: position,
            internalPosition: internalPosition,
            internalQuery: internalQuery,
            where: `where`,
            schemaName: schemaName,
            tableName: tableName,
            columnName: columnName,
            dataTypeName: dataTypeName,
            constraintName: constraintName,
            file: file,
            line: line,
            routine: routine
        )
        return DatabaseError(details: details)
    }
}
