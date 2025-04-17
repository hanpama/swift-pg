import NIO

enum PostgreSQLFrontendMessage: Sendable {
  case bind(
    portalName: String,
    statementName: String,
    parameterFormatCodes: [Int16],
    parameterValueCount: Int,
    parameterValues: ByteBuffer,
    resultColumnFormatCodes: [Int16]
  )
  case close(variant: UInt8, _ name: String)
  case describe(variant: UInt8, _ name: String)
  case execute(_ portalName: String, _ rowLimit: Int32)
  case flush
  case parse(_ statementName: String, _ sql: String)
  case saslInitialResponse(mechanism: String, initialResponse: String?)
  case saslResponse(_ response: String)
  case startupMessage(_ user: String, _ database: String)
  case sync
  case terminate
}

enum PostgreSQLBackendMessage: Sendable {
  case authenticationOk
  case authenticationKerberosV5
  case authenticationCleartextPassword
  case authenticationMD5Password(_ salt: [UInt8])
  case authenticationGSS
  case authenticationGSSContinue
  case authenticationSSPI
  case authenticationSasl(_ mechanisms: [String])
  case authenticationSaslContinue(String)
  case authenticationSaslFinal(String)
  case backendKeyData(_ processID: Int32, _ secretKey: Int32)
  case bindComplete
  case closeComplete
  case commandComplete(_ commandTag: String)
  case copyData(_ data: [UInt8])
  case copyDone
  case copyInResponse(_ format: Int8, _ columnFormats: [Int16])
  case copyOutResponse(_ format: Int8, _ columnFormats: [Int16])
  case copyBothResponse(_ format: Int8, _ columnFormats: [Int16])
  case dataRow(columns: Int16, columnData: ByteBuffer)
  case emptyQueryResponse
  case errorResponse(_ message: PostgreSQLErrorNoticeMessage)
  case functionCallResponse(_ functionResult: [UInt8])
  case negotiateProtocolVersion(_ newestVersion: Int32, _ notRecognized: [String])
  case noData
  case noticeResponse(_ noticeFields: PostgreSQLErrorNoticeMessage)
  case notificationResponse(_ processID: Int32, _ channel: String, _ payload: String)
  case parameterDescription(_ parameterOIDs: [Int32])
  case parameterStatus(_ parameter: String, _ value: String)
  case parseComplete
  case portalSuspended
  case readyForQuery(_ transactionStatus: UInt8)
  case rowDescription(_ fields: [PostgreSQLFieldDescription])
  case unknown  // Placeholder for unknown message types
}

typealias PostgreSQLErrorNoticeMessage = [PostgreSQLErrorNoticeMessageField]

enum PostgreSQLErrorNoticeMessageField: Sendable {
  case severity(String)
  case code(String)
  case message(String)
  case detail(String)
  case hint(String)
  case position(String)
  case internalPosition(String)
  case internalQuery(String)
  case where_(String)
  case schemaName(String)
  case tableName(String)
  case columnName(String)
  case dataTypeName(String)
  case constraintName(String)
  case file(String)
  case line(String)
  case routine(String)
}

struct PostgreSQLFieldDescription: Sendable {
  let name: String
  let tableOID: Int32
  let columnAttr: Int16
  let dataTypeOID: Int32
  let dataTypeSize: Int16
  let typeModifier: Int32
  let formatCode: Int16
}
