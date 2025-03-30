import NIOSSL

public struct PostgreSQLConnectionConfigs: Sendable {

  let socketAddress: SocketAddress
  let username: String
  let password: String
  let database: String
  let tls: TLSConfiguration?
  // let sslmode: String?
  // let sslcert: String?
  // let sslkey: String?
  // let sslrootcert: String?
  // let sslcrl: String?

  enum SocketAddress {
    case hostPort(host: String, port: Int)
    case unixDomainSocket(path: String)
  }
}
// sslmode, sslcert, sslkey, sslrootcert, and sslcrl
