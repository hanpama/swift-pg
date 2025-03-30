import Foundation
import NIOSSL

public struct PostgreSQLConnectionConfigs: Sendable {

  let socketAddress: SocketAddress
  let username: String
  let password: String
  let database: String
  let tls: TLSConfiguration?
  // let sslmode: SSLMode?
  // let sslcert: String?
  // let sslkey: String?
  // let sslrootcert: String?
  // let sslcrl: String?

  enum SocketAddress {
    case hostPort(host: String, port: Int)
    case unixDomainSocket(path: String)
  }

  public enum SSLMode: String {
    case disable = "disable"
    case allow = "allow"
    case prefer = "prefer"
    case require = "require"
    case verifyCA = "verify-ca"
    case verifyFull = "verify-full"
  }
}
// sslmode, sslcert, sslkey, sslrootcert, and sslcrl
