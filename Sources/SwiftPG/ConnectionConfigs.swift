import Foundation
import NIOSSL

public struct ConnectionConfigs: Sendable {

    var socketAddress: SocketAddress
    var username: String
    var password: String
    var database: String
    var sslmode: SSLMode
    var sslcert: NIOSSLCertificate?
    var sslkey: NIOSSLPrivateKey?
    var sslrootcert: NIOSSLAdditionalTrustRoots?
    // var sslcrl: String?

    init(
        socketAddress: SocketAddress = .hostPort(host: "localhost", port: 5432),
        username: String = "postgres",
        password: String = "",
        database: String = "postgres",
        sslmode: SSLMode = .require,
        sslcert: NIOSSLCertificate? = nil,
        sslkey: NIOSSLPrivateKey? = nil,
        sslrootcert: NIOSSLAdditionalTrustRoots? = nil,
        // sslcrl: String? = nil
    ) {
        self.socketAddress = socketAddress
        self.username = username
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.sslrootcert = sslrootcert
        // self.sslcrl = sslcrl
    }

    enum SocketAddress {
        case hostPort(host: String, port: Int)
        case unixDomainSocket(directory: String, port: Int)
    }

    public enum SSLMode: String, Sendable {
        case disable = "disable"
        case require = "require"
        case verifyCA = "verify-ca"
        case verifyFull = "verify-full"
    }

    public static func fromDatabaseURL(_ databaseURL: String) throws -> ConnectionConfigs {
        guard let components = URLComponents(string: databaseURL) else {
            throw ClientError.configurationError("Invalid URL")
        }
        guard let scheme = components.scheme, scheme == "postgres" || scheme == "postgresql" else {
            throw ClientError.configurationError("Invalid URL scheme: \(components.scheme ?? "")")
        }

        let hostPart = components.host
        let portPart = components.port
        let hostQuery = components.queryItems?.first(where: { $0.name == "host" })?.value
        let portQueryString = components.queryItems?.first(where: { $0.name == "port" })?.value
        let port = portPart ?? Int(portQueryString ?? "") ?? 5432

        let socketAddress: SocketAddress
        if let hostPart = hostPart, !hostPart.isEmpty {
            if hostPart.hasPrefix("/") {
                socketAddress = .unixDomainSocket(directory: hostPart, port: port)
            } else {
                socketAddress = .hostPort(host: hostPart, port: port)
            }
        } else if let hostQuery = hostQuery, !hostQuery.isEmpty {
            if hostQuery.hasPrefix("/") {
                socketAddress = .unixDomainSocket(directory: hostQuery, port: port)
            } else {
                socketAddress = .hostPort(host: hostQuery, port: port)
            }
        } else {
            socketAddress = .hostPort(host: "localhost", port: port)
        }

        let user = components.user ?? "postgres"
        let password = components.password ?? ""
        let database = components.path.count > 1 ? String(components.path.dropFirst()) : "postgres"

        let sslmodeQuery =
            components.queryItems?.first(where: { $0.name == "sslmode" })?.value ?? "require"
        let sslmode: SSLMode
        switch sslmodeQuery {
        case "disable":
            sslmode = .disable
        case "require":
            sslmode = .require
        case "verify-ca":
            sslmode = .verifyCA
        case "verify-full":
            sslmode = .verifyFull
        default:
            throw ClientError.configurationError("Invalid sslmode: \(sslmodeQuery)")
        }

        let sslcertString = components.queryItems?.first(where: { $0.name == "sslcert" })?.value
        let sslkeyString = components.queryItems?.first(where: { $0.name == "sslkey" })?.value
        let sslrootcertString = components.queryItems?.first(where: { $0.name == "sslrootcert" })?.value
        // let sslcrlString = components.queryItems?.first(where: { $0.name == "sslcrl" })?.value

        var sslcert: NIOSSLCertificate?
        var sslkey: NIOSSLPrivateKey?
        var sslrootcert: NIOSSLAdditionalTrustRoots?
        // let sslcrl: String?

        if let sslcertString = sslcertString {
            let certs = try NIOSSLCertificate.fromPEMFile(sslcertString)
            sslcert = certs.first
        }
        if let sslkeyString = sslkeyString {
            let privateKey = try NIOSSLPrivateKey(file: sslkeyString, format: .pem)
            sslkey = privateKey
        }
        if let sslrootcertString = sslrootcertString {
            let certs = try NIOSSLCertificate.fromPEMFile(sslrootcertString)
            sslrootcert = .file(sslrootcertString)
        }

        return .init(
            socketAddress: socketAddress,
            username: user,
            password: password,
            database: database,
            sslmode: sslmode,
            sslcert: sslcert,
            sslkey: sslkey,
            sslrootcert: sslrootcert,
            // sslcrl: sslcrl
        )
    }
}
