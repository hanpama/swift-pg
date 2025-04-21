import Foundation
import NIOSSL

public struct PostgreSQLConnectionConfigs: Sendable {

    let socketAddress: SocketAddress
    let username: String
    let password: String
    let database: String
    let sslmode: SSLMode
    let sslcert: String?
    let sslkey: String?
    let sslrootcert: String?
    let sslcrl: String?

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

    public static func fromDatabaseURL(_ databaseURL: String) throws -> PostgreSQLConnectionConfigs {
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

        let sslcert = components.queryItems?.first(where: { $0.name == "sslcert" })?.value
        let sslkey = components.queryItems?.first(where: { $0.name == "sslkey" })?.value
        let sslrootcert = components.queryItems?.first(where: { $0.name == "sslrootcert" })?.value
        let sslcrl = components.queryItems?.first(where: { $0.name == "sslcrl" })?.value

        return .init(
            socketAddress: socketAddress,
            username: user,
            password: password,
            database: database,
            sslmode: sslmode,
            sslcert: sslcert,
            sslkey: sslkey,
            sslrootcert: sslrootcert,
            sslcrl: sslcrl
        )
    }
}
