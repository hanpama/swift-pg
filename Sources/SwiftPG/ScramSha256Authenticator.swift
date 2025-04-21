import Crypto
import Foundation

class ScramSha256Authenticator {
    let username: String
    let passwordData: SymmetricKey
    let clientNonce: String

    var saltedPasswordKey: SymmetricKey?
    var authMessage: String?
    var combinedNonce: String?

    init(username: String, password: String) throws {
        let randomBytes: [UInt8] = SymmetricKey(size: .bits256).withUnsafeBytes { Array($0) }

        guard let passwordData = password.data(using: .utf8) else {
            throw ScramSha256AuthenticatorError(message: "Failed to encode password to UTF-8 Data")
        }

        self.username = username
        self.passwordData = SymmetricKey(data: passwordData)
        self.clientNonce = Data(randomBytes).base64EncodedString()
    }

    func formatClientFirstMessage() -> String {
        let saslUsername = escapeSaslString(username)
        let clientFirstMessageBare = "n=\(saslUsername),r=\(clientNonce)"
        return "n,,\(clientFirstMessageBare)"
    }

    func handleServerFirstMessage(_ serverFirstMessage: String) throws {
        let parts = serverFirstMessage.split(separator: ",")
        var serverParams: [String: String] = [:]

        for part in parts {
            let keyValue = part.split(separator: "=", maxSplits: 1)
            if keyValue.count == 2 {
                serverParams[String(keyValue[0])] = String(keyValue[1])
            }
        }

        guard let combinedNonce = serverParams["r"] else {
            throw ScramSha256AuthenticatorError(
                message: "Server nonce (r) missing in server-first-message")
        }
        guard let saltBase64 = serverParams["s"] else {
            throw ScramSha256AuthenticatorError(
                message: "Salt (s) missing in server-first-message")
        }
        guard let iterationsStr = serverParams["i"], let iterations = Int(iterationsStr) else {
            throw ScramSha256AuthenticatorError(
                message: "Iterations (i) missing in server-first-message or not an integer")
        }
        guard iterations > 0 else {
            throw ScramSha256AuthenticatorError(
                message: "Iterations (i) must be a positive integer")
        }
        guard combinedNonce.starts(with: clientNonce) else {
            throw ScramSha256AuthenticatorError(
                message: "Server nonce (r) does not start with client nonce")
        }

        let saslUsername = escapeSaslString(username)
        let clientFirstMessageBare = "n=\(saslUsername),r=\(clientNonce)"
        let clientFinalWithoutProof = "c=biws,r=\(combinedNonce)"
        let authMessage =
            "\(clientFirstMessageBare),\(serverFirstMessage),\(clientFinalWithoutProof)"

        guard let saltData = Data(base64Encoded: saltBase64) else {
            throw ScramSha256AuthenticatorError(
                message: "Invalid base64 encoding for salt")
        }

        let derivedKey = pbkdf2SHA256(
            pass: passwordData,
            salt: saltData,
            iterations: iterations,
            outLen: SHA256.byteCount
        )

        self.saltedPasswordKey = SymmetricKey(data: derivedKey)
        self.authMessage = authMessage
        self.combinedNonce = combinedNonce
    }

    func formatClientFinalMessage() throws -> String {
        guard
            let saltedPasswordKey = saltedPasswordKey,
            let authMessage = authMessage,
            let combinedNonce = combinedNonce
        else {
            throw ScramSha256AuthenticatorError(
                message:
                    "Cannot format client final message: state is incomplete (call handleServerFirstMessage first)."
            )
        }

        let clientKeyMessage = "Client Key".data(using: .utf8)!
        let clientKeyMac = HMAC<SHA256>.authenticationCode(
            for: clientKeyMessage, using: saltedPasswordKey)

        let clientKeyData = Data(clientKeyMac)
        let storedKeyDigest = SHA256.hash(data: clientKeyData)
        let storedKey = SymmetricKey(data: storedKeyDigest)

        guard let authMessageData = authMessage.data(using: .utf8) else {
            throw ScramSha256AuthenticatorError(
                message: "Failed to encode authMessage to UTF-8 Data")
        }
        let clientSignatureMac = HMAC<SHA256>.authenticationCode(
            for: authMessageData, using: storedKey)
        let clientSignatureData = Data(clientSignatureMac)

        let clientProofData = xor(clientKeyData, clientSignatureData)
        let clientProofBase64 = clientProofData.base64EncodedString()

        let clientFinalWithoutProof = "c=biws,r=\(combinedNonce)"
        return "\(clientFinalWithoutProof),p=\(clientProofBase64)"
    }

    func handleServerFinalMessage(_ serverFinalMessage: String) throws {
        guard
            let saltedPasswordKey = saltedPasswordKey,
            let authMessage = authMessage
        else {
            throw ScramSha256AuthenticatorError(
                message: "Cannot handle server final message: state is incomplete.")
        }

        if serverFinalMessage.starts(with: "e=") {
            let errorMessage = serverFinalMessage.dropFirst(2)
            throw ScramSha256AuthenticatorError(
                message: "SCRAM Authentication failed on server: \(errorMessage)")
        }

        let parts = serverFinalMessage.split(separator: ",", maxSplits: 1)
        guard parts.first?.starts(with: "v=") ?? false else {
            throw ScramSha256AuthenticatorError(
                message: "Server signature (v=) missing or invalid in server-final-message")
        }

        let serverSignatureBase64 = String(parts[0].dropFirst(2))

        guard let serverSignatureData = Data(base64Encoded: serverSignatureBase64) else {
            throw ScramSha256AuthenticatorError(
                message: "Invalid base64 encoding for server signature (v)")
        }

        let serverKeyMessage = "Server Key".data(using: .utf8)!
        let serverKeyMac = HMAC<SHA256>.authenticationCode(
            for: serverKeyMessage, using: saltedPasswordKey)
        let serverKey = SymmetricKey(data: serverKeyMac)

        guard let authMessageData = authMessage.data(using: .utf8) else {
            throw ScramSha256AuthenticatorError(
                message: "Failed to encode authMessage to UTF-8 Data")
        }
        let expectedServerSignatureMac = HMAC<SHA256>.authenticationCode(
            for: authMessageData, using: serverKey)
        let expectedServerSignatureData = Data(expectedServerSignatureMac)

        guard serverSignatureData == expectedServerSignatureData else {
            throw ScramSha256AuthenticatorError(
                message: "Server signature verification failed. Server proof is incorrect.")
        }
    }

    private func escapeSaslString(_ string: String) -> String {
        return string.replacingOccurrences(of: "=", with: "=3D")
            .replacingOccurrences(of: ",", with: "=2C")
    }

    private func xor(_ data1: Data, _ data2: Data) -> Data {
        guard data1.count == data2.count else {
            fatalError("Data lengths do not match for XOR operation")
        }
        var result = Data(count: data1.count)
        for i in 0..<data1.count {
            result[i] = data1[i] ^ data2[i]
        }
        return result
    }

    private func pbkdf2SHA256(pass: SymmetricKey, salt: Data, iterations: Int, outLen: Int) -> Data {
        let hashLength = 32
        let blocks = (outLen + hashLength - 1) / hashLength
        var derivedKey = Data()

        for block in 1...blocks {
            var saltAndCounter = salt
            var counter = UInt32(block).bigEndian
            withUnsafeBytes(of: &counter) { counterBytes in
                saltAndCounter.append(contentsOf: counterBytes)
            }

            var U = Data(HMAC<SHA256>.authenticationCode(for: saltAndCounter, using: pass))
            var T = Data(U)

            for _ in 1..<iterations {
                U = Data(HMAC<SHA256>.authenticationCode(for: U, using: pass))
                T = xor(T, U)
            }
            derivedKey.append(contentsOf: T)
        }
        return derivedKey.prefix(outLen)
    }
}

public struct ScramSha256AuthenticatorError: Error {
    let message: String
}
