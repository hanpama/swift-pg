import Foundation
import NIO

public protocol PostgreSQLCodable: PostgreSQLEncodable & PostgreSQLDecodable {}

public protocol PostgreSQLCodableArrayElement {
  static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32
}

extension Bool: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 1 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 16 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self ? 1 : 0, as: UInt8.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Bool as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 16 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Bool")
      }
      self = buffer.readInteger(as: UInt8.self) == 1
    } else {
      throw PostgreSQLError.codecError("Cannot decode Bool from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1000 {
      return 16
    }
    throw PostgreSQLError.codecError("Cannot get Bool element type oid from \(pgArrayTypeOid)")
  }
}

extension Int16: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 2 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 21 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self, as: Int16.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int16 as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 21 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int16")
      }
      guard let value = buffer.readInteger(as: Int16.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int16")
      }
      self = value
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int16 from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1005 {
      return 21
    }
    throw PostgreSQLError.codecError("Cannot get Int16 element type oid from \(pgArrayTypeOid)")
  }
}

extension Int32: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 4 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 23 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self, as: Int32.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int32 as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 23 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int32")
      }
      guard let value = buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int32")
      }
      self = value
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int32 from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1007 {
      return 23
    }
    throw PostgreSQLError.codecError("Cannot get Int32 element type oid from \(pgArrayTypeOid)")
  }
}

extension Int64: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 8 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 20 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self, as: Int64.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int64 as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 20 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int64")
      }
      guard let value = buffer.readInteger(as: Int64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int64")
      }
      self = value
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int64 from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1016 {
      return 20
    }
    throw PostgreSQLError.codecError("Cannot get Int64 element type oid from \(pgArrayTypeOid)")
  }
}

extension Int: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 20 {
      try Int64(self).encode(typeOid: 20, buffer: &buffer)
    } else if typeOid == 23 {
      guard self >= Int32.min && self <= Int32.max else {
        throw PostgreSQLError.codecError("Integer \(self) out of bounds for int4")
      }
      try Int32(self).encode(typeOid: 23, buffer: &buffer)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Int as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 20 {
      guard let length = buffer.readInteger(as: Int32.self), length == 8 else {
        throw PostgreSQLError.codecError("Invalid data for Int")
      }
      guard let value = buffer.readInteger(as: Int64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Int")
      }
      guard value >= Int.min && value <= Int.max else {
        throw PostgreSQLError.codecError("bigint \(value) out of bounds for Int")
      }
      self = Int(value)
    } else if pgTypeOid == 23 {
      let value = try Int32(pgTypeOid: 23, buffer: &buffer)
      self = Int(value)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Int from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1016 {
      return 20
    } else if pgArrayTypeOid == 1007 {
      return 23
    }
    throw PostgreSQLError.codecError("Cannot get Int element type oid from \(pgArrayTypeOid)")
  }
}

extension String: PostgreSQLCodable, PostgreSQLCodableArrayElement {

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 25 || typeOid == 1043 {
      buffer.writeInteger(Int32(utf8.count), as: Int32.self)
      buffer.writeBytes(utf8)
    } else {
      throw PostgreSQLError.codecError("Cannot encode String as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 25 || pgTypeOid == 1043 {
      guard let length = buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for String")
      }
      guard let string = buffer.readString(length: Int(length)) else {
        throw PostgreSQLError.codecError("Invalid data for String")
      }
      self = string
    } else {
      throw PostgreSQLError.codecError("Cannot decode String from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1009 {
      return 25
    } else if pgArrayTypeOid == 1015 {
      return 1043
    }
    throw PostgreSQLError.codecError("Cannot get String element type oid from \(pgArrayTypeOid)")
  }
}

extension Float: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 4 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 700 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self.bitPattern, as: UInt32.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Float as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 700 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Float")
      }
      guard let bitPattern = buffer.readInteger(as: UInt32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Float")
      }
      self = Float(bitPattern: bitPattern)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Float from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1021 {
      return 700
    }
    throw PostgreSQLError.codecError("Cannot get Float element type oid from \(pgArrayTypeOid)")
  }
}

extension Double: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 8 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 701 {
      buffer.writeInteger(Self.pgDataLength, as: Int32.self)
      buffer.writeInteger(self.bitPattern, as: UInt64.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Double as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 701 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Double")
      }
      guard let bitPattern = buffer.readInteger(as: UInt64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Double")
      }
      self = Double(bitPattern: bitPattern)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Double from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1022 {
      return 701
    }
    throw PostgreSQLError.codecError("Cannot get Double element type oid from \(pgArrayTypeOid)")
  }
}

extension Decimal: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private enum Sign: Int16 {
    case positive = 0
    case negative = 16384
    case nan = -16384
    case posInfinity = -12288
    case negInfinity = -4096
  }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 1700 {
      let ndigits: Int16
      let weight: Int16
      let sign: Sign
      var dscale: Int16
      var digits: [UInt16] = []

      if self.isNaN {
        (ndigits, weight, sign, dscale) = (0, 0, .nan, 0)
      } else if self.isZero {
        (ndigits, weight, sign, dscale) = (0, 0, .positive, 0)
      } else {
        let parts = String(describing: self).split(separator: ".")
        let signedIntegerPart = parts[0]
        let exp =
          signedIntegerPart.hasPrefix("-") ? signedIntegerPart.dropFirst() : signedIntegerPart
        let frac = parts.count > 1 ? parts[1] : ""

        let expDigits = stride(from: 0, to: exp.count, by: 4).map { i in
          let end = exp.index(exp.endIndex, offsetBy: -i)
          let start = exp.index(end, offsetBy: -4, limitedBy: exp.startIndex) ?? exp.startIndex
          return String(exp[start..<end])
        }.reversed()
        let fracDigits = stride(from: 0, to: frac.count, by: 4).map { i in
          let start = frac.index(frac.startIndex, offsetBy: i)
          let end = frac.index(start, offsetBy: 4, limitedBy: frac.endIndex) ?? frac.endIndex
          let digit = String(frac[start..<end])
          return digit.padding(toLength: 4, withPad: "0", startingAt: 0)
        }

        digits = (expDigits + fracDigits).map { UInt16($0)! }
        ndigits = Int16(digits.count)
        weight = Int16(expDigits.count - 1)
        sign = self.isSignMinus ? .negative : .positive
        dscale = Int16(frac.count)
      }
      buffer.writeInteger(Int32(ndigits) * 2 + 8, as: Int32.self)
      buffer.writeInteger(ndigits, as: Int16.self)
      buffer.writeInteger(weight, as: Int16.self)
      buffer.writeInteger(sign.rawValue, as: Int16.self)
      buffer.writeInteger(dscale, as: Int16.self)
      for digit in digits {
        buffer.writeInteger(digit, as: UInt16.self)
      }

    } else {
      throw PostgreSQLError.codecError("Cannot encode Decimal as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 1700 {
      guard buffer.readInteger(as: Int32.self) != nil else {
        throw PostgreSQLError.codecError("Invalid data for Decimal")
      }
      guard let ndigits = buffer.readInteger(as: Int16.self),
        let weight = buffer.readInteger(as: Int16.self),
        let sign = buffer.readInteger(as: Int16.self),
        let sign = Sign(rawValue: sign),
        buffer.readInteger(as: Int16.self) != nil  // dscale
      else {
        throw PostgreSQLError.codecError("Invalid data for Decimal")
      }
      var digits = [UInt16]()
      for _ in 0..<ndigits {
        guard let digit = buffer.readInteger(as: UInt16.self) else {
          throw PostgreSQLError.codecError("Invalid data for Decimal")
        }
        digits.append(digit)
      }
      if case .posInfinity = sign, case .negInfinity = sign {
        throw PostgreSQLError.codecError("Decimal infinity unsupported")
      } else if case .nan = sign {
        self = Decimal.nan
      } else if ndigits == 0 {
        self = Decimal.zero
      } else {
        let unsignedString = digits.enumerated().map {
          String(format: "%04d", $1) + ($0 == weight ? "." : "")
        }.joined()
        guard let unsigned = Decimal(string: unsignedString) else {
          throw PostgreSQLError.codecError("Invalid data for Decimal")
        }
        if sign == .negative {
          self = -unsigned
        } else {
          self = unsigned
        }
      }
    } else {
      throw PostgreSQLError.codecError("Cannot decode Decimal from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1231 {
      return 1700
    }
    throw PostgreSQLError.codecError("Cannot get Decimal element type oid from \(pgArrayTypeOid)")
  }
}

extension Date: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 8 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 1114 || typeOid == 1184 {
      let epochDifference: TimeInterval = 946_684_800  // PostgreSQL epoch offset for timestamp without time zone
      let microseconds = Int64((timeIntervalSince1970 - epochDifference) * 1_000_000)

      buffer.writeInteger(Self.pgDataLength)
      buffer.writeInteger(microseconds, as: Int64.self)
    } else {
      throw PostgreSQLError.codecError("Cannot encode Date as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 1114 || pgTypeOid == 1184 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for Date")
      }
      guard let microseconds = buffer.readInteger(as: Int64.self) else {
        throw PostgreSQLError.codecError("Invalid data for Date")
      }
      let epochDifference: TimeInterval = 946_684_800
      self = Date(
        timeIntervalSince1970: TimeInterval(microseconds) / 1_000_000 + epochDifference)
    } else {
      throw PostgreSQLError.codecError("Cannot decode Date from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 1185 {
      return 1184
    } else if pgArrayTypeOid == 1115 {
      return 1114
    }
    throw PostgreSQLError.codecError("Cannot get Date element type oid from \(pgArrayTypeOid)")
  }
}

extension UUID: PostgreSQLCodable, PostgreSQLCodableArrayElement {
  private static var pgDataLength: Int32 { 16 }

  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if typeOid == 2950 {
      let b = uuid
      buffer.writeInteger(Self.pgDataLength)
      buffer.writeBytes([
        b.0, b.1, b.2, b.3, b.4, b.5, b.6, b.7,
        b.8, b.9, b.10, b.11, b.12, b.13, b.14, b.15,
      ])
    } else {
      throw PostgreSQLError.codecError("Cannot encode UUID as \(typeOid)")
    }
  }

  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    if pgTypeOid == 2950 {
      guard Self.pgDataLength == buffer.readInteger(as: Int32.self) else {
        throw PostgreSQLError.codecError("Invalid data for UUID")
      }
      guard let b = buffer.readBytes(length: 16) else {
        throw PostgreSQLError.codecError("Invalid data for UUID")
      }
      self = .init(
        uuid: (
          b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
          b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
        ))
    } else {
      throw PostgreSQLError.codecError("Cannot decode UUID from \(pgTypeOid)")
    }
  }

  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    if pgArrayTypeOid == 2951 {
      return 2950
    }
    throw PostgreSQLError.codecError("Cannot get UUID element type oid from \(pgArrayTypeOid)")
  }
}

extension Optional: PostgreSQLEncodable where Wrapped: PostgreSQLEncodable {
  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    if let value = self {
      try value.encode(typeOid: typeOid, buffer: &buffer)
    } else {
      buffer.writeInteger(-1, as: Int32.self)
    }
  }
}

extension Optional: PostgreSQLDecodable where Wrapped: PostgreSQLDecodable {
  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    guard let length = buffer.getInteger(at: buffer.readerIndex, as: Int32.self) else {
      throw PostgreSQLError.codecError("Invalid data for Optional")
    }
    if length == -1 {
      buffer.moveReaderIndex(forwardBy: 4)
      self = nil
    } else {
      self = try Wrapped(pgTypeOid: pgTypeOid, buffer: &buffer)
    }
  }
}

extension Optional: PostgreSQLCodableArrayElement where Wrapped: PostgreSQLCodableArrayElement {
  public static func pgArrayElemTypeOid(pgArrayTypeOid: Int32) throws -> Int32 {
    return try Wrapped.pgArrayElemTypeOid(pgArrayTypeOid: pgArrayTypeOid)
  }
}

extension Array: PostgreSQLEncodable
where Element: PostgreSQLCodableArrayElement, Element: PostgreSQLEncodable {
  public func encode(typeOid: Int32, buffer: inout ByteBuffer) throws {
    let elementTypeOid = try Element.pgArrayElemTypeOid(pgArrayTypeOid: typeOid)
    let arrayShape = [Int32(self.count)]
    let arrayNdim = Int32(arrayShape.count)
    let elementHasNull = self.contains { val in
      return if case .none = val as Any? { true } else { false }
    }

    var body = ByteBuffer()
    body.writeInteger(arrayNdim, as: Int32.self)
    body.writeInteger(elementHasNull ? 1 : 0, as: Int32.self)
    body.writeInteger(elementTypeOid, as: Int32.self)
    for dim in arrayShape {
      body.writeInteger(dim, as: Int32.self)
      body.writeInteger(1, as: Int32.self)
    }
    for element in self {
      try element.encodeElem(pgArrayTypeOid: typeOid, buffer: &body)
    }

    buffer.writeInteger(Int32(body.readableBytes), as: Int32.self)
    buffer.writeBuffer(&body)
  }
}

extension Array: PostgreSQLDecodable
where Element: PostgreSQLCodableArrayElement, Element: PostgreSQLDecodable {
  public init(pgTypeOid: Int32, buffer: inout ByteBuffer) throws {
    guard buffer.readInteger(as: Int32.self) != nil,  // size
      let ndim: Int32 = buffer.readInteger(as: Int32.self),
      buffer.readInteger(as: Int32.self) != nil,  // frags
      buffer.readInteger(as: Int32.self) != nil,  // elemTypeOid
      let count = buffer.readInteger(as: Int32.self),
      let lbound = buffer.readInteger(as: Int32.self)
    else {
      throw PostgreSQLError.codecError("Invalid array data")
    }
    guard ndim == 1, lbound == 1 else {
      throw PostgreSQLError.codecError("Invalid 1dim array data")
    }
    let elements = try (0..<count).map { _ in
      try Element?(pgArrayTypeOid: pgTypeOid, buffer: &buffer)
    }
    self = elements as! [Element]
  }
}

extension PostgreSQLCodableArrayElement where Self: PostgreSQLEncodable {
  public func encodeElem(pgArrayTypeOid: Int32, buffer: inout ByteBuffer) throws {
    let elemTypeOid = try Self.pgArrayElemTypeOid(pgArrayTypeOid: pgArrayTypeOid)
    try self.encode(typeOid: elemTypeOid, buffer: &buffer)
  }
}

extension PostgreSQLCodableArrayElement where Self: PostgreSQLDecodable {
  public init(pgArrayTypeOid: Int32, buffer: inout ByteBuffer) throws {
    let elemTypeOid = try Self.pgArrayElemTypeOid(pgArrayTypeOid: pgArrayTypeOid)
    self = try .init(pgTypeOid: elemTypeOid, buffer: &buffer)
  }
}


let DEFAULT_DECODER_MAP: [Int32: PostgreSQLDecodable.Type] = [
  16: Bool.self,
  1000: [Bool].self,
  21: Int16.self,
  1005: [Int16].self,
  23: Int32.self,
  1007: [Int32].self,
  20: Int64.self,
  1016: [Int64].self,
  25: String.self,
  1043: String.self,
  1009: [String].self,
  1015: [String].self,
  700: Float.self,
  1021: [Float].self,
  701: Double.self,
  1022: [Double].self,
  1700: Decimal.self,
  1231: [Decimal].self,
  1114: Date.self,
  1184: Date.self,
  1115: [Date].self,
  1185: [Date].self,
  2950: UUID.self,
  2951: [UUID].self,
]
