//
//  PCKAttributeValue.swift
//  ZeeQL3PCK
//
//  Created by Helge Heß on 15.08.19.
//  Copyright © 2019 Helge Heß. All rights reserved.
//

import struct   Foundation.Date
import struct   Foundation.Decimal
import struct   Foundation.URL
import class    Foundation.NSNumber
import struct   PostgresClientKit.PostgresTimestampWithTimeZone
import struct   PostgresClientKit.PostgresValue
import protocol PostgresClientKit.PostgresValueConvertible
import protocol ZeeQL.Attribute
import protocol ZeeQL.AttributeValue
import class    ZeeQL.SingleIntKeyGlobalID
import let      ZeeQL.globalZeeQLLogger

extension SingleIntKeyGlobalID: PostgresValueConvertible {
  public var postgresValue: PostgresValue { return value.postgresValue }
}
extension Date: PostgresValueConvertible {
  public var postgresValue: PostgresValue {
    return PostgresTimestampWithTimeZone(date: self).postgresValue
  }
}
extension Float: PostgresValueConvertible {
  public var postgresValue: PostgresValue { Double(self).postgresValue }
}
extension UInt64: PostgresValueConvertible {
  public var postgresValue: PostgresValue { String(self).postgresValue }
}
extension NSNumber: PostgresValueConvertible {
  public var postgresValue: PostgresValue {
    let ctDouble : Int8 = 100 // "d"
    let ctFloat  : Int8 = 102 // "f"
    let ctUInt64 : Int8 = 81  // "Q"
    switch objCType.pointee {
      // 99 is "c" which is also used as bool
      case ctDouble : return doubleValue.postgresValue
      case ctFloat  : return floatValue .postgresValue
      case ctUInt64 : return uint64Value.postgresValue
      default       : return intValue   .postgresValue
    }
  }
}

/**
 * Convert a `PostgresValue` from PostgresClientKit to the internal value
 * `Attribute` would like to have.
 */
protocol PCKAttributeValue: AttributeValue {
  
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any?
  
}

extension Optional: PCKAttributeValue where Wrapped : PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    if value.isNull { return Optional<Wrapped>.none }
    return try Wrapped.pckValue(value, a)
  }
}

fileprivate extension PostgresValue {
  @inline(__always) func zzVerifyNotNil() throws {
    guard !isNull else { throw PostgreSQLAdaptorChannel.Error.unexpectedNull }
  }
}

extension String: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    return try value.string()
  }
}

// FIXME: Those should just parse the raw value ...

extension PostgresValue {
  func int<V: FixedWidthInteger>(ofType type: V.Type) throws -> Int {
    try zzVerifyNotNil()
    let v = try int()
    guard v >= type.min && v <= type.max else {
      throw PostgreSQLAdaptorChannel.Error.conversionError(type, v)
    }
    return v
  }
}


extension Int: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    return try value.int()
  }
}
extension Int8: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return Int8(try value.int(ofType: Int8.self))
  }
}
extension Int16: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return Int16(try value.int(ofType: Int16.self))
  }
}
extension Int32: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return Int32(try value.int(ofType: Int32.self))
  }
}
extension Int64: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return Int64(try value.int(ofType: Int64.self))
  }
}
extension UInt8: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return UInt8(try value.int(ofType: UInt8.self))
  }
}
extension UInt16: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return UInt16(try value.int(ofType: UInt16.self))
  }
}
extension UInt32: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    return UInt32(try value.int(ofType: UInt32.self))
  }
}
extension UInt64: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    // FIXME: can break for valid value
    return UInt64(try value.int(ofType: UInt64.self))
  }
}

extension Float: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    guard let rv = value.rawValue else { return nil }
    guard let v  = Float(rv) else {
      throw PostgreSQLAdaptorChannel.Error.conversionError(Float.self, rv)
    }
    return v
  }
}
extension Double: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    return try value.double()
  }
}
extension Bool: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    return try value.bool()
  }
}

extension Decimal: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    return try value.decimal()
  }
}

import Foundation

extension Date: PCKAttributeValue {
  
  private static var dateHackFormatter: DateFormatter = {
    let df = DateFormatter()
    df.dateFormat = "yyyy-MM-dd HH:mm:ss"
    df.timeZone = TimeZone(secondsFromGMT: 0)
    return df
  }()
  private static var dateHackFormatter2: DateFormatter = {
    let df = DateFormatter()
    df.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    df.timeZone = TimeZone(secondsFromGMT: 0)
    return df
  }()

  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    // We get in the dvdrental db:
    //       "2006-02-15 10:09:17"
    // also: "2013-05-26 14:47:57.62"
    // But this is actually declared as "timestamp without time zone" (why?)
    /* PCK wants:
        let df = DateFormatter()
        df.calendar = Postgres.enUsPosixUtcCalendar
        df.dateFormat = "yyyy-MM-dd HH:mm:ss.SSSxxxxx"
        df.locale = Postgres.enUsPosixLocale
        df.timeZone = Postgres.utcTimeZone
     */
    
    // temp hack (lolz)
    if let s = value.rawValue {
      let count = s.count
      if count == 19, let d = dateHackFormatter.date(from: s) {
        return d // "2006-02-15 10:09:17"
      }
      if count > 20 && count < 29, let d = dateHackFormatter2.date(from: s) {
        return d // "2006-02-15 10:09:17.62" "2006-05-16 16:13:11.79328"
      }
    }
    
    return try value.timestampWithTimeZone().date
  }
}

extension URL: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    let s = try value.string()
    guard let v = URL(string: s) else {
      throw PostgreSQLAdaptorChannel.Error.conversionError(URL.self, s)
    }
    return v
  }
}

extension Data: PCKAttributeValue {
  static func pckValue(_ value: PostgresValue, _ a: Attribute) throws -> Any? {
    try value.zzVerifyNotNil()
    let s = try value.string()
    if s.isEmpty { return Data()}
    
    if a.externalType == nil || a.externalType == "BYTEA" {
      // https://www.postgresql.org/docs/9.0/datatype-binary.html
      // \x89504e470d0a5a0a
      if s.hasPrefix("E") {
        globalZeeQLLogger.error("bytea escape format is unsupported", a)
        assert(!s.hasPrefix("E"), "bytea escape format is unsupported")
        return nil
      }
      
      if s.hasPrefix("\\x") { // hex
        // 2 hexadecimal digits per byte, most significant nibble first
        let sub = s.dropFirst(2)
        guard sub.count % 2 == 0 else {
          globalZeeQLLogger.error("invalid hex encoding:", s, a)
          throw PostgreSQLAdaptorChannel.Error.conversionError(Data.self, s)
        }
        
        // Probably quite slow, but well, one should binary PG encoding ...
        // https://stackoverflow.com/questions/26501276/converting-hex-string
        var data = Data(capacity: sub.count / 2)
        var indexIsEven = true
        for i in sub.indices {
          defer { indexIsEven = !indexIsEven }
          if indexIsEven {
            let byteRange = i...sub.index(after: i)
            guard let byte = UInt8(sub[byteRange], radix: 16) else {
              globalZeeQLLogger.error("invalid hex encoding:", s, a)
              throw PostgreSQLAdaptorChannel.Error.conversionError(Data.self, s)
            }
            data.append(byte)
          }
        }
        return data
      }

      globalZeeQLLogger.error("unexpected data encoding:", s, a)
      throw PostgreSQLAdaptorChannel.Error.conversionError(Data.self, s)
    }
    else {
      globalZeeQLLogger.error("unsupported PG data type:", a)
      throw PostgreSQLAdaptorChannel.Error.conversionError(Data.self, s)
    }
  }
}
