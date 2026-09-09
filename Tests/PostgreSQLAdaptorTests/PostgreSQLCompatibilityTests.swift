import Foundation
import XCTest
import ZeeQL
import PostgresClientKit
@testable import PostgreSQLAdaptor

final class PostgreSQLCompatibilityTests: XCTestCase {

  func testNumberedPlaceholdersPreserveBindingMetadata() {
    let attribute = ModelAttribute(name: "name", externalType: "TEXT")
    let expression = PostgreSQLExpression()
    let expected = SQLExpression()
      .bindVariableDictionary(for: attribute, value: "Alice")
    let first = expression.bindVariableDictionary(for: attribute,
                                                  value: "Alice")
    let second = expression.bindVariableDictionary(for: nil, value: nil)

    XCTAssertEqual(first.placeholder, "$1")
    XCTAssertEqual(second.placeholder, "$2")
    XCTAssertTrue(first.attribute === attribute)
    XCTAssertEqual(first.name, expected.name)
    XCTAssertEqual(first.value as? String, "Alice")
    XCTAssertNil(second.attribute)
    XCTAssertNil(second.value)
  }

  func testScalarParameters() throws {
    XCTAssertEqual(try PostgreSQLAdaptorChannel.parameterValue(42).int(), 42)
    let text = try PostgreSQLAdaptorChannel.parameterValue("value")
    XCTAssertEqual(try text.string(), "value")
    XCTAssertTrue(try PostgreSQLAdaptorChannel.parameterValue(true).bool())
    XCTAssertTrue(try PostgreSQLAdaptorChannel.parameterValue(nil).isNull)
  }

  func testImportedScalarConformances() throws {
    let uuid = UUID()
    let url = try XCTUnwrap(URL(string: "https://example.test/path?q=1"))
    let values : [ ( PostgresValueConvertible, String ) ] = [
      ( Int32.min, "-2147483648" ),
      ( Int64.max, "9223372036854775807" ),
      ( UInt64.max, "18446744073709551615" ),
      ( uuid, uuid.uuidString ),
      ( url, url.absoluteString )
    ]
    for ( value, expected ) in values {
      XCTAssertEqual(value.postgresValue.rawValue, expected)
      let parameter = try PostgreSQLAdaptorChannel.parameterValue(value)
      XCTAssertEqual(parameter.rawValue, expected)
    }
  }

  func testScalarGlobalIDConformance() {
    let uuid = UUID()
    let values : [ ( PostgresValueConvertible, String? ) ] = [
      ( KeyGlobalID(entityName: "Object", value: 42), "42" ),
      ( KeyGlobalID(entityName: "Object", value: "abc"), "abc" ),
      ( KeyGlobalID(entityName: "Object", value: uuid), uuid.uuidString ),
      ( KeyGlobalID(entityName: "Object", values: [ nil ]), nil )
    ]
    for ( value, expected ) in values {
      XCTAssertEqual(value.postgresValue.rawValue, expected)
    }
  }

  func testIntegerGlobalID() throws {
    let key = KeyGlobalID(entityName: "Object", value: 42)
    XCTAssertEqual(try PostgreSQLAdaptorChannel.parameterValue(key).int(), 42)
  }

  func testStringGlobalID() throws {
    let key = KeyGlobalID(entityName: "Object", value: "abc")
    XCTAssertEqual(try PostgreSQLAdaptorChannel.parameterValue(key).string(),
                   "abc")
  }

  func testUUIDGlobalID() throws {
    let uuid = UUID()
    let key = KeyGlobalID(entityName: "Object", value: uuid)
    XCTAssertEqual(try PostgreSQLAdaptorChannel.parameterValue(key).string(),
                   uuid.uuidString)
    XCTAssertEqual(try PostgreSQLAdaptorChannel.parameterValue(uuid).string(),
                   uuid.uuidString)
  }

  func testNullGlobalID() throws {
    let key = KeyGlobalID(entityName: "Object", values: [ nil ])
    XCTAssertTrue(try PostgreSQLAdaptorChannel.parameterValue(key).isNull)
  }

  func testAnyHashableBackedGlobalID() throws {
    let key = KeyGlobalID(entityName: "Object", values: [ AnyHashable(1.25) ])
    XCTAssertEqual(try PostgreSQLAdaptorChannel.parameterValue(key).double(),
                   1.25)
  }

  func testCompoundAndEmptyGlobalIDsAreRejected() {
    typealias BindingError = PostgreSQLAdaptorChannel.Error

    let keys = [
      KeyGlobalID(entityName: "Object", values: []),
      KeyGlobalID(entityName: "Object", values: [ 1, 2 ]),
      KeyGlobalID(entityName: "Object", values: [ 1, nil ])
    ]
    for key in keys {
      XCTAssertThrowsError(try PostgreSQLAdaptorChannel.parameterValue(key)) {
        error in
        guard case BindingError.invalidGlobalID(let actual) = error else {
          return XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(actual, key)
      }
    }
  }
}
