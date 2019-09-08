//
//  TestZeeQL3PCK.swift
//  TestZeeQL3PCK
//
//  Created by Helge Heß on 14.08.19.
//  Copyright © 2019 Helge Heß. All rights reserved.
//

import XCTest
import ZeeQL
@testable import ZeeQL3PCK

class TestZeeQL3PCK: XCTestCase {
  // Assumes that the `dvdrental` database is configured:
  // http://www.postgresqltutorial.com/load-postgresql-sample-database/
  // createuser -s postgres
  // createdb dvdrental
  // cd dvdrental
  // pg_restore -h localhost -U postgres -d dvdrental .

  var adaptor : Adaptor! {
    XCTAssertNotNil(_adaptor)
    return _adaptor
  }
  var modelAdaptor : Adaptor! {
    XCTAssertNotNil(_modelAdaptor)
    return _modelAdaptor
  }
  lazy var database = Database(adaptor: modelAdaptor)

  let _adaptor = PostgreSQLAdaptor(database: "dvdrental")
  
  lazy var _modelAdaptor: Adaptor = {
    let adaptor = PostgreSQLAdaptor(database: "dvdrental")
    let model : Model? = {
      guard let channel = try? adaptor.openChannel() else { return nil }
      return try? PostgreSQLModelFetch(channel: channel).fetchModel()
    }()
    adaptor.model = model
    return adaptor
  }()

  func testConnect() throws {
    let channel = try adaptor.openChannel()
    let results = try channel.querySQL("SELECT * FROM actor;")
    assert(results.count > 100)
  }

  func testConnect2() throws {
    let adaptor = PostgreSQLAdaptor(database: "dvdrental")
    let channel = try adaptor.openChannel()
    
    do {
    try channel.select("SELECT actor_id, first_name, last_name FROM actor;") {
      ( id: Int, firstName: String, lastName: String ) in
      print("\(id): \(firstName) \(lastName)")
    }
    }
    catch {
      print("ERROR:", error)
      XCTAssertNil(error)
    }
  }

  func testDescribeDatabaseNames() throws {
    let channel = try adaptor.openChannel()
    let values  = try channel.describeDatabaseNames()
    
    XCTAssert(values.count >= 4)
    XCTAssert(values.contains("template0"))
    XCTAssert(values.contains("template1"))
    XCTAssert(values.contains("postgres"))
    XCTAssert(values.contains("dvdrental"))
  }
  
  func testDescribeRentalTableNames() throws {
    let channel = try adaptor.openChannel()
    let values  = try channel.describeTableNames()
    
    XCTAssert(values.count >= 15) // 126 in my OGo2 DB with extras
    XCTAssert(values.contains("film_actor"))
    XCTAssert(values.contains("customer"))
    XCTAssert(values.contains("store"))
  }

  func testFetchModel() throws {
    let channel = try adaptor.openChannel()
    let model   = try PostgreSQLModelFetch(channel: channel).fetchModel()
    
    XCTAssert(model.entities.count >= 15)
    let values = model.entityNames
    XCTAssert(values.contains("film_actor"))
    XCTAssert(values.contains("customer"))
    XCTAssert(values.contains("store"))
    
    if let entity = model[entity: "actor"] {
      print("Actor:", entity)
      XCTAssert(entity.attributes.count == 4)
    }
    if let entity = model[entity: "film_actor"] {
      print("film_actor:", entity)
      XCTAssert(entity.attributes   .count == 3)
      XCTAssert(entity.relationships.count == 2)
      
      // film_actor_actor_id_fkey
      // FIXME: should be just actor?
      let relshipName = "film_actor_actor_id_fkey"
      XCTAssertNotNil(entity[relationship: relshipName])
      if let relship = entity[relationship: relshipName] {
        print("  =>actor:", relship)
        XCTAssertEqual(relship.entity.name,             "film_actor")
        XCTAssertEqual(relship.destinationEntity?.name, "actor")
        XCTAssertFalse(relship.isToMany)
        XCTAssert(relship.isMandatory)
        XCTAssertEqual(relship.joins.count, 1)
        if let join = relship.joins.first {
          XCTAssertEqual(join.sourceName,      "actor_id")
          XCTAssertEqual(join.destinationName, "actor_id")
        }
      }
    }

    XCTAssertNotNil(model.tag, "model has no tag")
  }

  func testInventoryAdaptorFetch() throws {
    let adaptor = modelAdaptor!
    let entity = adaptor.model![entity: "inventory"]!
    
    let sqlAttrs = entity.attributes.map { $0.columnName ?? $0.name }
                         .joined(separator: ", ")
    
    do {
      let channel = try adaptor.openChannel()
      try channel.querySQL(
        "SELECT \(sqlAttrs) FROM \(entity.externalName ?? entity.name) LIMIT 1",
        entity.attributes
      ) { record in
        print("REC:", record)
      }
    }
    catch {
      print("ERROR:", error)
      XCTAssertNil(error)
    }
  }

  func testInventoryDataSourceFetch() throws {
    let db = database
    let ds = ActiveDataSource<ActiveRecord>(
      database: db, entity: db.model![entity: "inventory"]!)
    
    do {
      let results = try ds.fetchObjects()
      print("RESULTS: #\(results.count)")

      // test pool
      let results2 = try ds.fetchObjects()
      print("RESULTS: #\(results2.count)")
    }
    catch {
      print("ERROR:", error)
      XCTAssertNil(error)
    }
  }
  
  func testActorAdaptorFetch() throws {
    let adaptor = modelAdaptor!
    let entity = adaptor.model![entity: "actor"]!
    
    let sqlAttrs = entity.attributes.map { $0.columnName ?? $0.name }
                         .joined(separator: ", ")
    
    do {
      let channel = try adaptor.openChannel()
      try channel.querySQL(
        "SELECT \(sqlAttrs) FROM \(entity.externalName ?? entity.name) LIMIT 1",
        entity.attributes
      ) { record in
        print("REC:", record)
      }
    }
    catch {
      print("ERROR:", error)
      XCTAssertNil(error)
    }
  }
  
  func testPicSchemaReflection() throws {
    guard let adaptor  = modelAdaptor, let model = adaptor.model,
          let entity   = model[entity: "staff"],
          let picattr  = entity[attribute: "picture"] else {
      XCTAssert(false, "missing entity")
      return
    }
    XCTAssertNotNil(picattr.valueType)
    guard let vt = picattr.valueType else { return }
    
    XCTAssertEqual(ObjectIdentifier(vt), ObjectIdentifier(Optional<Data>.self))
  }

  func testDataAdaptorFetch() throws {
    let adaptor  = modelAdaptor!
    let entity   = adaptor.model![entity: "staff"]!
    let picattr  = entity[attribute: "picture"]!
    
    let sqlAttrs = picattr.columnName ?? picattr.name
    XCTAssertEqual(ObjectIdentifier(picattr.valueType!),
                   ObjectIdentifier(Optional<Data>.self))

    do {
      let channel = try adaptor.openChannel()
      try channel.querySQL(
        "SELECT \(sqlAttrs) FROM \(entity.externalName ?? entity.name) LIMIT 1",
        [picattr]
      )
      { record in
        print("REC:", record)
      }
    }
    catch {
      print("ERROR:", error)
      XCTAssertNil(error)
    }
  }
  func testDataDBFetch() throws {
    let db     = database
    let entity = db.model![entity: "staff"]!
    let ds     = ActiveDataSource<ActiveRecord>(database: db, entity: entity)
    
    
    do {
      let objects = try ds.fetchObjects()
      print("objects:", objects)
      XCTAssert(objects.count >= 2)
    }
    catch {
      print("ERROR:", error)
      XCTAssertNil(error)
    }
  }
}
