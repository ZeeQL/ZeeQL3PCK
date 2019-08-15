//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017-2019 ZeeZide GmbH. All rights reserved.
//

#if os(Linux)
  import func Glibc.atol
  import func Glibc.atof
#else
  import func Darwin.atol
  import func Darwin.atof
#endif
import struct Foundation.Data
import ZeeQL
import PostgresClientKit


open class PostgreSQLAdaptorChannel : AdaptorChannel, SmartDescription {

  public enum Error : Swift.Error {
    case prepare(Swift.Error, sql: String)
    case execute(Swift.Error, sql: String)
    case missingResultSchema
    case unsupportedValueType(AttributeValue.Type)
    case conversionError(Any.Type, Any)
    case connectionClosed
    case unexpectedNull
  }

  public let expressionFactory : SQLExpressionFactory
  public var handle : PostgresClientKit.Connection?
  final  let logSQL  = true
  
  init(adaptor: Adaptor, handle: PostgresClientKit.Connection) {
    self.expressionFactory = adaptor.expressionFactory
    self.handle = handle
  }
  
  deinit {
    if let handle = handle {
      handle.close()
    }
  }
  
  func close() {
    guard let handle = handle else { return }
    handle.close()
    self.handle = nil
  }
  
  
  // MARK: - Raw Queries
  
  /**
   * Iterate over the raw result set and produce `AdaptorRecord`s.
   */
  func fetchRows(_ cursor   : Cursor,
                 _ optAttrs : [ Attribute ]? = nil,
                 cb         : ( AdaptorRecord ) throws -> Void) throws
  {
    var schema : AdaptorRecordSchema? = {
      guard let attrs = optAttrs else { return nil }
      return AdaptorRecordSchemaWithAttributes(attrs)
    }()
    
    // PostgresClientKit doesn't currently provide result schema information,
    // not even the counts!
    // https://github.com/codewinsdotcom/PostgresClientKit/issues/12
    
    var count    = 0
    var colCount = 0
    
    for result in cursor {
      let row = try result.get()
      
      // setup schema info on first result item
      if count == 0 {
        colCount = row.columns.count
        if schema == nil {
          // stupid default schema (col[0], col[1], col[2])
          let names = (0..<colCount).map({"col[\($0)]"})
          schema = AdaptorRecordSchemaWithNames(names)
        }
      }
      assert(schema != nil, "missing schema!")
      guard let schema = schema else { throw Error.missingResultSchema }
      
      var values = [ Any? ]()
      values.reserveCapacity(colCount)
      
      count += 1
      
      for col in 0..<colCount {
        let attr : Attribute? = {
          guard let attrs = optAttrs, col < attrs.count else { return nil }
          return attrs[col]
        }()
        
        let pgValue = row.columns[col]
        
        // PostgresClientKit also lacks value reflection
        if let attrType = attr?.valueType {
          guard let pckType = attrType as? PCKAttributeValue.Type else {
            throw Error.unsupportedValueType(attrType)
          }

          values.append(try pckType.pckValue(pgValue))
        }
        else {
          values.append(try pgValue.optionalString())
        }
      }
      
      let record = AdaptorRecord(schema: schema, values: values)
      try cb(record)
    }
  }
  
  
  private func _runSQL(sql: String, optAttrs : [ Attribute ]?,
                       bindings: [ SQLExpression.BindVariable ]?,
                       cb: ( AdaptorRecord ) throws -> Void) throws
               -> Int?
  {
    guard let handle = handle else { throw Error.connectionClosed }
    
    if logSQL { print("SQL: \(sql)") }
    
    let statement : Statement
    do {
      statement = try handle.prepareStatement(text: sql)
    }
    catch {
      throw Error.prepare(error, sql: sql)
    }
    defer { statement.close() }

    let cursor : Cursor
    do {
      if let bindings = bindings, !bindings.isEmpty {
        let parameters = bindings.map {
          $0.value as? PostgresValueConvertible
        }
        cursor = try statement.execute(parameterValues: parameters)
      }
      else {
        cursor = try statement.execute()
      }
    }
    catch {
      throw Error.execute(error, sql: sql)
    }
    defer { cursor.close() }

    try fetchRows(cursor, optAttrs, cb: cb)
    
    return cursor.rowCount
  }
  
  public func querySQL(_ sql: String, _ optAttrs : [ Attribute ]?,
                         cb: ( AdaptorRecord ) throws -> Void) throws
  {
    _ = try _runSQL(sql: sql, optAttrs: optAttrs, bindings: nil, cb: cb)
  }
  
  @discardableResult
  public func performSQL(_ sql: String) throws -> Int {
    // Hm, funny. If we make 'cb' optional, it becomes escaping. So avoid that.
    return try _runSQL(sql: sql, optAttrs: nil, bindings: nil) { rec in } ?? 0
  }
  
  
  // MARK: - Model Queries
  
  public func evaluateQueryExpression(_ sqlexpr  : SQLExpression,
                                      _ optAttrs : [ Attribute ]?,
                                      result: ( AdaptorRecord ) throws -> Void)
                throws
  {
    _ = try _runSQL(sql: sqlexpr.statement, optAttrs: optAttrs,
                    bindings: sqlexpr.bindVariables, cb: result)
  }

  public func evaluateUpdateExpression(_ sqlexpr: SQLExpression) throws -> Int {
    return try _runSQL(sql: sqlexpr.statement, optAttrs: nil,
                       bindings: sqlexpr.bindVariables) { rec in } ?? 0
  }
  
  
  // MARK: - Transactions
  
  public var isTransactionInProgress : Bool = false
  
  public func begin() throws {
    guard let handle = handle else { throw Error.connectionClosed }
    guard !isTransactionInProgress
     else { throw AdaptorChannelError.TransactionInProgress }
    
    try handle.beginTransaction()
    isTransactionInProgress = true
  }
  public func commit() throws {
    isTransactionInProgress = false
    guard let handle = handle else { throw Error.connectionClosed }
    try handle.commitTransaction()
  }
  public func rollback() throws {
    isTransactionInProgress = false
    guard let handle = handle else { throw Error.connectionClosed }
    try handle.rollbackTransaction()
  }
  
  
  // MARK: - Description
  
  public func appendToDescription(_ ms: inout String) {
    if let handle = handle {
      ms += " \(handle)"
    }
    else {
      ms += " finished"
    }
  }
  

  // MARK: - reflection
  
  public func describeSequenceNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeSequenceNames()
  }
  
  public func describeDatabaseNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeDatabaseNames()
  }
  public func describeTableNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeTableNames()
  }

  public func describeEntityWithTableName(_ table: String) throws -> Entity? {
    return try PostgreSQLModelFetch(channel: self)
                 .describeEntityWithTableName(table)
  }

  
  // MARK: - Insert w/ auto-increment support
  
  open func insertRow(_ row: AdaptorRow, _ entity: Entity?, refetchAll: Bool)
              throws -> AdaptorRow
  {
    let attributes : [ Attribute ]? = {
      guard let entity = entity else { return nil }
      
      if refetchAll { return entity.attributes }
      
      // TBD: refetch-all if no pkeys are assigned
      guard let pkeys = entity.primaryKeyAttributeNames, !pkeys.isEmpty
       else { return entity.attributes }
      
      return entity.attributesWithNames(pkeys)
    }()
    
    let expr = PostgreSQLExpression(entity: entity)
    expr.prepareInsertReturningExpressionWithRow(row, attributes: attributes)
    
    var rec : AdaptorRecord? = nil
    try evaluateQueryExpression(expr, attributes) { record in
      guard rec == nil else { // multiple matched!
        throw AdaptorError.FailedToRefetchInsertedRow(
                             entity: entity, row: row)
      }
      rec = record
    }
    guard let rrec = rec else { // no record returned?
      throw AdaptorError.FailedToRefetchInsertedRow(entity: entity, row: row)
    }
    
    return rrec.asAdaptorRow
  }
}
