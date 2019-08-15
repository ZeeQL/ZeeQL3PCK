//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017-2019 ZeeZide GmbH. All rights reserved.
//

import struct Foundation.URL
import ZeeQL
import PostgresClientKit

open class PostgreSQLAdaptor : Adaptor, SmartDescription {
  // TODO: Pool. We really need one for PG. (well, not for ApacheExpress which
  //       should use mod_dbd)
  
  public enum Error : Swift.Error {
    case Generic
    case NotImplemented
    case CouldNotConnect(String)
    case Wrapped(Swift.Error)
  }
  
  /// The connectString the adaptor was configured with.
  internal let connectionConfiguration : ConnectionConfiguration
  
  /**
   * Configure the adaptor with the given connect string.
   *
   * A connect string can be a URL like:
   *
   *     postgresql://OGo:OGo@localhost:5432/OGo
   *
   * or a space separated list of parameters, like:
   *
   *     host=localhost port=5432 dbname=OGo user=OGo
   *
   * Common parameters
   *
   * - host
   * - port
   * - dbname
   * - user
   * - password
   *
   * The full capabilities are listed in the
   * [PostgreSQL docs](https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING).
   *
   * Note: The init doesn't validate the connect string, if it is malformed,
   *       channel creation will fail.
   */
  public convenience init(_ connectString: String) {
    if let url = URL(string: connectString) {
      let db : String? = {
        guard !url.path.isEmpty else { return nil }
        if !url.path.hasPrefix("/") { return url.path }
        let pp = url.path.dropFirst()
        guard !pp.isEmpty else { return nil }
        return String(pp)
      }()
      
      let useSSL = url.scheme == "postgresqls" || url.scheme == "pgs"
      
      self.init(host: url.host, port: url.port, database: db,
                user: url.user, password: url.password ?? "",
                useSSL: useSSL)
    }
    else {
      self.init()
    }
  }
  
  /**
   * Configure the adaptor with the given values.
   *
   * Example:
   *
   *     let adaptor = PostgreSQLAdaptor(database: "OGo", 
   *                                     user: "OGo", password: "OGo")
   */
  public init(host: String? = nil, port: Int? = nil,
              database: String? = nil,
              user: String? = nil, password: String = "",
              useSSL: Bool = false)
  {
    var config = ConnectionConfiguration()
    config.host       = host     ?? "127.0.0.1"
    config.port       = port     ?? 5432
    config.database   = database ?? "postgres"
    config.user       = user     ?? "postgres"
    config.credential = password.isEmpty
                      ? .trust : .md5Password(password: password)
    config.ssl        = useSSL
    self.connectionConfiguration = config
  }
  
  
  // MARK: - Support
  
  public var expressionFactory : SQLExpressionFactory
                               = PostgreSQLExpressionFactory.shared
  public var model             : Model? = nil
  
  
  // MARK: - Channels

  open func openChannel() throws -> AdaptorChannel {
    // TODO: better errors
    do {
      let handle = try PostgresClientKit.Connection(
                         configuration: connectionConfiguration)
      return PostgreSQLAdaptorChannel(adaptor: self, handle: handle)
    }
    catch {
      throw AdaptorError.CouldNotOpenChannel(Error.Wrapped(error))
    }
  }
  
  public func releaseChannel(_ channel: AdaptorChannel) {
    // not maintaing a pool
  }
  
  
  // MARK: - Model
  
  public func fetchModel() throws -> Model {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try PostgreSQLModelFetch(channel: channel).fetchModel()
  }
  public func fetchModelTag() throws -> ModelTag {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try PostgreSQLModelFetch(channel: channel).fetchModelTag()
  }
  
  
  // MARK: - Description

  public func appendToDescription(_ ms: inout String) {
    ms += " \(connectionConfiguration)"
    if model != nil {
      ms += " has-model"
    }
  }
}
