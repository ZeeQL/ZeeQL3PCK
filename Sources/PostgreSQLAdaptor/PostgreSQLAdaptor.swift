//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017-2019 ZeeZide GmbH. All rights reserved.
//

import Foundation
import struct Foundation.URL
import struct Foundation.URLComponents
import struct Foundation.URLQueryItem
import struct Foundation.Date
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
  
  public var url: URL? {
    let cfg    = connectionConfiguration
    var url    = URLComponents()
    url.scheme = cfg.ssl ? "postgresqls" : "postgresql"
    url.port   = cfg.port
    if !cfg.host.isEmpty { url.host   = cfg.host }
    if !cfg.user.isEmpty { url.user   = cfg.user }
    switch cfg.credential {
      case .trust: break
      case .md5Password      (let password): url.password = password
      case .cleartextPassword(let password): url.password = password
    }
    if !cfg.database.isEmpty {
      url.path = "/" +
        cfg.database
          .addingPercentEncoding(withAllowedCharacters: .urlPathAllowed)!
    }
    return url.url
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
  
  
  // MARK: - Connection pool
  
  private final class SingleConnectionPool {
    
    struct Entry {
      let releaseDate : Date
      let connection  : AdaptorChannel
      
      var age : TimeInterval { return -(releaseDate.timeIntervalSinceNow) }
    }

    let maxAge          : TimeInterval
    let lock            = NSLock()
    let expirationQueue = DispatchQueue(label: "de.zeezide.zeeql.pck.expire")
    var entry           : Entry? // here we just pool one :-)
    var gc              : DispatchWorkItem?
    
    init(maxAge: TimeInterval) {
      self.maxAge = maxAge
    }
    
    func grab() -> AdaptorChannel? {
      lock.lock(); defer { entry = nil; lock.unlock() }
      return entry?.connection
    }
    
    func add(_ channel: AdaptorChannel) {
      lock.lock()
      if entry == nil {
        entry = Entry(releaseDate: Date(), connection: channel)
      }
      lock.unlock()

      expirationQueue.async {
        if self.gc != nil { return }
        self.gc = DispatchWorkItem(block: self.expire)
        self.expirationQueue.asyncAfter(deadline: .now() + .seconds(1),
                                        execute: self.gc!)
      }
    }
    func expire() {
      let rerun : Bool
      lock.lock()
      if let entry = entry {
        rerun = entry.age > maxAge
        if !rerun { self.entry = nil }
      }
      else { rerun = false }
      lock.unlock()
      
      if rerun {
        gc = DispatchWorkItem(block: self.expire)
        expirationQueue.asyncAfter(deadline: .now() + .seconds(1),
                                   execute: gc!)
      }
    }
  }
  private var connectionPool = SingleConnectionPool(maxAge: 10)
  
  open func openChannelFromPool() throws -> AdaptorChannel {
    if let pooled = connectionPool.grab() { return pooled }
    return try openChannel()
  }
  
  open func releaseChannel(_ channel: AdaptorChannel) {
    guard !channel.isTransactionInProgress else {
      globalZeeQLLogger.warn("release a channel w/ an option TX:", channel)
      try? channel.rollback()
      return
    }
    guard let pgChannel = channel as? PostgreSQLAdaptorChannel else {
      globalZeeQLLogger.warn("attempt to release a foreign channel:", channel)
      return
    }
    guard pgChannel.handle != nil else { return } // closed
    
    connectionPool.add(pgChannel)
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
