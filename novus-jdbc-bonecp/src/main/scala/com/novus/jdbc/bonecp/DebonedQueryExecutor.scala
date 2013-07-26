/*
 * Copyright (c) 2013 Novus Partners, Inc. (http://www.novus.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.novus.jdbc.bonecp

import com.jolbox.bonecp.{ BoneCP, BoneCPConfig }
import java.sql.Connection
import com.novus.jdbc.{Queryable, QueryExecutor}

/**
 * Implementation of the QueryExecutor using the BoneCP connection pool as the backing SQL connection pool. This class
 * does not handle exceptions other than to log that they happened. Regardless of the outcome of a query, returns the
 * connection back to the connection pool.
 *
 * @param pool a reference to a BoneCP connection pool.
 */
class DebonedQueryExecutor[DBType : Queryable](pool: BoneCP) extends QueryExecutor[DBType] {

  /** Obtain a connection from the underlying connection pool */
  protected def connection(): Connection = pool.getConnection

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown() {
    pool.shutdown()
  }

  /**
   * Fallback mechanism. The finalizer thread is not guaranteed to call this method. It is hoped that developers using
   * this class are intelligent enough to call shutdown before allowing this executor to be garbage collected.
   */
  override def finalize() {
    shutdown()
  }

  override def toString = "DebonedQueryEvaluator: " + pool.getConfig.getPoolName
}

//TODO: Before this goes live talk about what parameters we might want the pools to have.
object DebonedQueryExecutor {

  private lazy val hook = new DebonedLoggingHook

  @deprecated("Please use the BoneCPConfig object to create a db connection with BoneCP.")
  def apply[DBType : Queryable](driver: String,
                                uri: String,
                                user: String,
                                password: String,
                                name: String,
                                minConnections: Int,
                                maxConnections: Int,
                                idlePeriodMinutes: Int,
                                statementCacheSize: Int = 0,
                                logStatements: Boolean = true): DebonedQueryExecutor[DBType] = {
    val config = new BoneCPConfig

    config.setUsername(user)
    config.setPassword(password)
    config.setMinConnectionsPerPartition(minConnections)
    config.setMaxConnectionsPerPartition(maxConnections)
    config.setJdbcUrl(uri)
    config.setPoolName(name)
    config.setIdleConnectionTestPeriodInMinutes(idlePeriodMinutes)
    config.setConnectionTestStatement("SELECT 1")
    config.setLogStatementsEnabled(logStatements)
    config.setConnectionHook(hook)
    config.setLazyInit(true)
    config.setStatementsCacheSize(statementCacheSize)

    Class.forName(driver)
    new DebonedQueryExecutor[DBType](new BoneCP(config))
  }

  def apply[DBType : Queryable](config: BoneCPConfig, driver: String): DebonedQueryExecutor[DBType] ={
    Class.forName(driver)

    new DebonedQueryExecutor[DBType](new BoneCP(config))
  }
}