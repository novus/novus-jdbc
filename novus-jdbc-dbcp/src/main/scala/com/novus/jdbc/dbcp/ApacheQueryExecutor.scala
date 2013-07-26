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
package com.novus.jdbc.dbcp

import com.novus.jdbc.{Queryable, QueryExecutor}
import org.apache.commons.dbcp.{PoolableConnectionFactory, DriverManagerConnectionFactory, PoolingDataSource}
import java.sql.Connection
import org.apache.commons.pool.impl.{GenericObjectPool, GenericKeyedObjectPoolFactory, StackObjectPool}
import org.apache.commons.pool.BaseObjectPool

/**
 * Implementation of the QueryExecutor using the C3P0 connection pool as the backing SQL connection pool. This class
 * does not handle exceptions other than to log that they happened. Regardless of the outcome of a query, returns the
 * connection back to the connection pool.
 *
 * @param pool The Apache pooled data source
 * @param underlying The underlying pool backing the pooled data source
 * @param name The name of the pool (appears in the logging statements)
 */
class ApacheQueryExecutor[DBType : Queryable](pool: PoolingDataSource, underlying: BaseObjectPool, name: String)
    extends QueryExecutor[DBType]{

  /** Obtain a connection from the underlying connection pool */
  protected def connection(): Connection = pool.getConnection

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown() {
    underlying close ()
  }

  override def toString = "ApacheQueryExecutor: " + name
}

object ApacheQueryExecutor{

  def apply[DBType: Queryable](driver: String,
                               uri: String,
                               user: String,
                               password: String,
                               name: String,
                               initCapacity: Int,
                               maxIdleConnections: Int,
                               poolStatements: Boolean): ApacheQueryExecutor[DBType] ={
    val connectionFactory = new DriverManagerConnectionFactory(uri, user, password)
    val connectionPool = new StackObjectPool(maxIdleConnections, initCapacity)
    val statementPool = if(poolStatements) new GenericKeyedObjectPoolFactory(null) else null

    //wrap the connection pool with a pooling connection factory and statement pool with a connection validation pulse.
    new PoolableConnectionFactory(connectionFactory, connectionPool, statementPool, "SELECT 1", false, true)

    val pool = new PoolingDataSource(connectionPool)
    pool setAccessToUnderlyingConnectionAllowed true

    Class forName driver
    new ApacheQueryExecutor[DBType](pool, connectionPool, name)
  }

  def apply[DBType: Queryable](driver: String,
                               uri: String,
                               user: String,
                               password: String,
                               name: String,
                               config: GenericObjectPool.Config,
                               poolStatements: Boolean): ApacheQueryExecutor[DBType] ={
    val connectionFactory = new DriverManagerConnectionFactory(uri, user, password)
    val connectionPool = new GenericObjectPool(null, config)
    val statementPool = if(poolStatements) new GenericKeyedObjectPoolFactory(null) else null

    new PoolableConnectionFactory(connectionFactory, connectionPool, statementPool, "SELECT 1", false, true)

    val pool = new PoolingDataSource(connectionPool)
    pool setAccessToUnderlyingConnectionAllowed true

    Class forName driver
    new ApacheQueryExecutor[DBType](pool, connectionPool, name)
  }
}