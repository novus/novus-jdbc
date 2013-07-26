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
package com.novus.jdbc.tomcat

import com.novus.jdbc.{QueryExecutor, Queryable}
import java.sql.Connection
import org.apache.tomcat.jdbc.pool.{DataSource, DataSourceProxy, PoolProperties}

class TomcatQueryExecutor[DBType : Queryable](pool: DataSourceProxy) extends QueryExecutor[DBType]{

  /** Obtain a connection from the underlying connection pool */
  protected def connection(): Connection = pool.getConnection

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown() {
    pool close true
  }

  override def toString = "TomcatQueryExecutor: %s" format (pool getName)
}

object TomcatQueryExecutor{

  @deprecated("Please use the PoolProperties object to configure the Tomcat connection pool")
  def apply[DBType: Queryable](driver: String,
                               uri: String,
                               user: String,
                               password: String,
                               name: String,
                               initCapacity: Int,
                               maxIdleConnections: Int,
                               maxConnections: Int): TomcatQueryExecutor[DBType] ={
    val ds = new DataSource()
    ds setUrl uri
    ds setPassword password
    ds setUsername user
    ds setAccessToUnderlyingConnectionAllowed true
    ds setDriverClassName driver
    ds setInitialSize initCapacity
    ds setMaxActive maxConnections
    ds setMinIdle initCapacity
    ds setMaxIdle maxIdleConnections

    new TomcatQueryExecutor[DBType](ds)
  }

  def apply[DBType : Queryable](config: PoolProperties): TomcatQueryExecutor[DBType] ={
    val ds = new DataSource(config)

    new TomcatQueryExecutor[DBType](ds)
  }
}