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
package com.novus.jdbc.c3p0

import com.novus.jdbc.{QueryExecutor, Queryable}
import java.sql.Connection
import com.mchange.v2.c3p0.ComboPooledDataSource

/**
 * Implementation of the QueryExecutor using the C3P0 connection pool as the backing SQL connection pool. This class
 * does not handle exceptions other than to log that they happened. Regardless of the outcome of a query, returns the
 * connection back to the connection pool.
 *
 * @param pool a reference to a C3P0 connection pool.
 */
class C3P0QueryExecutor[DBType : Queryable](pool: ComboPooledDataSource) extends QueryExecutor[DBType]{

  /** Obtain a connection from the underlying connection pool */
  protected def connection(): Connection = pool.getConnection

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown() {
    pool close ()
  }

  override def toString = "C3P0QueryExecutor: " + pool.getDataSourceName
}

object C3P0QueryExecutor{

  @deprecated("For time being, please use a config file until something else can be figured out")
  def apply[DBType : Queryable](driver: String,
                                uri: String,
                                user: String,
                                password: String,
                                maxIdle: Int,
                                minPoolSize: Int,
                                maxPoolSize: Int): C3P0QueryExecutor[DBType] ={
    val pool = new ComboPooledDataSource()
    pool setDriverClass driver
    pool setJdbcUrl uri
    pool setUser user
    pool setPassword password
    pool setMaxIdleTime maxIdle
    pool setInitialPoolSize minPoolSize
    pool setMinPoolSize minPoolSize
    pool setMaxPoolSize maxPoolSize

    new C3P0QueryExecutor[DBType](pool)
  }

  /**
   * Creates a QueryExecutor with the C3P0 connection pool without explicit parameter passing in the constructor.
   * Pool parameters should be written to either the properties file or the XML configuration file. If these do not
   * exist, the pool will use the hard-coded default parameters in the pool implementation itself.
   *
   * @param driver The driver to be used with the underlying connection pool
   */
  def apply[DBType : Queryable](driver: String, configName: String) ={
    val pool = new ComboPooledDataSource(configName)

    Class forName driver
    new C3P0QueryExecutor[DBType](pool)
  }
}