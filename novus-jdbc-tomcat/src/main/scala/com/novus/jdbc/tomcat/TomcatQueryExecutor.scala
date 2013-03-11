package com.novus.jdbc.tomcat

import com.novus.jdbc.{QueryExecutor, Queryable}
import java.sql.Connection
import org.apache.tomcat.jdbc.pool.{DataSource, DataSourceProxy, PoolProperties}

class TomcatQueryExecutor[DBType : Queryable](pool: DataSourceProxy) extends QueryExecutor[DBType]{

  /**
   * Execute some function requiring a connection, performing whatever management is necessary (eg ARM / loaner).
   */
  protected def managed[A](f: (Connection) => A): A ={
    val connection = pool.getConnection
    try{
      f(connection)
    }
    catch{
      case ex: Exception => log error ("%s, threw exception: %s" format(this, ex.getMessage)); throw ex
    }
    finally{
      if (connection != null) connection.close()
    }
  }

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown() {
    pool close true
  }

  override def toString = "TomcatQueryExecutor: %s" format (pool getName)
}

object TomcatQueryExecutor{
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