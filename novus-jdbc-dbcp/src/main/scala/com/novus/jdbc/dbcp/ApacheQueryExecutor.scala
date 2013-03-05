package com.novus.jdbc.dbcp

import com.novus.jdbc.{Queryable, QueryExecutor}
import org.apache.commons.dbcp.{PoolableConnectionFactory, DriverManagerConnectionFactory, PoolingDataSource}
import java.sql.Connection
import org.apache.commons.pool.impl.{GenericKeyedObjectPoolFactory, StackObjectPool}
import org.apache.commons.pool.BaseObjectPool

/**
 *
 * @param pool
 * @param underlying
 * @param name
 */
class ApacheQueryExecutor[DBType : Queryable](pool: PoolingDataSource, underlying: BaseObjectPool, name: String)
    extends QueryExecutor[DBType]{

  /** Execute some function requiring a connection, performing whatever management is necessary (eg ARM / loaner). */
  protected def managed[A](f: Connection => A): A ={
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
                               poolStatements: Boolean = true): ApacheQueryExecutor[DBType] ={
    val connectionFactory = new DriverManagerConnectionFactory(uri, user, password)
    val connectionPool = new StackObjectPool(maxIdleConnections, initCapacity)
    val statementPool = if(poolStatements) new GenericKeyedObjectPoolFactory(null) else null

    //wrap the connection pool with a pooling connection factory and statement pool with a connection validation pulse.
    new PoolableConnectionFactory(connectionFactory, connectionPool, statementPool, "SELECT 1", false, true)
    val pool = new PoolingDataSource(connectionPool)

    pool.setAccessToUnderlyingConnectionAllowed(true)

    Class.forName(driver)
    new ApacheQueryExecutor[DBType](pool, connectionPool, name)
  }
}