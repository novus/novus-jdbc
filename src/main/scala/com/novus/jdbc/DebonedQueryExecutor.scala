package com.novus.jdbc

import com.jolbox.bonecp.{ BoneCP, BoneCPConfig }
import java.sql.Connection
import org.slf4j.LoggerFactory

/**
 * Implementation of the QueryExecutor using the BoneCP connection pool as the backing SQL connection pool. This class
 * does not handle exceptions other than to log that they happened. Regardless of the outcome of a query, returns the
 * connection back to the connection pool.
 *
 * @param pool a reference to a BoneCP connection pool.
 */
class DebonedQueryExecutor[DBType](pool: BoneCP) extends QueryExecutor[DBType] {

  val log = LoggerFactory.getLogger(getClass)

  /**
   * Obtain a connection from the BoneCP connection pool object and require that it be returned back to the pool after
   * attempted query.
   */
  protected def managed[A](f: Connection => A): A = {
    val connection = pool.getConnection
    try {
      f(connection)
    }
    catch {
      case ex: NullPointerException => log.error("%s, boneCP pool object returned a null connection", this); throw ex
      case ex: Exception            => log.error("%s, threw exception: %s", this, ex.getMessage); throw ex
    }
    finally {
      if (connection != null) connection.close()
    }
  }

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

  private val hook = new DebonedLoggingHook

  def apply[DBType](driver: String,
                    uri: String,
                    user: String,
                    password: String,
                    name: String,
                    minConnections: Int,
                    maxConnections: Int,
                    idlePeriodMinutes: Int,
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

    Class.forName(driver)
    new DebonedQueryExecutor[DBType](new BoneCP(config))
  }
}