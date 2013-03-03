package com.novus.jdbc.bonecp

import com.jolbox.bonecp.ConnectionHandle
import com.jolbox.bonecp.hooks.AbstractConnectionHook
import java.sql.Statement
import java.util.{ Map => JavaMap }
import org.slf4j.LoggerFactory

/**
 * Giving Tarik a way to log damned near everything about SQL Server as seen from the server/client boxes. Please see
 * the documentation on implementing more of the AbstractConnectionHook's functionality here:
 *
 * http://jolbox.com/bonecp/downloads/site/apidocs/com/jolbox/bonecp/hooks/AbstractConnectionHook.html
 *
 * Suggested that we don't do more than we are doing here.
 */
class DebonedLoggingHook extends AbstractConnectionHook {
  val log = LoggerFactory.getLogger(getClass)

  override def onAcquire(handle: ConnectionHandle) {
    log.debug("SQL Server: <Pool {}> Creating connection", poolName(handle))
  }

  override def onCheckIn(handle: ConnectionHandle) {
    log.debug("SQL Server: <Pool {}> Returning connection to pool", poolName(handle))
  }

  override def onCheckOut(handle: ConnectionHandle) {
    log.debug("SQL Server: <Pool {}> Obtaining connection from pool", poolName(handle))
  }

  override def onDestroy(handle: ConnectionHandle) {
    if (!handle.isPossiblyBroken) {
      log.debug("SQL Server: <Pool {}> Destroying connection", poolName(handle))
    }
  }

  override def onQueryExecuteTimeLimitExceeded(handle: ConnectionHandle,
                                               statement: Statement,
                                               sql: String,
                                               params: JavaMap[Object, Object],
                                               timeElapsedInNs: Long) {
    log.warn("SQL Server: <Pool %s> query time limit exceeded; %s; using %s; in %s ns" format(
      poolName(handle), sql, params, timeElapsedInNs))
  }

  override def onConnectionException(handle: ConnectionHandle, state: String, throwable: Throwable) = {
    connectionState(state, poolName(handle))

    true
  }

  /**
   * Error messages taken directly from the JavaDocs of BoneCP.
   */
  protected def connectionState(state: String, pool: String) {
    state match {
      case "08001" => log.error("SQL Server: <Pool {}> The application requester is unable to establish the connection", pool)
      case "08002" => log.error("SQL Server: <Pool {}> The connection already exists", pool)
      case "08003" => log.error("SQL Server: <Pool {}> The connection does not exist", pool)
      case "08004" => log.error("SQL Server: <Pool {}> The application server rejected establishment of the connection", pool)
      case "08007" => log.error("SQL Server: <Pool {}> Transaction resolution unknown", pool)
      case "08502" => log.error("SQL Server: <Pool {}> The CONNECT statement issued by an application process running with a SYNCPOINT of TWOPHASE has failed, because no transaction manager is available", pool)
      case "08504" => log.error("SQL Server: <Pool {}> An error was encountered while processing the specified path rename configuration file", pool)
      case "57P01" => log.error("SQL Server: <Pool {}> The database is broken/died", pool)
      case _       => log.error("SQL Server: <Pool %s> returned error code %s" format (pool, state))
    }
  }

  protected def poolName(handle: ConnectionHandle): String = handle.getPool.getConfig.getPoolName
}