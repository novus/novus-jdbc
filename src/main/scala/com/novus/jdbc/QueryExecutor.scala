package com.novus.jdbc

import java.sql.{ ResultSet, Connection }
import org.slf4j.LoggerFactory

/**
 * A simple mechanism for executing queries on a JDBC data source and transforming
 *  ResultSet instances into arbitrary types. Implementations must provide an
 *  execution strategy by implementing #managed.
 *
 *  Warning: Does not work properly with objects that can only be traversed once.
 */
trait QueryExecutor[DBType] {
  val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Execute some function requiring a connection, performing whatever management
   *  is necessary (eg ARM / loaner).
   */
  protected def managed[A](f: Connection => A): A

  final protected def execute[T](q: String, params: Any*)(f: Connection => T): T ={
    val msg = """
      QUERY:  %s
      PARAMS: %s
    """ format (q, params.mkString(", "))

    val now = System.currentTimeMillis
    val output = managed(f)
    val later = System.currentTimeMillis

    log.info("Timed: %s timed for %s ms", msg, later - now)

    output
  }

  /** Execute a query and transform only the head of the ResultSet. */
  final def selectOne[T](q: String, params: Any*)(f: ResultSet => T)(implicit query: Queryable[DBType]): Option[T] = {
    val rs = execute(q, params: _*) { con => query.execute(con, q, params: _*) }

    val out = if (rs.next()) {
      Some(f(rs))
    }
    else {
      None
    }
    rs.close()

    out
  }

  /**
   * Execute a query and yield an Iterator[T] which, as consumed, will progress through the ResultSet and lazily
   * transform each member.
   */
  final def select[T](q: String, params: Any*)(f: ResultSet => T)(implicit query: Queryable[DBType]): Iterator[T] = {
    val rs = execute(q, params: _*) { con => query.execute(con, q, params: _*) }

    new ResultSetIterator(rs, f)
  }

  /** Returns an iterator containing the ID column which was inserted. */
  final def insert(q: String, params: Any*)(implicit query: Queryable[DBType]): Iterator[Int] = {
    execute(q, params: _*) { con => query.insert(con, q, params: _*) }
  }

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   */
  final def update(q: String, params: Any*)(implicit query: Queryable[DBType]): Int = {
    execute(q, params: _*) { con => query.update(con, q, params: _*) }
  }

  /** Returns the row count deleted by this SQL statement. */
  final def delete(q: String, params: Any*)(implicit query: Queryable[DBType]): Int = {
    execute(q, params: _*) { con => query.delete(con, q, params: _*) }
  }

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown()
}