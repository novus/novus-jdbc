package com.novus.jdbc

import java.sql.Connection
import org.slf4j.LoggerFactory

/**
 * A simple mechanism for executing queries on a JDBC data source and transforming
 *  ResultSet instances into arbitrary types. Implementations must provide an
 *  execution strategy by implementing #connection.
 *
 *  Warning: Does not work properly with objects that can only be traversed once.
 */
trait QueryExecutor[DBType] {
  val log = LoggerFactory.getLogger(this.getClass)

  /** Obtain a connection from the underlying connection pool */
  protected def connection(): Connection

  final protected def execute[T](q: String, params: Any*)(f: Connection => T): T ={
    val msg = """
      QUERY:  %s
      PARAMS: %s
    """ format (q, params mkString (", "))

    val now = System.currentTimeMillis
    val con = connection()
    try {
      val output = f(con)

      val later = System.currentTimeMillis

      log info ("Timed: {} timed for {} ms", msg, later - now)

      output
    }
    catch {
      case ex: NullPointerException => log error ("{} pool object returned a null connection", this); throw ex
      case ex: Exception            => log error ("%s, threw exception: %s" format(this, ex.getMessage)); throw ex
    }
    finally {
      if (con != null) con close ()
    }
  }

  /** Returns an iterator containing update counts. */
  final def executeBatch[I <: Seq[Any]](batchSize: Int = 1000)(q: String, params: Iterator[I])(implicit query: Queryable[DBType]): Iterator[Int] =
    execute(q, params) { query.executeBatch(batchSize)(q, params) }

  /** Execute a query and transform only the head of the ResultSet. */
  final def selectOne[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType], wrapper: ResultSetWrapper[DBType]): Option[T] = {
    val rs = execute(q, params: _*) { query execute (q, params: _*) }

    val out = if (rs next ()) {
      Some(f(wrapper wrap rs))
    }
    else {
      None
    }
    rs close ()

    out
  }

  /**
   * Execute a query and yield an Iterator[T] which, as consumed, will progress through the ResultSet and lazily
   * transform each member.
   */
  final def select[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType], wrapper: ResultSetWrapper[DBType]): Iterator[T] = {
    val rs = execute(q, params: _*) { query execute (q, params: _*) }

    new ResultSetIterator(wrapper wrap rs, f)
  }

  /** 'Cause sometimes you just want a List of the transformed ResultSet. */
  final def eagerlySelect[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType], wrapper: ResultSetWrapper[DBType]): List[T] =
    select(q, params: _*)(f)(query, wrapper).toList

  /** Returns an iterator containing the ID column which was inserted. */
  final def insert(q: String, params: Any*)(implicit query: Queryable[DBType]): Iterator[Int] =
    execute(q, params: _*) { query insert (q, params: _*) }

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   */
  final def update(q: String, params: Any*)(implicit query: Queryable[DBType]): Int =
    execute(q, params: _*) { query update (q, params: _*) }

  /** Returns the row count deleted by this SQL statement. */
  final def delete(q: String, params: Any*)(implicit query: Queryable[DBType]): Int =
    execute(q, params: _*) { query delete (q, params: _*) }

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown()
}