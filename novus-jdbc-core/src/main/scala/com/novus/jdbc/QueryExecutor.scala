package com.novus.jdbc

import java.sql.Connection
import org.slf4j.LoggerFactory

/**
 * The container object for an underlying database connection pool. All queries are both timed and logged to an
 * [[org.slf4j.Logger]] which must be configured to the actual logging framework. It is assumed, although not enforced
 * at compile time, that all statements are DML queries. Implementations of `QueryExecutor` must define the #connection
 * and the #shutdown methods.
 *
 * @since 0.1
 * @tparam DBType The database type
 *
 * @note Warning: Does not work properly with parameter objects that can only be traversed once.
 */
trait QueryExecutor[DBType] {
  val log = LoggerFactory getLogger this.getClass

  /** Obtain a connection from the underlying connection pool */
  protected def connection(): Connection

  /**
   * Responsible for obtaining and returning a DB connection from the connection pool to execute the given query
   * function.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f Any one of the select, update, delete or merge commands
   * @tparam T The return type of the query
   */
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
      case ex: Exception            => log error ("{}, threw exception" format this, ex); throw ex
    }
    finally {
      if (con != null) con close ()
    }
  }

  /** Returns an iterator containing update counts. */
  final def executeBatch[I <: Seq[Any]](batchSize: Int = 1000)(q: String, params: Iterator[I])(implicit query: Queryable[DBType]): Iterator[Int] =
    execute(q, params) { query.executeBatch(batchSize)(q, params) }

  /**
   * Execute a query and transform only the head of the `RichResultSet`. If this query would produce multiple results,
   * they are lost.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The returned type from the query
   */
  final def selectOne[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] = {
    val (stmt, rs) = execute(q, params: _*) { query execute (q, params: _*) }

    try{
      if (rs next ()) {
        Some(f(rs))
      }
      else {
        None
      }
    }
    finally{
      stmt close ()
    }
  }

  /**
   * Execute a query and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress through the
   * underlying `RichResultSet` and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The returned type from the query
   */
  final def select[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] = {
    val (stmt, rs) = execute(q, params: _*) { query execute (q, params: _*) }

    new ResultSetIterator(stmt, rs, f)
  }

  /**
   * Eagerly evaluates the argument function against the returned `RichResultSet`.
   *
   * @see #select
   */
  final def eagerlySelect[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): List[T] =
    select(q, params: _*)(f)(query).toList

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def insert(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int] =
    execute(q, params: _*) { query insert (q, params: _*) }

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def update(q: String, params: Any*)(implicit query: Queryable[DBType]): Int =
    execute(q, params: _*) { query update (q, params: _*) }

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def delete(q: String, params: Any*)(implicit query: Queryable[DBType]): Int =
    execute(q, params: _*) { query delete (q, params: _*) }

  /**
   * Returns an iterator containing the ID column which was inserted as a result of the merge statement. If this merge
   * statement does not cause an insertion into a table generating new IDs, the iterator returns empty. It is suggested
   * that update be used in the case where row counts affected is preferable to IDs.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def merge(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int] =
    execute(q, params: _*) { query merge (q, params: _*) }

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown()
}