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
  @inline final protected def execute[T](q: String, params: Any*)(f: Connection => T): T ={
    val msg = """
      QUERY:  %s
      PARAMS: %s
    """ format (q, params mkString (", "))

    executeQuery(msg, f)
  }

  /**
   * Responsible for obtaining and returning a DB connection from the connection pool to execute the given query
   * function.
   *
   * @param q The query statement
   * @param f Any one of the select, update, delete or merge commands
   * @tparam T The return type of the query
   */
  @inline final protected def execute[T](q: String)(f: Connection => T): T ={
    val msg = """
      QUERY:  %s
    """ format q

    executeQuery(msg, f)
  }

  /**
   * Responsible for obtaining and returning a DB connection from the connection pool to execute the given query
   * function.
   *
   * @param msg The timing message
   * @param f Any one of the select, update, delete or merge commands
   * @tparam T The return type of the query
   */
  final private def executeQuery[T](msg: String, f: Connection => T): T = {
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
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  final def selectOne[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] = {
    val res = execute(q, params: _*) { query select (f, q, params: _*) }

    one(res)
  }

  /**
   * Execute a query and transform only the head of the [[com.novus.jdbc.RichResultSet]]. If this query would produce
   * multiple results, they are lost.
   *
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  final def selectOne[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] ={
    val res = execute(q){ query select (f, q) }

    one(res)
  }

  /**
   * Given the products of an executed query, transforms only the first result using the supplied function `f`.
   *
   * @param res The [[com.novus.jdbc.CloseableIterator]] produced from the query
   * @tparam T The return type from the query
   */
  final private def one[T](res: CloseableIterator[T]): Option[T] = try {
    if (res.hasNext) Some(res next ()) else None
  }
  finally{
    res close ()
  }

  /**
   * Execute a query and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress through the
   * underlying `RichResultSet` and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  final def select[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q, params: _*) { query select (f, q, params: _*) }

  /**
   * Execute a query and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress through the
   * underlying `RichResultSet` and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The returned type from the query
   */
  final def select[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q) { query select (f, q) }

  /**
   * Eagerly evaluates the argument function against the returned [[com.novus.jdbc.RichResultSet]].
   *
   * @see #select
   */
  final def eagerlySelect[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): List[T] =
    select(q, params: _*)(f)(query).toList

  /**
   * Eagerly evaluates the argument function against the returned `RichResultSet`.
   *
   * @see #select
   */
  final def eagerlySelect[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): List[T] =
    select(q)(f)(query).toList

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def insert(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int] =
    execute(q, params: _*) { query insert (q, params: _*) }

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  final def insert[T](columns: Array[Int], q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q, params: _*){ query insert (columns, f, q, params: _*) }

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The return type of the query
   */
  final def insert[T](columns: Array[String], q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q, params: _*){ query insert (columns, f, q, params: _*) }

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   */
  final def insert(q: String)(implicit query: Queryable[DBType]): CloseableIterator[Int] = execute(q) { query insert q }

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The return type of the query
   */
  final def insert[T](columns: Array[Int], q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q){ query insert (columns, f, q) }

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The return type of the query
   */
  final def insert[T](columns: Array[String], q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q){ query insert (columns, f, q) }

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
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   *
   * @param q The query statement
   */
  final def update(q: String)(implicit query: Queryable[DBType]): Int = execute(q) { query update q }

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def delete(q: String, params: Any*)(implicit query: Queryable[DBType]): Int =
    execute(q, params: _*) { query delete (q, params: _*) }

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   */
  final def delete(q: String)(implicit query: Queryable[DBType]): Int = execute(q) { query delete q }

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

  /**
   * Returns an iterator containing the ID column which was inserted as a result of the merge statement. If this merge
   * statement does not cause an insertion into a table generating new IDs, the iterator returns empty. It is suggested
   * that update be used in the case where row counts affected is preferable to IDs.
   *
   * @param q The query statement
   */
  final def merge(q: String)(implicit query: Queryable[DBType]): CloseableIterator[Int] = execute(q) { query merge q }

  final def proc[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q){ query proc (f, q) }

  final def proc[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    execute(q, params: _*){ query proc (f, q, params: _*) }

  final def proc[T](out: Array[Int], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    execute(q) { query proc (out, f, q) }

  final def proc[T](out: Array[String], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    execute(q) { query proc (out, f, q) }

  final def proc[T](out: Array[Int], q: String, params: Any*)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    execute(q, params: _*) { query proc (out, f, q, params: _*) }

  final def proc[T](out: Array[String], q: String, params: Any*)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    execute(q, params: _*) { query proc (out, f, q, params: _*) }

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown()
}