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

import java.sql.{Savepoint, Connection}

import org.slf4j.LoggerFactory

/**
 * Represents a means of executing a number of queries which are not committed to the database and can at any point be
 * reverted using the #rollback member function.
 *
 * @since 0.9
 * @param con A reference to a database connection
 * @param savePoint A reference to a database save point
 * @param savePoints The list of save points which can be rolled back to, starting with the head
 * @tparam DBType The database type
 */
class SavePoint[DBType](con: Connection, savePoint: Savepoint, savePoints: List[SavePoint[DBType]] = Nil)
    extends StatementExecutor[DBType]{
  self =>
  val log = LoggerFactory getLogger this.getClass

  protected def store[T](f: Connection => T): T = f(con)

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
    try {
      val output = f(con)
      val later = System.currentTimeMillis

      log info ("Timed: {} timed for {} ms", msg, later - now)

      output
    }
    catch {
      case ex: NullPointerException => log error ("{} pool object returned a null connection", this); throw ex
      case ex: Exception            => log error ("%s threw exception with %s" format (this, msg), ex); throw ex
    }
    finally {
      if (con != null) con close ()
    }
  }

  /** Returns an iterator containing update counts. */
  final def executeBatch[I <: Seq[Any]](batchSize: Int = 1000)(q: String, params: Iterator[I])(implicit query: Queryable[DBType]): Iterator[Int] =
    store{ query.executeBatch(batchSize)(q, params) }

  /**
   * Execute a query and transform only the head of the `RichResultSet`. If this query would produce multiple results,
   * they are lost.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  final def selectOne[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] =
    store{ query one (f, q, params: _*) }

  /**
   * Execute a query and transform only the head of the [[com.novus.jdbc.RichResultSet]]. If this query would produce
   * multiple results, they are lost.
   *
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  final def selectOne[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] =
    store{ query one (f, q) }

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
    store{ query select (f, q, params: _*) }

  /**
   * Execute a query and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress through the
   * underlying `RichResultSet` and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The returned type from the query
   */
  final def select[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    store { query select (f, q) }

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
    select(q)(f).toList

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def insert(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int] =
    store{ query insert (q, params: _*) }

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
    store{ query insert (columns, f, q, params: _*) }

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
    store{ query insert (columns, f, q, params: _*) }

  /**
   * Returns an iterator containing the ID column of the rows which were inserted by this insert statement.
   *
   * @param q The query statement
   */
  final def insert(q: String)(implicit query: Queryable[DBType]): CloseableIterator[Int] = store { query insert q }

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param f A transform from a `RichResultSet` to a type `T`
   * @tparam T The return type of the query
   */
  final def insert[T](columns: Array[Int], q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    store{ query insert (columns, f, q) }

  /**
   * Returns an iterator containing the compound index column of the rows which were inserted by this insert statement.
   *
   * @param columns The index of each column which represents the auto generated key
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  final def insert[T](columns: Array[String], q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    store{ query insert (columns, f, q) }

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def update(q: String, params: Any*)(implicit query: Queryable[DBType]): Int = store{ query update (q, params: _*) }

  /**
   * Returns the row count updated by this SQL statement. If the SQL statement is not a row update operation, such as a
   * DDL statement, then a 0 is returned.
   *
   * @param q The query statement
   */
  final def update(q: String)(implicit query: Queryable[DBType]): Int = store { query update q }

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def delete(q: String, params: Any*)(implicit query: Queryable[DBType]): Int = store{ query delete (q, params: _*) }

  /**
   * Returns the row count deleted by this SQL statement.
   *
   * @param q The query statement
   */
  final def delete(q: String)(implicit query: Queryable[DBType]): Int = store { query delete q }

  /**
   * Returns an iterator containing the ID column which was inserted as a result of the merge statement. If this merge
   * statement does not cause an insertion into a table generating new IDs, the iterator returns empty. It is suggested
   * that update be used in the case where row counts affected is preferable to IDs.
   *
   * @param q The query statement
   * @param params The query parameters
   */
  final def merge(q: String, params: Any*)(implicit query: Queryable[DBType]): CloseableIterator[Int] =
    store{ query merge (q, params: _*) }

  /**
   * Returns an iterator containing the ID column which was inserted as a result of the merge statement. If this merge
   * statement does not cause an insertion into a table generating new IDs, the iterator returns empty. It is suggested
   * that update be used in the case where row counts affected is preferable to IDs.
   *
   * @param q The query statement
   */
  final def merge(q: String)(implicit query: Queryable[DBType]): CloseableIterator[Int] = store { query merge q }

  /**
   * Executes arbitrary SQL statements.
   *
   * @param q The query statement
   */
  def exec(q: String)(implicit query: Queryable[DBType]): Unit ={
    execute(q){ query execute q }
  }

  /**
   * Executes arbitrary SQL statements.
   *
   * @param q The query statement.
   * @param params The query parameters
   */
  def exec(q: String, params: Any*)(implicit query: Queryable[DBType]): Unit ={
    execute(q){ query execute (q, params) }
  }

  /**
   * Execute a stored procedure and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress
   * through the underlying [[com.novus.jdbc.RichResultSet]] and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    store{ query proc (f, q) }

  /**
   * Execute a stored procedure and yield a [[com.novus.jdbc.CloseableIterator]] which, as consumed, will progress
   * through the underlying [[com.novus.jdbc.RichResultSet]] and lazily evaluate the argument function.
   *
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): CloseableIterator[T] =
    store{ query proc (f, q, params: _*) }

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](out: Array[Int], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    store { query proc (out, f, q) }

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](out: Array[String], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    store { query proc (out, f, q) }

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](out: Array[Int], q: String, params: Any*)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    store{ query proc (out, f, q, params: _*) }

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param params The query parameters
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](out: Array[String], q: String, params: Any*)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    store{ query proc (out, f, q, params: _*) }

  /**
   * Creates a new `SavePoint` branching off from the parent. This new `SavePoint` will itself rollback to the parent
   * `SavePoint`.
   */
  final def save(): SavePoint[DBType] = new SavePoint[DBType](con, con setSavepoint (), this :: savePoints)

  /**
   * Creates a new `SavePoint` branching off from the parent. This new `SavePoint` will itself rollback to the parent
   * `SavePoint`.
   *
   * @param name Allows the generated `SavePoint` to be named
   */
  final def save(name: String): SavePoint[DBType] = new SavePoint[DBType](con, con setSavepoint name, this :: savePoints){
    override def toString() = "SavePoint(%s)" format name
  }

  /**
   * Rolls all transactions back to the start of this `SavePoint`.
   *
   * @note All child `SavePoint` are subsequently rolled back as well. Do not call this method and then attempt to use a
   *       child afterwards, a [[java.sql.SQLException]] will be thrown.
   */
  def rollback(): SavePoint[DBType] = savePoints match{
    case Nil =>
      con rollback ()
      new SavePoint[DBType](con, con setSavepoint ())
    case head :: tail =>
      con rollback savePoint
      tail match{
        case next :: _ => next
        case Nil => new SavePoint[DBType](con, con setSavepoint ())
      }
  }

  override def toString() = "SavePoint"


  final def withConnection[A](op: Connection => A): A = {
    val conn = con
    try {
      op(conn)
    }
    finally {
      if (conn != null) conn.close()
    }
  }
}