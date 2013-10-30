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

import java.sql.{SQLException, Connection, PreparedStatement}
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
trait QueryExecutor[DBType] extends StatementExecutor[DBType]{
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
      case ex: Exception            => log error ("%s threw exception with %s" format (this, msg), ex); throw ex
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
  final def selectOne[T](q: String, params: Any*)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] =
    execute(q, params: _*) { query one (f, q, params: _*) }

  /**
   * Execute a query and transform only the head of the [[com.novus.jdbc.RichResultSet]]. If this query would produce
   * multiple results, they are lost.
   *
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @tparam T The returned type from the query
   */
  final def selectOne[T](q: String)(f: RichResultSet => T)(implicit query: Queryable[DBType]): Option[T] =
    execute(q){ query one (f, q) }

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
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
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

  /**
   * Executes arbitrary SQL statements.
   *
   * @param q The query statement
   */
  final def exec(q: String)(implicit query: Queryable[DBType]){
    execute(q){ query execute q }
  }

  /**
   * Executes arbitrary SQL statements.
   *
   * @param q The query statement.
   * @param params The query parameters
   */
  final def exec(q: String, params: Any*)(implicit query: Queryable[DBType]){
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
    execute(q){ query proc (f, q) }

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
    execute(q, params: _*){ query proc (f, q, params: _*) }

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](out: Array[Int], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    execute(q) { query proc (out, f, q) }

  /**
   * Execute a stored procedure containing OUT parameters, yield the resolution of those parameters.
   *
   * @param out The query OUT parameters
   * @param q The query statement
   * @param f A transform from a [[com.novus.jdbc.StatementResult]] to a type `T`
   * @tparam T The return type of the query
   */
  final def proc[T](out: Array[String], q: String)(f: StatementResult => T)(implicit query: Queryable[DBType]): T =
    execute(q) { query proc (out, f, q) }

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
    execute(q, params: _*) { query proc (out, f, q, params: _*) }

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
    execute(q, params: _*) { query proc (out, f, q, params: _*) }

  /**
   * Execute a series of statements without committing them to the database until every single statement was successful.
   * Allows for all statements to be rolled back at the event of an exception or at any point of the transaction.
   *
   * @param f a transform from a [[com.novus.jdbc.SavePoint]] to a type `T`
   * @tparam T The return type of the transaction
   */
  final def transaction[T](f: SavePoint[DBType] => T)(implicit query: Queryable[DBType]): T ={
    val con = connection()
    try{
      con setAutoCommit false
      val out = f(new SavePoint[DBType](con, con setSavepoint ()))
      con commit ()

      out
    }
    catch{
      case ex: NullPointerException => log error ("{} pool object returned a null connection", this); throw ex
      case ex: Exception =>
        try{
          con rollback ()
        }
        catch{
          case ex: SQLException => log error ("Unable to rollback transaction", ex)
        }
        log error ("%s threw exception" format this, ex)
        throw ex
    }
    finally{
      if(con != null){
        con setAutoCommit true
        con close ()
      }
    }
  }

  /** Batch insertion method that takes care of inserting a massive Iterator[A].
    * @param insert `INSERT` SQL statement
    * @param batchSize number of rows to be inserted in each batch
    * @param set0 curried function that takes a `PreparedStatement` and returns a function of type `A => Unit` that, when called, will update by side effect the `PreparedStatement` with the given `A`'s contents.
    * @param log an optional logging function that can log three numeric metrics: index of batch which was just executed, number of elements in that batch, and number of rows inserted into the underlying database
    * @param elems an iterator of some `A`'s
    */
  final def batchInsert[A](insert: String, batchSize: Int, set0: PreparedStatement => A => Unit, log: (Int, Int, Int) => Unit = (_, _, _) => ())(elems: Iterator[A]) {
    var _conn = Option.empty[Connection]
    var _stmt = Option.empty[PreparedStatement]
    try {
      _conn = Some(connection())
      _stmt = _conn.map(_.prepareStatement(insert))

      val Some(conn) = _conn
      val Some(stmt) = _stmt
      val set = set0(stmt)

      var batchCount = 0

      for (batch <- elems.sliding(batchSize, batchSize)) {
        var elemCount = 0

        for (elem <- batch) {
          set(elem)
          stmt.addBatch()
          elemCount += 1
        }

        val affectedCount = stmt.executeBatch()
        batchCount += 1
        log(batchCount, elemCount, affectedCount.sum)
      }

      conn.commit()
    }
    finally {
      _stmt.foreach(_.close())
      _conn.foreach(_.close())
    }
  }

  /** Shuts down the underlying connection pool. Should be called before this object is garbage collected. */
  def shutdown()
}
