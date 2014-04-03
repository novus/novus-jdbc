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

import java.util.regex.{ Matcher, Pattern }
import annotation.tailrec
import java.sql.{CallableStatement, Connection, Statement, ResultSet, PreparedStatement, SQLException, Types, Date, Timestamp}
import java.io.{ Reader, InputStream }
import xml.{NodeSeq, Document}
import org.joda.time.{DateTime, LocalDate, LocalTime}

/**
 * Abstracts the Database specific logic away from the management of the connection pools.
 *
 * @tparam DBType The database type
 */
trait Queryable[DBType] {

  /**
   * This function takes a [[java.sql.ResultSet]] and converts it into a [[com.novus.jdbc.RichResultSet]].
   *
   * @param row The object to be converted
   */
  def wrap(row: ResultSet) = new RichResultSet(row)

  /**
   * Takes a [[java.sql.CallableStatement]] and convert it into a [[com.novus.jdbc.StatementResult]] for evaluation by
   * a function call.
   *
   * @param callable The object to be converted
   */
  def wrap(callable: CallableStatement) = new StatementResult(callable)

  /**
   * Given a connection, a valid SQL statement, and a list of parameters for that statement, executes the statement
   * against the database and returns a JDBC ResultSet.
   *
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def select[T](f: RichResultSet => T, query: String, params: Any*)(con: Connection): CloseableIterator[T] = {
    val prepared = con prepareStatement formatQuery(query, params: _*)
    try{
      statement(con, prepared, params: _*)

      new ResultSetIterator(prepared, wrap(prepared executeQuery ()), f)
    }
    catch{
      case ex: Throwable => prepared close (); throw ex
    }
  }

  /**
   * Given a connection and a valid SQL statement, executes the statement against the database and returns a JDBC
   * ResultSet.
   *
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def select[T](f: RichResultSet => T, query: String)(con: Connection): CloseableIterator[T] = {
    val stmt = con createStatement ()
    try{
      new ResultSetIterator(stmt, wrap(stmt executeQuery query), f)
    }
    catch{
      case ex: Throwable => stmt close (); throw ex
    }
  }

  /**
   * Given a connection and a valid SQL statement, executes the statement against the database and returns the first
   * value parsed iff that statement produces a non-empty result.
   *
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def one[T](f: RichResultSet => T, query: String)(con: Connection): Option[T] = {
    val stmt = con createStatement ()
    try{
      val rs = wrap(stmt executeQuery query)

      if(rs next ()) Some(f(rs)) else None
    }
    finally{
      stmt close ()
    }
  }

  /**
   * Given a connection and a valid SQL statement, executes the statement against the database and returns the first
   * value parsed iff that statement produces a non-empty result.
   *
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def one[T](f: RichResultSet => T, query: String, params: Any*)(con: Connection): Option[T] = {
    val prepared = con prepareStatement formatQuery(query, params: _*)
    try{
      statement(con, prepared, params: _*)
      val rs = wrap(prepared executeQuery ())

      if(rs next ()) Some(f(rs)) else None
    }
    finally{
      prepared close ()
    }
  }

  /**
   * Given a connection, a batch size, an exec statement, and a collection iterator in which each element is
   * a sequence of parameters conforming to the query, executes the statements in batch against the database.
   * Returns an iterator of update counts, per jdbc semantics.
   */
  def executeBatch[I <: Seq[Any]](batchSize: Int = 1000)(query: String, params: Iterator[I])(con: Connection): Iterator[Int] = {
    val prepared = con.prepareStatement(query)
    try{
      var results  = Vector[Int]()
      for( group <- params.grouped(batchSize) ) {
        for (currParams <- group) {
          statement(con, prepared, currParams : _*)
          prepared.addBatch()
        }
        results ++= prepared.executeBatch()
      }
      results.iterator
    }
    finally{
      prepared close ()
    }
  }

  /**
   * Given a connection, an insert statement, and a list of parameters for that statement, executes the insertion
   * against the database and returns an iterator of IDs.
   *
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   */
  def insert(query: String, params: Any*)(con: Connection): CloseableIterator[Int] = {
    val prepared = con prepareStatement (query, Statement.RETURN_GENERATED_KEYS)
    try{
      statement(con, prepared, params: _*) executeUpdate ()

      new ResultSetIterator[ResultSet,Int](prepared, prepared getGeneratedKeys (), _ getInt 1) //compiler can't deduce the types...
    }
    catch{
      case ex: Throwable => prepared close (); throw ex
    }
  }

  /**
   * Given a connection, an array of column indexes of a compound auto generated key, an insert statement, and a list of
   * parameters for that statement, executes the insertion against the database and returns a JDBC ResultSet.
   *
   * @param columns The index of each compound key column
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def insert[T](columns: Array[Int], f: RichResultSet => T, query: String, params: Any*)(con: Connection): CloseableIterator[T] ={
    val prepared = con prepareStatement (query, columns)
    try{
      statement(con, prepared, params: _*) executeUpdate ()

      new ResultSetIterator(prepared, wrap(prepared getGeneratedKeys ()), f)
    }
    catch{
      case ex: Throwable => prepared close (); throw ex
    }
  }

  /**
   * Given a connection, an array of column names of a compound auto generated key, an insert statement, and a list of
   * parameters for that statement, executes the insertion against the database and returns a JDBC ResultSet.
   *
   * @param columns The name of each compound key column
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   */
  def insert[T](columns: Array[String], f: RichResultSet => T, query: String, params: Any*)(con: Connection): CloseableIterator[T] ={
    val prepared = con prepareStatement (query, columns)
    try{
      statement(con, prepared, params: _*) executeUpdate ()

      new ResultSetIterator(prepared, wrap(prepared getGeneratedKeys ()), f)
    }
    catch{
      case ex: Throwable => prepared close (); throw ex
    }
  }

  /**
   * Given a connection and an insert statement, executes the insertion against the database and returns an iterator of
   * IDs.
   *
   * @param query The query string
   * @param con A database connection object
   */
  def insert(query: String)(con: Connection): CloseableIterator[Int] = {
    val stmt = con createStatement ()
    try{
      stmt executeUpdate (query, Statement.RETURN_GENERATED_KEYS)

      new ResultSetIterator[ResultSet,Int](stmt, stmt getGeneratedKeys (), _ getInt 1) //compiler can't deduce the types...
    }
    catch{
      case ex: Throwable => stmt close (); throw ex
    }
  }

  /**
   * Given a connection, an array of column indexes of a compound auto generated key and an insert statement, executes
   * the insertion against the database and returns a JDBC ResultSet.
   *
   * @param columns The array of column indexes
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param con A database connection object
   */
  def insert[T](columns: Array[Int], f: RichResultSet => T, query: String)(con: Connection): CloseableIterator[T] ={
    val stmt = con createStatement ()
    try{
      stmt execute (query, columns)

      new ResultSetIterator(stmt, wrap(stmt getGeneratedKeys ()), f)
    }
    catch{
      case ex: Throwable => stmt close (); throw ex
    }
  }

  /**
   * Given a connection, an array of column names of a compound auto generated key and an insert statement, executes
   * the insertion against the database and returns a JDBC ResultSet.
   *
   * @param columns The array of column names
   * @param f A transform from a `RichResultSet` to a type `T`
   * @param query The query string
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def insert[T](columns: Array[String], f: RichResultSet => T, query: String)(con: Connection): CloseableIterator[T] ={
    val stmt = con createStatement ()
    try{
      stmt execute (query, columns)

      new ResultSetIterator(stmt, wrap(stmt getGeneratedKeys ()), f)
    }
    catch{
      case ex: Throwable => stmt close (); throw ex
    }
  }

  /**
   * Given a connection, an update statement and a list of parameters for that statement, executes the update against
   * the database and returns the count of the rows affected.
   *
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   */
  def update(query: String, params: Any*)(con: Connection): Int = {
    val prepared = con prepareStatement formatQuery(query, params: _*)
    try{
      statement(con, prepared, params: _*) executeUpdate ()
    }
    finally {
      prepared close ()
    }
  }

  /**
   * Given a connection and an update statement, executes the update against the database and returns the count of the
   * rows affected.
   *
   * @param query The query string
   * @param con A database connection object
   */
  def update(query: String)(con: Connection): Int = {
    val stmt = con createStatement ()
    try{
      stmt executeUpdate query
    }
    finally {
      stmt close ()
    }
  }

  /**
   * Given a connection, a delete statement, and a list of parameters for that statement, executes the delete against
   * the database and returns the count of the rows affected.
   *
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   */
  @inline def delete(query: String, params: Any*)(con: Connection): Int = update(query, params: _*)(con)

  /**
   * Given a connection and a delete statement, executes the delete against the database and returns the count of the
   * rows affected.
   *
   * @param query The query string
   * @param con A database connection object
   */
  @inline def delete(query: String)(con: Connection): Int = update(query)(con)

  /**
   * Given a connection, a valid merge statement, and a list of parameters for that statement, executes the merge
   * against the database and returns an iterator of IDs.
   *
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   */
  @inline def merge(query: String, params: Any*)(con: Connection): CloseableIterator[Int] = insert(query, params: _*)(con)

  /**
   * Given a connection and a valid merge statement, executes the merge against the database and returns an iterator of
   * IDs.
   *
   * @param query The query string
   * @param con A database connection object
   */
  @inline def merge(query: String)(con: Connection): CloseableIterator[Int] = insert(query)(con)

  /**
   * Given a connection, executes the query against the database.
   *
   * @param query The query string
   * @param con A database connection object
   */
  def execute(query: String)(con: Connection) {
    val stmt = con createStatement ()
    try{
      stmt execute query
    }
    finally {
      stmt close ()
    }
  }

  /**
   * Given a connection, executes the query against the database.
   *
   * @param query The query string
   * @param params The query parameters.
   * @param con A database connection object
   */
  def execute(query: String, params: Any*)(con: Connection) {
    val prepared = con prepareStatement formatQuery(query, params: _*)
    try{
      statement(con, prepared, params: _*) execute ()
    }
    finally {
      prepared close ()
    }
  }

  /**
   * Given a connection and a valid stored procedure, executes the procedure against the database and returns an
   * iterator of the parsed [[com.novus.jdbc.RichResultSet]].
   *
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @param query The query string
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def proc[T](f: RichResultSet => T, query: String)(con: Connection): CloseableIterator[T] ={
    val callable = con prepareCall query
    try{
      new ResultSetIterator(callable, wrap(callable executeQuery ()), f)
    }
    catch{
      case ex: Throwable => callable close (); throw ex
    }
  }

  /**
   * Given a connection, a valid stored procedure and a list of parameters for that procedure, executes the procedure
   * against the database and returns an iterator of the parsed [[com.novus.jdbc.RichResultSet]].
   *
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @param query The query string
   * @param params The query parameters
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def proc[T](f: RichResultSet => T, query: String, params: Any*)(con: Connection): CloseableIterator[T] ={
    val callable = con prepareCall formatQuery(query, params: _*)
    try{
      statement(con, callable, params: _*)

      new ResultSetIterator(callable, wrap(callable executeQuery ()), f)
    }
    catch{
      case ex: Throwable => callable close (); throw ex
    }
  }

  /**
   * Given a connection and a valid stored procedure, executes the procedure against the database and returns the parsed
   * result.
   *
   * @param out The names of the OUT parameters in the stored procedure
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @param query The query string
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[String], f: StatementResult => T, query: String)(con: Connection): T ={
    val callable = con prepareCall query
    try{
      out foreach { name =>
        callable registerOutParameter (name, Types.JAVA_OBJECT)
      }
      callable execute ()

      f(wrap(callable))
    }
    finally{
      callable close ()
    }
  }

  /**
   * Given a connection and a valid stored procedure, executes the procedure against the database and returns the parsed
   * result.
   *
   * @param out The indexes of the OUT parameters in the stored procedure
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @param query The query string
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[Int], f: StatementResult => T, query: String)(con: Connection): T ={
    val callable = con prepareCall query
    try{
      out foreach { name =>
        callable registerOutParameter (name, Types.JAVA_OBJECT)
      }
      callable execute ()

      f(wrap(callable))
    }
    finally{
      callable close ()
    }
  }

  /**
   * Given a connection, a valid stored procedure and a list of parameters, executes the procedure against the database
   * and returns the parsed result.
   *
   * @param out The names of the OUT parameters in the stored procedure
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @param query The query string
   * @param params The query paramters
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[String], f: StatementResult => T, query: String, params: Any*)(con: Connection): T ={
    val callable = con prepareCall formatQuery(query, params: _*)
    try{
      out foreach { name =>
        callable registerOutParameter (name, Types.JAVA_OBJECT)
      }
      statement(con, callable, params: _*) execute ()

      f(wrap(callable))
    }
    finally{
      callable close ()
    }
  }

  /**
   * Given a connection, a valid stored procedure and a list of parameters, executes the procedure against the database
   * and returns the parsed result.
   *
   * @param out The indexes of the OUT parameters in the stored procedure
   * @param f A transform from a [[com.novus.jdbc.RichResultSet]] to a type `T`
   * @param query The query string
   * @param params The query paramters
   * @param con A database connection object
   * @tparam T The return type of the query
   */
  def proc[T](out: Array[Int], f: StatementResult => T, query: String, params: Any*)(con: Connection): T ={
    val callable = con prepareCall formatQuery(query, params: _*)
    try{
      out foreach { name =>
        callable registerOutParameter (name, Types.JAVA_OBJECT)
      }
      statement(con, callable, params: _*) execute ()

      f(wrap(callable))
    }
    finally{
      callable close ()
    }
  }

  protected[jdbc] val questionMark = Pattern.compile("""\?""")
  /**
   * PreparedStatements can not take in a List, Set, or some other Iterable as an argument unless they have the required
   * number of query marks (?) in the query string. This method generates a new query string with the right ? count if
   * passed an iterable.
   *
   * @param query The query string
   * @param params The query parameters
   */
  final protected[jdbc] def formatQuery(query: String, params: Any*): String =
    if (params exists (_.isInstanceOf[Iterable[_]])) replace(params.toList, questionMark matcher query) else query

  /**
   * Parses the parameter list and substitutes a list '?' for each parameter which is an instance of an
   * [[scala.collection.Iterable]]. If the matched query contains too few available '?' for the number of parameters,
   * throws an exception.
   *
   * @param params The list of parameter objects
   * @param matcher A [[java.util.regex.Matcher]] over the query string
   * @param buffer The accumulated [[java.lang.StringBuffer]]
   *
   * @see #formatQuery.
   */
  @tailrec final protected[jdbc] def replace(params: List[Any], matcher: Matcher, buffer: StringBuffer = new StringBuffer): String = params match {
    case head :: xs if matcher.find() =>
      head match {
        case iter: Iterable[_] if iter.isEmpty => matcher appendReplacement (buffer, "")
        case iter: Iterable[_]                 => matcher appendReplacement (buffer, "?" + ",?" * (iter.size - 1))
        case _                                 =>
      }
      replace(xs, matcher, buffer)
    case head :: xs            => throw new SQLException("Too many parameters in query %s." format matcher.appendTail(buffer).toString)
    case Nil if matcher.find() => throw new SQLException("Too few parameters in query %s" format matcher.appendTail(buffer).toString)
    case Nil                   => matcher.appendTail(buffer).toString
  }

  /**
   * Places the query params into the PreparedStatement. In the case of instances of [[scala.collection.Iterable]],
   * inserts each contained item into the statement individually.
   *
   * @param stmt The `PreparedStatement` to modify
   * @param params The list of parameter objects
   *
   * @note This works via side-effect due to the underlying nature of the Java JDBC API. The return of the statement is
   *       merely for chaining method calls.
   */
  protected[jdbc] def statement(con: Connection, stmt: PreparedStatement, params: Any*): PreparedStatement = {
    var i = 1
    def set[T](next: T) {
      next match {
        case null => stmt setNull (i, Types.NULL)
        case None => stmt setNull (i, Types.NULL)
        case xml: NodeSeq =>
          val container = con createSQLXML ()
          container setString (xml toString ())
          stmt setSQLXML (i, container)
        case x: Char => stmt setObject (i, x, Types.CHAR)
        case Some(value) => stmt setObject (i, value)
        case x: InputStream => stmt setBinaryStream (i, x)
        case x: Reader => stmt setCharacterStream (i, x)
        case Right(value) => stmt setObject (i, value)
        case Left(value) => stmt setObject (i, value)
        case x: java.math.BigDecimal => stmt setBigDecimal(i, x)
        case x: java.math.BigInteger => stmt setObject(i, x, Types.BIGINT)
        case iter: Iterable[_] => iter.foreach(set)
        case x: DateTime => stmt.setTimestamp(i, new Timestamp(x.getMillis))
        case x: LocalDate => stmt.setDate(i, new Date(x.toDateTime(LocalTime.MIDNIGHT).getMillis))
        case x => stmt setObject (i, x)
      }
      i += 1
    }
    params.foreach(set)
    stmt
  }
}
