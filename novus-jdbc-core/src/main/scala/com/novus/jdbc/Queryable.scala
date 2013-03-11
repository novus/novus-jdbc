package com.novus.jdbc

import java.util.regex.{ Matcher, Pattern }
import annotation.tailrec
import java.sql._
import java.io.{ Reader, InputStream }

/**
 * Abstracts the Database specific logic away from the management of the connection pools.
 *
 * @tparam DBType The database type
 */
trait Queryable[DBType] {

  /**
   * Given a connection, a valid SQL statement, and an optional list of parameters for that statement, executes the
   * statement against the database and returns a JDBC ResultSet.
   */
  def execute[T](query: String, params: Any*)(con: Connection): ResultSet = {
    val prepared = con.prepareStatement(formatQuery(query, params: _*))
    val stmt = statement(prepared, params: _*)
    stmt.executeQuery()
  }

  /**
   * Given a connection, a batch size, an exec statement, and a collection iterator in which each element is
   * a sequence of parameters conforming to the query, executes the statements in batch against the database.
   * Returns an iterator of update counts, per jdbc semantics.
   */
  def executeBatch[I <: Seq[Any]](batchSize: Int = 1000)(query: String, params: Iterator[I])(con: Connection): Iterator[Int] = {
    val prepared = con.prepareStatement(query)
    var results  = Vector[Int]()
    for( group <- params.grouped(batchSize) ) {
      for (currParams <- group) {
        statement(prepared, currParams : _*)
        prepared.addBatch()
      }
      results ++= prepared.executeBatch()
    }
    results.iterator
  }

  /**
   * Given a connection, an insert statement, and an optional list of parameters for that statement, executes the
   * insertion against the database and returns an iterator of IDs.
   */
  def insert(q: String, params: Any*)(con: Connection): Iterator[Int] = {
    val prepared = con.prepareStatement(q, Statement.RETURN_GENERATED_KEYS)
    val stmt = statement(prepared, params: _*)
    stmt.executeUpdate()
    val keys = stmt.getGeneratedKeys

    new ResultSetIterator[ResultSet,Int](keys, _ getInt (0))
  }

  /**
   * Given a connection, an update statement, and an optional list of parameters for that statement, executes the
   * update against the database and returns the count of the rows affected.
   */
  def update(query: String, params: Any*)(con: Connection): Int = {
    val prepared = con.prepareStatement(formatQuery(query, params: _*))
    val stmt = statement(prepared, params: _*)
    stmt.executeUpdate()
  }

  /**
   * Given a connection, a delete statement, and an optional list of parameters for that statement, executes the
   * delete against the database and returns the count of the rows affected.
   */
  def delete(query: String, params: Any*)(con: Connection): Int = update(query, params: _*)(con)

  protected[jdbc] val questionMark = Pattern.compile("""\?""")
  /**
   * PreparedStatements can not take in a List, Set, or some other Iterable as an argument unless they have the required
   * number of query marks (?) in the query string. This method generates a new query string with the right ? count if
   * passed an iterable.
   */
  final protected[jdbc] def formatQuery(q: String, params: Any*): String = {
    if (params.exists(_.isInstanceOf[Iterable[_]])) {
      replace(params.toList, questionMark.matcher(q))
    }
    else {
      q
    }
  }

  /** See formatQuery. */
  @tailrec final protected[jdbc] def replace(params: List[Any], matcher: Matcher, buffer: StringBuffer = new StringBuffer): String = params match {
    case head :: xs if matcher.find() =>
      head match {
        case iter: Iterable[_] if iter.isEmpty => matcher.appendReplacement(buffer, "")
        case iter: Iterable[_]                 => matcher.appendReplacement(buffer, "?" + ",?" * (iter.size - 1))
        case _                                 =>
      }
      replace(xs, matcher, buffer)
    case head :: xs => throw new SQLException("Too many parameters in query %s.".format(buffer.toString))
    case Nil        => matcher.appendTail(buffer).toString
  }

  /**
   * Places the query params into the PreparedStatement. In the case of instances of Iterable, inserts each contained
   * item into the statement individually.
   */
  protected[jdbc] def statement(stmt: PreparedStatement, params: Any*): PreparedStatement = {
    var i = 1
    params foreach { next =>
      next match {
        case null => stmt setNull (i, Types.NULL); i += 1
        case None => stmt setNull (i, Types.NULL); i += 1
        case x: Char => stmt setString (i, x.toString); i += 1
        case Some(value) => stmt setObject (i, value); i += 1
        case x: InputStream => stmt setBinaryStream (i, x); i += 1
        case x: Reader => stmt setCharacterStream (i, x); i += 1
        case iter: Iterable[_] => iter foreach { item =>
          stmt setObject (i, item)
          i += 1
        }
        case x => stmt setObject (i, x); i += 1
      }
    }

    stmt
  }
}