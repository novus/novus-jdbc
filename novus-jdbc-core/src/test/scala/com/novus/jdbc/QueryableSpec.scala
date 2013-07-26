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

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import java.sql.{Connection, Statement, ResultSet, PreparedStatement, SQLException, Types}

class QueryableSpec extends Specification with Mockito{

  trait TestDB

  val able = new Queryable[TestDB]{}

  "statement" should{

    "handle multiple Iterable types" in{
      val stmt = mock[PreparedStatement]

      able.statement(stmt, List(1,2), List(3, 4))

      (1 to 4) map (x => there was one(stmt).setObject(x,x)) reduce(_ and _)
    }

    "handle a single Iterable type" in{
      val stmt = mock[PreparedStatement]

      able.statement(stmt, 1, List(2,3))

      (1 to 3) map (x => there was one(stmt).setObject(x,x)) reduce(_ and _)
    }

    "handle a single Iterable type, in any order" in{
      val stmt = mock[PreparedStatement]

      able.statement(stmt, List(1, 2), 3)

      (1 to 3) map (x => there was one(stmt).setObject(x,x)) reduce(_ and _)
    }

    "handle a null" in{
      val stmt = mock[PreparedStatement]

      able.statement(stmt, null)

      there was one(stmt).setNull(1, Types.NULL)
    }

    "handle a None" in{
      val stmt = mock[PreparedStatement]

      able.statement(stmt, None)

      there was one(stmt).setNull(1, Types.NULL)
    }
  }

  "replace" should{

    "handle multiple iterable types" in{
      val matcher = able.questionMark.matcher("SELECT a FROM b WHERE c in(?) AND d in(?)")

      able.replace(List(List(1,2), List(3,4)), matcher) must be equalTo "SELECT a FROM b WHERE c in(?,?) AND d in(?,?)"
    }

    "handle an iterable with one item" in{
      val matcher = able.questionMark.matcher("SELECT a FROM b WHERE c in(?)")

      able.replace(List(List(1)), matcher) must be equalTo "SELECT a FROM b WHERE c in(?)"
    }

    "handle non-iterables" in{
      val matcher = able.questionMark.matcher("SELECT a FROM b WHERE c in(?)")

      able.replace(List(1), matcher) must be equalTo "SELECT a FROM b WHERE c in(?)"
    }
  }

  "formatQuery" should {

    "handle cases where there are Iterables" in {
      able formatQuery ("SELECT 1 FROM foo WHERE v IN(?)", List(1,2)) must be equalTo "SELECT 1 FROM foo WHERE v IN(?,?)"
    }
    "not change the query if there are no Iterables" in {
      val query = "SELECT 1 FROM foo WHERE v = ? AND p = ?"

      able formatQuery (query, "foo", "bar") must be equalTo query
    }
    "handle mixed cases" in {
      val query = "SELECT 1 FROM foo WHERE v = ? AND b IN(?) AND p = ?"

      able formatQuery (query, "foo", List(1,2), "bar") must be equalTo "SELECT 1 FROM foo WHERE v = ? AND b IN(?,?) AND p = ?"
    }
    "throw an error if not enough parameters" in {
      val query = "SELECT 1 FROM foo WHERE v = ? AND p = ?"

      able formatQuery (query, List(1,2,3)) must throwA[SQLException]
    }
    "throw an error if too many parameters" in {
      val query = "SELECT 1 FROM foo WHERE v = ? AND p = ?"

      able formatQuery (query, List(1,2,3), 1, 2, 3) must throwA[SQLException]
    }
  }

  "select" should {

    "produce a statement and a result set with no query parameters" in {
      val query = "SELECT 1 FROM foo"

      val resultset = mock[ResultSet]
      val stmt = mock[Statement]
      stmt executeQuery(anyString) returns resultset

      val con = mock[Connection]
      con createStatement() returns stmt

      val (_, richset) = able.select(query)(con)

      (richset must beAnInstanceOf[RichResultSet]) and
        (there was no(con).prepareStatement(query))
    }
    "produce a statement and a result set with query parameters" in {
      val query = "SELECT 1 FROM foo WHERE b IN (?)"

      val resultset = mock[ResultSet]
      val prepared = mock[PreparedStatement]
      prepared executeQuery() returns resultset
      prepared setObject(anyInt,any) answers(_ => Unit)

      val con = mock[Connection]
      con prepareStatement(anyString) returns prepared

      val (_, richset) = able.select(query, List(1,2))(con)
      (richset must beAnInstanceOf[RichResultSet]) and
        (there was two(prepared).setObject(anyInt,any))
    }
    "close a statement if an exception is thrown" in {
      val query = "SELECT 1 FROM foo WHERE v = ? AND p = ?"

      val resultset = mock[ResultSet]
      val prepared = mock[PreparedStatement]
      prepared executeQuery() returns resultset
      prepared setObject(anyInt,any) throws (new SQLException())

      val con = mock[Connection]
      con prepareStatement(anyString) returns prepared

      (able.select(query, 1, 2, 3)(con) must throwA[SQLException]) and
        (there was one(prepared).close())
    }
    "close a statement if an exception is thrown" in {
      val query = "SELECT 1 FROM foo"

      val statement = mock[Statement]
      statement executeQuery(anyString) throws (new SQLException())
      val con = mock[Connection]
      con createStatement() returns statement

      (able.select(query)(con) must throwA[SQLException]) and
        (there was one(statement).close())
    }
  }

  "insert" should {
    "produce a statement and a result set with no query parameters" in {
      val query = "INSERT INTO foo VALUES(1)"

      val resultset = mock[ResultSet]
      val stmt = mock[Statement]
      stmt executeUpdate(anyString) returns 1
      stmt getGeneratedKeys() returns resultset

      val con = mock[Connection]
      con createStatement() returns stmt

      val iter = able.insert(query)(con)

      (iter must beAnInstanceOf[CloseableIterator[Int]]) and
        (there was no(con).prepareStatement(query))
    }
    "produce a statement and a result set with no query parameters and a compound index" in {
      val query = "INSERT INTO foo VALUES(1)"

      val resultset = mock[ResultSet]
      val stmt = mock[Statement]
      stmt executeUpdate(anyString, any[Array[Int]]) returns 1
      stmt getGeneratedKeys() returns resultset

      val con = mock[Connection]
      con createStatement() returns stmt

      val (_, rs) = able.insert(Array(1, 2), query)(con)

      (rs must beAnInstanceOf[RichResultSet]) and
        (there was no(con).prepareStatement(query))
    }
    "produce a statement and a result set with query parameters" in {
      val query = "INSERT INTO foo VALUES(?)"

      val resultset = mock[ResultSet]
      val prepared = mock[PreparedStatement]
      prepared executeUpdate() returns 1
      prepared setObject(anyInt,any) answers(_ => Unit)
      prepared getGeneratedKeys() returns resultset

      val con = mock[Connection]
      con prepareStatement(anyString,anyInt) returns prepared

      val iter = able.insert(query, 1)(con)
      (iter must beAnInstanceOf[CloseableIterator[Int]]) and
        (there was one(prepared).setObject(anyInt,any))
    }
    "produce a statement and a result set with query parameters and a compound index" in {
      val query = "INSERT INTO foo VALUES(?)"

      val resultset = mock[ResultSet]
      val prepared = mock[PreparedStatement]
      prepared executeUpdate() returns 1
      prepared setObject(anyInt,any) answers(_ => Unit)
      prepared getGeneratedKeys() returns resultset

      val con = mock[Connection]
      con prepareStatement(anyString,any[Array[Int]]) returns prepared

      val (_, rs) = able.insert(Array(1), query, 1)(con)
      (rs must beAnInstanceOf[RichResultSet]) and
        (there was one(prepared).setObject(anyInt,any))
    }
    "close a statement if an exception is thrown" in {
      val query = "INSERT INTO foo VALUES(?,?)"

      val resultset = mock[ResultSet]
      val prepared = mock[PreparedStatement]
      prepared executeQuery() returns resultset
      prepared setObject(anyInt,any) throws (new SQLException())

      val con = mock[Connection]
      con prepareStatement(anyString, anyInt) returns prepared

      (able.insert(query, 1, 2)(con) must throwA[SQLException]) and
        (there was one(prepared).close())
    }
    "close a statement if an exception is thrown" in {
      val query = "INSERT INTO foo VALUES(1)"

      val statement = mock[Statement]
      statement executeUpdate(anyString,anyInt) throws (new SQLException())
      val con = mock[Connection]
      con createStatement() returns statement

      (able.insert(query)(con) must throwA[SQLException]) and
        (there was one(statement).close())
    }
  }

  "update" should {
    "produce an updated count with no query parameters" in {
      val query = "UPDATE foo SET bar=1"

      val stmt = mock[Statement]
      stmt executeUpdate(anyString) returns 1

      val con = mock[Connection]
      con createStatement() returns stmt

      (able.update(query)(con) must be equalTo 1) and
        (there was one(stmt).close())
    }
    "produce an updated count with query parameters" in {
      val query = "UPDATE foo SET bar=?"

      val prepared = mock[PreparedStatement]
      prepared executeUpdate() returns 1
      prepared setObject(anyInt,any) answers(_ => Unit)

      val con = mock[Connection]
      con prepareStatement(anyString) returns prepared

      (able.update(query, 1)(con) must be equalTo 1) and
        (there was one(prepared).setObject(anyInt,any)) and
        (there was one(prepared).close())
    }
    "close a statement if an exception is thrown" in {
      val query = "UPDATE foo SET bar=?"

      val prepared = mock[PreparedStatement]
      prepared executeUpdate() returns 1
      prepared setObject(anyInt,any) throws (new SQLException())

      val con = mock[Connection]
      con prepareStatement(anyString) returns prepared

      (able.update(query, 1)(con) must throwA[SQLException]) and
        (there was one(prepared).close())
    }
    "close a statement if an exception is thrown" in {
      val query = "UPDATE foo SET bar=1"

      val statement = mock[Statement]
      statement executeUpdate(anyString) throws (new SQLException())
      val con = mock[Connection]
      con createStatement() returns statement

      (able.update(query)(con) must throwA[SQLException]) and
        (there was one(statement).close())
    }
  }
}