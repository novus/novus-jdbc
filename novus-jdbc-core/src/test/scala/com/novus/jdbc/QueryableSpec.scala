package com.novus.jdbc

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import java.sql._

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
  }
}