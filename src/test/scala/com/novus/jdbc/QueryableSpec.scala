package com.novus.jdbc

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import java.sql.{Types, PreparedStatement}

trait TestDB

class QueryableSpec extends Specification with Mockito{

  "statement" should{
    val able = new Queryable[TestDB]{}

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
    val able = new Queryable[TestDB]{}

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
}