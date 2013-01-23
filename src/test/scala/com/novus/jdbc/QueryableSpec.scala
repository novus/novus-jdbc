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
}