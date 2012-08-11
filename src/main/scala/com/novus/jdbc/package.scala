package com.novus.jdbc

import java.sql.{ Statement, Connection }

object `package` extends QueryableImplicits

trait SqlServer

trait QueryableImplicits {
  implicit object SqlServerQueryable extends Queryable[SqlServer] {

    def insert(con: Connection, q: String, params: Any*): Iterator[Int] = {
      val prepared = con.prepareStatement(q, Statement.RETURN_GENERATED_KEYS)
      val stmt = statement(prepared, params: _*)
      stmt.executeUpdate()
      val keys = stmt.getGeneratedKeys

      /* SQL Server is "1" array based indexing. Guess starting from "0" like the rest of the world wasn't for them. */
      new ResultSetIterator(keys, _.getInt(1))
    }
  }
}
