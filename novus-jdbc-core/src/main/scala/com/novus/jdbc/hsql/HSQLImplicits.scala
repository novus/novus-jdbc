package com.novus.jdbc.hsql

import com.novus.jdbc.{RichResultSet, ResultSetWrapper, ResultSetIterator, Queryable}
import java.sql.{ResultSet, Statement, Connection}

trait HSQLImplicits{
  implicit object HSQLQueryable extends Queryable[HSQL] {

    override def insert(q: String, params: Any*)(con: Connection): Iterator[Int] = {
      val prepared = con.prepareStatement(q, Statement.RETURN_GENERATED_KEYS)
      val stmt = statement(prepared, params: _*)
      stmt.executeUpdate()
      val keys = stmt.getGeneratedKeys

      /* HSQLDB also starts the indexing at "1." Maybe they talked to Microsoft... */
      new ResultSetIterator[ResultSet,Int](keys, _ getInt 1)
    }
  }

  implicit object HSQLWrapper extends ResultSetWrapper[HSQL]{
    def wrap(row: ResultSet) = new RichResultSet(row) {}
  }
}