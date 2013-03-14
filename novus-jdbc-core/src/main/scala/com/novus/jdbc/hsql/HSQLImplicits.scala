package com.novus.jdbc.hsql

import com.novus.jdbc.{RichResultSet, ResultSetWrapper, ResultSetIterator, Queryable}
import java.sql.{ResultSet, Statement, Connection}

trait HSQLImplicits{
  implicit object HSQLQueryable extends Queryable[HSQL]

  implicit object HSQLWrapper extends ResultSetWrapper[HSQL]{
    def wrap(row: ResultSet) = new RichResultSet(row) {}
  }
}