package com.novus.jdbc.hsql

import com.novus.jdbc.Queryable

object `package` extends HSQLImplicits

sealed trait HSQL

trait HSQLImplicits{
  implicit object HSQLQueryable extends Queryable[HSQL]
}