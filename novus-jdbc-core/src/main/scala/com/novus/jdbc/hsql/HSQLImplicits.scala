package com.novus.jdbc.hsql

import com.novus.jdbc.Queryable

trait HSQLImplicits{
  implicit object HSQLQueryable extends Queryable[HSQL]
}