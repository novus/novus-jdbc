package com.novus.jdbc.h2

import com.novus.jdbc.Queryable

object `package` extends H2Implicits

trait H2

trait H2Implicits{
  implicit object H2Queryable extends Queryable[H2]
}


