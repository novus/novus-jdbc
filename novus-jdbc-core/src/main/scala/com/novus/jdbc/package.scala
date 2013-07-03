package com.novus.jdbc

object `package`{
  import com.novus.jdbc.sqlserver._
  import com.novus.jdbc.hsql._

  type SqlServer = com.novus.jdbc.sqlserver.SqlServer
  type HSQL = com.novus.jdbc.hsql.HSQL

  /**
   * Helper method to ensure that `close` is called after using a `CloseableIterator` within a function.
   *
   * @param iter the iterator
   * @param f the function applied to the iterator
   * @tparam A type returned by the iterator
   * @tparam B type returned by the function invocation
   */
  def manage[A,B](iter: CloseableIterator[A])(f: => B): B = try{
    f
  }
  finally{
    iter close ()
  }
}
