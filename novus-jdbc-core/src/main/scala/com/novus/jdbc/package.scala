package com.novus.jdbc

object `package`{
  import com.novus.jdbc.sqlserver._
  import com.novus.jdbc.hsql._

  /**
   * Helper method to ensure that `close` is called after using a `CloseableIterator` within a function.
   *
   * @param iter the iterator
   * @param f the function applied to the iterator
   * @tparam A type returned by the iterator
   * @tparam B type returned by the function invocation
   */
  def manage[A,B](iter: CloseableIterator[A])(f: CloseableIterator[A] => B): B ={
    val out = f(iter)
    iter close ()
    out
  }

  /**
   * Helper method to ensure that `close` is called after using a `CloseableIterator` within a function.
   *
   * @param f the function to apply to the iterator
   * @tparam A type returned by the iterator
   * @tparam B type returned by the function
   * @return a decorated function of the same type as `f`
   */
  def manage[A,B](f: CloseableIterator[A] => B): CloseableIterator[A] => B = manage(_)(f)
}




