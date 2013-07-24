package com.novus.jdbc

import java.sql.{Statement, ResultSet}

/**
 * Container class which lazily evaluates a [[java.sql.ResultSet]] with the supplied function. Reorients the destructive
 * nature of ResultSet#next with Iterator#next. Auto-increments the ResultSet to the initial result from the DB, if it
 * has one.
 *
 * @param statement An underlying [[java.sql.Statement]] backing the results of the query
 * @param result The underlying [[java.sql.ResultSet]] which contains the rows queried from the DB
 * @param f The transform from `Res` to some type `A`
 * @tparam Res A type derived from [[java.sql.ResultSet]]
 * @tparam A The resultant type of the transform `f`
 *
 * @note As with all `Iterator` objects, requires mutable state.
 */
class ResultSetIterator[Res <: ResultSet, +A](statement: Statement, result: Res, f: Res => A) extends CloseableIterator[A] {
  self =>

  private var canBeIncremented = result next ()

  /** Indicates if the underlying [[java.sql.ResultSet]] has more rows. */
  override def hasNext = canBeIncremented

  /**
   * The next element from the underlying [[java.sql.ResultSet]] if it has one. Throws an exception if an attempt is
   * made to access an empty `Iterator`.
   */
  override def next() = if(canBeIncremented){
    val output = f(result)

    canBeIncremented = result next ()
    if (!canBeIncremented) close()

    output
  }
  else Iterator.empty next()

  /**
   * Creates an `Iterator` returning an interval of the values produced by this `Iterator`.
   *
   * @param from the index of the first element in this iterator which forms part of the slice.
   * @param to the index of the first element following the slice.
   * @return an iterator which advances this iterator past the first `from` elements using `drop`, and then takes
   *         `to - from` elements, using `take`.
   * @note Reuse: $consumesAndProducesIterator
   */
  override def slice(from: Int, to: Int) ={
    require(0 <= from, "Must be a positive Integer value.")

    canBeIncremented = result relative (from)
    if (!canBeIncremented) close()

    new CloseableIterator[A]{
      private var until = to - from

      override def hasNext = self.hasNext && 0 <= until

      override def next() = if(hasNext){
        until -= 1
        val output = self next ()
        if(!hasNext) close()
        output
      }
      else Iterator.empty next ()

      def close(){
        self close ()
      }
    }
  }

  /**
   * Releases the underlying [[java.sql.Statement]] which according to the JDBC spec, a properly engineered JAR will
   * also release the underlying [[java.sql.ResultSet]].
   */
  def close(){
    statement close ()
  }
}