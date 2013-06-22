package com.novus.jdbc

import java.sql.{Statement, ResultSet}

/**
 * Container class which lazily evaluates a [[java.sql.ResultSet]] with the supplied function. Reorients the destructive
 * nature of ResultSet#next with Iterator#next. Auto-increments the ResultSet to the initial result from the DB.
 * Requires a stateful change to accomplish these goals.
 *
 * @param statement The [[java.sql.Statement]] which was used to query for the `Res`
 * @param result The `Res` that was produced as a result of a query
 * @param f The function transforming a type `Res` to some type `A`
 * @tparam Res A type which extends [[java.sql.ResultSet]]
 * @tparam A The type returned from this `Iterator`
 *
 * @note In order to properly handle DB resources it is recommended that the #close method be called immediately after
 * the contents of this `Iterator` are consumed if not explicitly done by a member function.
 */
class ResultSetIterator[Res <: ResultSet, +A](statement: Statement, result: Res, f: Res => A) extends CloseableIterator[A] {
  self =>

  private var canBeIncremented = result next ()

  override def hasNext = canBeIncremented

  override def next() = if(canBeIncremented){
    val output = f(result)

    canBeIncremented = result next ()
    if (!canBeIncremented) close()

    output
  }
  else Iterator.empty next()

  /**
   * Creates an iterator returning an interval of the values produced by this iterator.
   *
   * @param from the index of the first element in this iterator which forms part of the slice.
   * @param to the index of the first element following the slice.
   * @return an iterator which advances this iterator past the first `from` elements using `drop`, and then takes
   *         `to - from` elements, using `take`.
   * @note Reuse: $consumesAndProducesIterator
   */
  override def slice(from: Int, to: Int) ={
    canBeIncremented = result relative (from)
    if (!canBeIncremented) close()

    new CloseableIterator[A]{
      private var until = to

      override def hasNext = self.hasNext && 0 <= until

      override def next() = if(hasNext){
        until -= 1
        val output = self next ()
        if(until < 0) close()
        output
      }
      else Iterator.empty next ()

      def close(){
        self close ()
      }
    }
  }

  def close(){
    statement close ()
  }
}