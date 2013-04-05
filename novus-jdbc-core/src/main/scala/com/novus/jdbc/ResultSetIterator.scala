package com.novus.jdbc

import java.sql.ResultSet

/**
 * Container class which lazily evaluates a JDBC ResultSet with the supplied function. Reorients the destructive nature
 * of ResultSet::next() with Iterator::next(). Auto-increments the ResultSet to the initial result from the DB.
 * Requires a stateful change to accomplish these goals.
 */
class ResultSetIterator[Res <: ResultSet, +A](result: Res, f: Res => A) extends CloseableIterator[A] {
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
    result close ()
  }
}