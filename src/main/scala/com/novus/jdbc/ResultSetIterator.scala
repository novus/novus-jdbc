package com.novus.jdbc

import java.sql.ResultSet

/**
 * Container class which lazily evaluates a JDBC ResultSet with the supplied function. Reorients the destructive nature
 * of ResultSet::next() with Iterator::next(). Auto-increments the ResultSet to the initial result from the DB.
 * Requires a stateful change to accomplish these goals.
 */
class ResultSetIterator[A](result: ResultSet, f: ResultSet => A) extends Iterator[A] {
  self =>

  private var canBeIncremented = result.next()

  override def hasNext = canBeIncremented

  override def next() = {
    val output = f(result)

    if (canBeIncremented) {
      canBeIncremented = result.next()
    }
    else {
      result.close()
    }

    output
  }

  override def slice(from: Int, to: Int) = new Iterator[A]{
    canBeIncremented = result.relative(from)

    private var until = to

    override def hasNext = self.hasNext

    override def next() = if(until > 0){
      until -= 1
      self.next()
    }
    else{
      Iterator.empty.next()
    }
  }
}