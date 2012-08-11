package com.novus.jdbc

import java.sql.ResultSet

/**
 * Container class which lazily evaluates a JDBC ResultSet with the supplied function. Reorients the destructive nature
 * of ResultSet::next() with Iterator::next(). Auto-increments the ResultSet to the initial result from the DB.
 * Requires a stateful change to accomplish these goals.
 */
class ResultSetIterator[A](result: ResultSet, f: ResultSet => A) extends Iterator[A] {
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
}