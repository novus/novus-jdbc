/*
 * Copyright (c) 2013 Novus Partners, Inc. (http://www.novus.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
   * @param start the index of the first element in this iterator which forms part of the slice.
   * @param stop the index of the last element following the slice.
   * @return an iterator which advances this iterator past the first `from` elements using `drop`, and then takes
   *         `to - from` elements, using `take`.
   * @note Reuse: $consumesAndProducesIterator
   */
  override def slice(start: Int, stop: Int) ={
    require(0 <= start, "Must be a positive Integer value.")
    require(start <= stop, "The sliced range must have a lower bound less than or equal to the upper bound.")

    canBeIncremented = result relative (start)
    if (!canBeIncremented) close()

    new CloseableIterator[A]{
      private var limit = stop - start

      override def hasNext = self.hasNext && 0 <= limit

      override def next() = if(hasNext){
        limit -= 1
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