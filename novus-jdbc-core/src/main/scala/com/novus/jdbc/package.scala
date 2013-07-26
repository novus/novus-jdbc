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

object `package`{
  import com.novus.jdbc.sqlserver._
  import com.novus.jdbc.hsql._
  import com.novus.jdbc.h2._

  type SqlServer = com.novus.jdbc.sqlserver.SqlServer
  type HSQL = com.novus.jdbc.hsql.HSQL
  type H2 = com.novus.jdbc.h2.H2

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
