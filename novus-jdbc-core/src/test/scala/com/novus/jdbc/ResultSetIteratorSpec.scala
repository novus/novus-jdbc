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

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import java.sql.{Statement, SQLException, ResultSet}

class ResultSetIteratorSpec extends Specification with Mockito{

  "ResultSetIterator" should{
    "allow toList to work without error" in{
      val statement = mock[Statement]
      val mocked = mock[ResultSet]

      mocked getString("yo") returns "hey" thenThrows (new SQLException())
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo")

      iter.toList must haveSize(1)
    }
    "toList closes the connection" in{
      val statement = mock[Statement]
      val mocked = mock[ResultSet]

      mocked getString("yo") returns "hey" thenThrows (new NoSuchElementException("next on empty iterator"))
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo")
      iter.toList

      there was one(statement).close()
    }
  }

  "next" should{
    val statement = mock[Statement]

    "throw an exception on empty ResultSet objects" in{
      val mocked = mock[ResultSet]
      mocked getString("yo") throws(new SQLException())
      mocked next() returns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo")

      iter next() must throwA(new NoSuchElementException("next on empty iterator"))
    }
    "handle a non-empty ResultSet object" in{
      val mocked = mock[ResultSet]
      mocked getString("yo") returns "hey" thenThrows (new SQLException())
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo")
      (iter next() must beEqualTo("hey")) and
        (iter next() must throwA(new NoSuchElementException("next on empty iterator")))
    }
  }

  "slice" should{
    val statement = mock[Statement]

    "handle empty ResultSet objects" in{
      val mocked = mock[ResultSet]

      mocked relative(0) returns false
      mocked getString("yo") throws(new SQLException())
      mocked next() returns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo")

      iter.slice(0,1).isEmpty must beTrue
    }
    "handle when take more than it can give" in{
      val mocked = mock[ResultSet]

      mocked relative(0) returns true
      mocked getString("yo") returns "hey" thenThrows (new SQLException())
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo") slice(0,10)

      (iter next() must beEqualTo("hey")) and
        (iter.hasNext must beFalse) and
        (iter next() must throwA(new NoSuchElementException("next on empty iterator")))
    }
    "handle when we drop more than it has" in{
      val mocked = mock[ResultSet]

      mocked relative(10) returns false
      mocked getString("yo") throws (new SQLException())
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo")

      iter.slice(10, Int.MaxValue) next() must throwA(new NoSuchElementException("next on empty iterator"))
    }
    "allow toList to work with truncated sets" in{
      val mocked = mock[ResultSet]

      mocked relative(0) returns true
      mocked getString("yo") returns "hey" thenThrows (new SQLException())
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo") slice(0,10)

      iter.toList must haveSize(1)
    }
    "allow toList to work with truncated sets" in{
      val mocked = mock[ResultSet]

      mocked relative(10) returns false
      mocked getString("yo") throws (new SQLException())
      mocked next() returns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo") slice(10,Int.MaxValue)

      iter.toList must haveSize(0)
    }

    "allow toList to close a connection on a non-truncated set" in{
      val statement = mock[Statement]
      val mocked = mock[ResultSet]

      mocked relative(0) returns true
      mocked getString("yo") returns "hey" thenThrows (new SQLException())
      mocked next() returns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo") slice(0,10)
      iter.toList

      there was atLeastOne(statement).close()
    }
    "allow toList to close a connection on a truncated empty set" in{
      val statement = mock[Statement]
      val mocked = mock[ResultSet]

      mocked relative(10) returns false
      mocked getString("yo") throws (new SQLException())
      mocked next() returns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo") slice(10,Int.MaxValue)
      iter.toList

      there was one(statement).close()
    }
    "allow toList to close a connection on a truncated empty set" in{
      val statement = mock[Statement]
      val mocked = mock[ResultSet]

      mocked relative(1) returns true
      mocked getString("yo") returns "hey" thenReturns "yo" thenThrows (new SQLException())
      mocked next() returns true thenReturns true thenReturns false

      val iter = new ResultSetIterator[ResultSet,String](statement, mocked, _ getString "yo") slice(0,1)
      iter.toList

      there was one(statement).close()
    }
  }
}