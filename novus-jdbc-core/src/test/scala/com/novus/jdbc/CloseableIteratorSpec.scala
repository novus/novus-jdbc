package com.novus.jdbc

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito

class CloseableIteratorSpec extends Specification with Mockito{
  def createMocks() = {
    val mocked = mock[CloseableIterator[Int]]
    mocked.hasNext returns(true, true, true, true, false)
    mocked.next() returns(1, 1, -1, 3)

    val iter = new CloseableIterator[Int] {
      def hasNext = mocked.hasNext
      def next() = mocked.next()
      def close(){
        mocked.close()
      }
    }

    (mocked, iter)
  }

  "max" should{
    "only call close once from direct calls" in{
      val (mocked, iter) = createMocks()

      iter.max

      there was atMostOne(mocked).close()
    }
    "only call close once from indirect calls" in{
      val (mocked, iter) = createMocks()

      iter.drop(1).max

      there was atMostOne(mocked).close()
    }
    "produce the right value" in{
      val (_, iter) = createMocks()

      iter.max must be equalTo 3
    }
  }

  "slice" should{
    "handle empty ResultSet objects" in{
      val mocked = mock[CloseableIterator[Int]]
          mocked.hasNext returns(false)
          mocked.next() throws (new NoSuchElementException("next on empty iterator"))

          val iter = new CloseableIterator[Int] {
            def hasNext = mocked.hasNext
            def next() = mocked.next()
            def close(){
              mocked.close()
            }
          }

          (mocked, iter)

      iter.slice(0,1).isEmpty must beTrue
    }
    "handle when take more than it can give" in{
      val (_, iter0) = createMocks()

      val iter = iter0 slice (0,10)

      (iter next () must be equalTo 1) and
      (iter next () must be equalTo 1) and
      (iter next () must be equalTo -1) and
      (iter next () must be equalTo 3) and
        (iter.hasNext must beFalse) and
        (iter next() must throwA(new NoSuchElementException("next on empty iterator")))
    }
  }
}