package com.novus.jdbc

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito

class CloseableIteratorSpec extends Specification with Mockito{
  "max" should{
    "only call close once from direct calls" in{
      var cnt = 0
      val iter = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext ={
          val value = inner.hasNext
          if(!value) close()
          value
        }
        def next() = inner.next()
        def close(){ cnt += 1 }
      }

      iter.max

      cnt must be equalTo 1
    }
    "only call close once from indirect calls" in{
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

      iter.drop(1).max

      there was atMostOne(mocked).close()
    }
    "produce the right value" in{
      val iter = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      iter.max must be equalTo 3
    }
  }

  "slice" should{
    "handle empty iterator objects" in{
      val iter = new CloseableIterator[Int] {
        val empty = Iterator.empty
        def hasNext = empty.hasNext
        def next() = empty.next()
        def close(){}
      }

      iter.slice(0,1).isEmpty must beTrue
    }
    "handle when take more than it can give" in{
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      val iter = iter0 slice (0,10)

      (iter next () must be equalTo 1) and
      (iter next () must be equalTo 1) and
      (iter next () must be equalTo -1) and
      (iter next () must be equalTo 3) and
        (iter.hasNext must beFalse) and
        (iter next() must throwA(new NoSuchElementException("next on empty iterator")))
    }
    "handle when we drop more than it has" in{
      val iter = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      iter.slice(10, Int.MaxValue) next() must throwA(new NoSuchElementException("next on empty iterator"))
    }
    "allow toList to work with truncated sets" in{
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      val iter = iter0 slice(0,10)

      iter.toList must haveSize(4)
    }
    "allow toList to work with truncated sets" in{
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      val iter = iter0 slice(10,Int.MaxValue)

      iter.toList must haveSize(0)
    }
    "allow toList to close a connection on a replicated set" in{
      var cnt = 0
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){ cnt += 1 }
      }

      val iter = iter0 slice(0,4)
      iter.toList

      cnt must be equalTo 1
    }
    "allow toList to close a connection on a non-truncated set" in{
      var cnt = 0
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){ cnt += 1 }
      }

      val iter = iter0 slice(0,10)
      iter.toList

      cnt must be equalTo 1
    }
    "allow toList to close a connection on a truncated empty set" in{
      var cnt = 0
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){ cnt += 1 }
      }

      val iter = iter0 slice(10,Int.MaxValue)
      iter.toList

      cnt must be equalTo 1
    }
    "allow toList to close a connection on a truncated empty set" in{
      var cnt = 0
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){ cnt += 1 }
      }

      val iter = iter0 slice(0,1)
      iter.toList

      cnt must be equalTo 1
    }
  }

  "padTo" should {
    "handle cases where it won't be padding" in {
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

     iter0 padTo(3, -4) must haveSize(4)
    }
    "call close when it won't be padding" in {
      var cnt = 0
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() ={
          val out = inner.next()
          if(!inner.hasNext) close()
          out
        }
        def close(){cnt += 1 }
      }

      iter0.padTo(3,7).toList

      cnt must be equalTo 1
    }
    "handle cases where it pads" in{
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      iter0 padTo(14, -4) must haveSize(14)
    }
    "call close when it pads" in{
      var cnt = 0
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator(1, 1, -1, 3)
        def hasNext = inner.hasNext
        def next() ={
          val out = inner.next()
          if(!inner.hasNext) close()
          out
        }
        def close(){ cnt += 1 }
      }

      iter0.padTo(14,7).toList

      cnt must be equalTo 1
    }
    "handle padding an empty iterator" in {
      val iter0 = new CloseableIterator[Int] {
        val inner = Iterator.empty
        def hasNext = inner.hasNext
        def next() = inner.next()
        def close(){}
      }

      iter0 padTo (1,3) must haveSize(1)
    }
  }
}