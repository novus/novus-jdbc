package com.novus.jdbc

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito

class CloseableIteratorSpec extends Specification with Mockito{
  def counter(f: () => Any, iter: Iterator[Int] = Iterator(1, 1, -1, 3)) = new CloseableIterator[Int] {
    def hasNext = {
      val out = iter.hasNext
      if(!out) close()
      out
    }
    def next() = iter next ()
    def close(){ f() }
  }
  def nonCounter(iter: Iterator[Int] = Iterator(1, 1, -1, 3)) = new CloseableIterator[Int] {
    def hasNext = {
      val out = iter.hasNext
      if(!out) close()
      out
    }
    def next() = iter next ()
    def close(){ }
  }

  "max" should{
    "only call close once from direct calls" in{
      var cnt = 0
      val iter = counter(() => cnt += 1)

      iter.max

      cnt must be greaterThan 0
    }
    "only call close once from indirect calls" in{
      val it = Iterator(1,2,3,4)
      val mocked = mock[CloseableIterator[Int]]
      mocked.hasNext answers{ _ => it.hasNext }
      mocked.next() answers{ _ => it next () }

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
      val iter = nonCounter()

      iter.max must be equalTo 3
    }
  }

  "slice" should{
    "handle empty iterator objects" in{
      val iter = nonCounter(Iterator.empty)

      iter.slice(0,1).isEmpty must beTrue
    }
    "handle when take more than it can give" in{
      val iter0 = nonCounter()

      val iter = iter0 slice (0,10)

      (iter next () must be greaterThan 0) and
      (iter next () must be greaterThan 0) and
      (iter next () must be equalTo -1) and
      (iter next () must be equalTo 3) and
        (iter.hasNext must beFalse) and
        (iter next() must throwA(new NoSuchElementException("next on empty iterator")))
    }
    "handle when we drop more than it has" in{
      val iter = nonCounter()

      iter.slice(10, Int.MaxValue) next() must throwA(new NoSuchElementException("next on empty iterator"))
    }
    "allow toList to work with truncated sets" in{
      val iter0 = nonCounter()

      val iter = iter0 slice(0,10)

      iter.toList must haveSize(4)
    }
    "allow toList to work with truncated sets" in{
      val iter0 = nonCounter()

      val iter = iter0 slice(10,Int.MaxValue)

      iter.toList must haveSize(0)
    }
    "allow toList to close a connection on a replicated set" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 slice(0,4)
      iter.toList

      cnt must be greaterThan 0
    }
    "allow toList to close a connection on a non-truncated set" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 slice(0,10)
      iter.toList

      cnt must be greaterThan 0
    }
    "allow toList to close a connection on a truncated empty set" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 slice(10,Int.MaxValue)
      iter.toList

      cnt must be greaterThan 0
    }
    "allow toList to close a connection on a truncated empty set" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 slice(0,1)
      iter.toList

      cnt must be greaterThan 0
    }
  }

  "padTo" should {
    "handle cases where it won't be padding" in {
      val iter0 = nonCounter()

     (iter0 padTo(3, -4) size) must be equalTo(4) //must haveSize(4)
    }
    "call close when it won't be padding" in {
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.padTo(3,7).toList

      cnt must be greaterThan 0
    }
    "handle cases where it pads" in{
      val iter0 = nonCounter()

      (iter0 padTo(14, -4) size) must be equalTo(14)//must haveSize(14)
    }
    "call close when it pads" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.padTo(14,7).toList

      cnt must be greaterThan 0
    }
    "handle padding an empty iterator" in {
      val iter0 = nonCounter()

      (iter0 padTo (1,3) size) must be equalTo(4)//must haveSize(4)
    }
    "call close on an iterator that has been padded, dropped and then toList" in {
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0 padTo (7, 14) drop(3) toList

      cnt must be greaterThan 0
    }
  }

  "length" should{
    "call close" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.length

      cnt must be greaterThan 0
    }
    "call close after drop is called" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.drop(1).length

      cnt must be greaterThan 1
    }
  }

  "collectFirst" should{
    "find a defined value and call close" in{
      var cnt = 0
      val iter0 = new CloseableIterator[Any] {
        val inner = Iterator(1, 1, "foo", 3)
        def hasNext = inner.hasNext
        def next() = inner next ()
        def close(){ cnt += 1 }
      }

      val found = iter0 collectFirst{
        case x:String => x
      }

      (found must beSome("foo")) and
        (cnt must be greaterThan 0)
    }
    "call close if no value is found" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val found = iter0 collectFirst{
        case 42 => "foo"
      }

      (found must beNone) and
        (cnt must be greaterThan 0)
    }
  }

  "takeWhile" should {
    "handle an empty iterator" in{
      val iter0 = nonCounter()
      val iter = iter0 takeWhile(_ == 0)
      iter.hasNext must beFalse
    }
    "create an empty iterator if the predicate is never satisfied" in{
      val iter0 = nonCounter()

      val iter = iter0 takeWhile(_ == 42)

      iter.hasNext must beFalse
    }
    "call close if the predicate is never satisfied" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.takeWhile(_ == 42).toList

      cnt must be greaterThan 0
    }
    "" in{
      val iter0 = nonCounter()

      (iter0 takeWhile(_ == 1) size) must be equalTo(2)//must haveSize(2)
    }
    "call close" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.takeWhile(_ == 1).toList

      cnt must be greaterThan 0
    }
  }

  "patch" should{
    "not replace anything if the iterator is too small" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 patch(10, Iterator(5, 5), 12)
      ((iter size) must be equalTo(4)) and //(iter must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
    "not replace anything if the that iterator is empty" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 patch(2, Iterator.empty, 4)
      ((iter size) must be equalTo(4)) and //(iter must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
    "replace up to the that iterator's length if not as big as the requested replacement" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val list = iter0.patch(2, Iterator(5, 7), 6).toList
      ((list size) must be equalTo(4)) and //(list must haveSize(4)) and
        (list must contain(5)) and
        (list must contain(7)) and
        (cnt must be greaterThan 0)
    }
    "replace [to, from]" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 patch(1, Iterator(4, 4), 3)
      (iter.toList must be equalTo(List(1, 4, 4, 3))) and
        (cnt must be greaterThan 0)
    }
    "replace [to, from] only if the that iterator is larger than needed" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      val iter = iter0 patch(1, Iterator(2, 3, 4), 2)
      (iter.toList must be equalTo(List(1, 2, 3, 3))) and
        (cnt must be greaterThan 0)
    }
    "replace [to, from] even if from exceeds the iterator's length" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 patch(1, Iterator(2, 3, 4), 5)

      (iter.toList must be equalTo(List(1, 2, 3, 4))) and
        (cnt must be greaterThan 0)
    }
    "handle if 'to' is 0" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 patch(0, Iterator(2, 3, 4), 5)

      (iter.toList must be equalTo(List(2, 3, 4, 3))) and
        (cnt must be greaterThan 0)
    }
    "handle if 'to' is +1 iterator length" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 patch(4, Iterator(1), 1)

      (iter.toList must be equalTo(List(1, 1, -1, 3, 1))) and
        (cnt must be greaterThan 0)
    }
    "close both if both CloseableIterators" in{
      var cnt0 = 0
      var cnt1 = 0
      val iter0 = counter(() => cnt0 += 1)
      val iter1 = counter(() => cnt1 += 1)

      iter0.patch(2, iter1, 2).toList
      (cnt0 must be greaterThan 0) and
        (cnt1 must be greaterThan 0)
    }
  }

  "scanLeft" should{
    "handle an empty iterator" in{
      val iter0 = nonCounter(Iterator.empty)
      val out = iter0.scanLeft(10)(_ + _)

      ((out size) must be equalTo(1)) //out must haveSize(1)
    }
    "call close" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.scanLeft(0)(_ + _).toList
      cnt must be greaterThan 0
    }
    "produce the right result" in{
      val iter0 = nonCounter()
      val iter = iter0.scanLeft(0)(_ + _)

      (iter.toList must beEqualTo (List(0, 1, 2, 1, 4)))
    }
  }

  "scanRight" should{ //sanity check
    "call close" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)

      iter0.scanRight(0)(_ + _).toList

      cnt must be greaterThan 0
    }
  }

  "zip" should{
    "handle an empty iterator" in{
      val iter0 = nonCounter(Iterator.empty)
      val iter = iter0 zip Iterator(1, 2, 3, 4)

      iter must beEmpty
    }
    "handle an empty iterator argument" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 zip Iterator.empty

      iter must beEmpty
    }
    "work with an iterator that is small" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1, Iterator(1, 2, 3, 4))
      val iter = iter0 zip Iterator(1, 2, 3, 4, 5)

      ((iter size) must be equalTo(4)) and //(iter must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
    "work with an argument iterator that is small" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1, Iterator(1, 2, 3, 4))
      val iter = iter0 zip Iterator(1, 2)

      ((iter size) must be equalTo(2)) and //(iter must haveSize(2)) and
        (cnt must be greaterThan 0)
    }
    "call close" in{
      var cnt0 = 0
      var cnt1 = 0
      val iter0 = counter(() => cnt0 += 1)
      val iter1 = counter(() => cnt1 += 1)

      (iter0 zip iter1).toList
      (cnt0 must be greaterThan 0) and
        (cnt1 must be greaterThan 0)
    }
  }

  "zipAll" should{
    "handle an empty iterator" in{
      val iter0 = nonCounter(Iterator.empty)
      val iter = iter0 zipAll (Iterator(1, 2, 3, 4), 1, 1)

      ((iter size) must be equalTo(4)) //iter must haveSize(4)
    }
    "handle an empty iterator argument" in{
      val iter0 = nonCounter()
      val iter = iter0 zipAll (Iterator.empty, 1, 1)

      ((iter size) must be equalTo(4))//iter must haveSize(4)
    }
    "work with an iterator that is small" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1, Iterator(1, 2, 3, 4))
      val iter = iter0 zipAll (Iterator(1, 2, 3, 4), 1, 1)

      ((iter size) must be equalTo(4)) and //(iter must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
    "work with an argument iterator that is small" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1, Iterator(1, 2, 3, 4))
      val iter = iter0 zipAll (Iterator(1, 2), 1, 1)

      ((iter size) must be equalTo(4)) and //(iter must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
    "call close" in{
      var cnt0 = 0
      var cnt1 = 0
      val iter0 = counter(() => cnt0 += 1)
      val iter1 = counter(() => cnt1 += 1)

      (iter0 zipAll (iter1, 1, 1)).toList
      (cnt0 must be greaterThan 0) and
        (cnt1 must be greaterThan 0)
    }
  }

  "zipWithIndex" should{
    "handle an empty iterator" in{
      val iter0 = nonCounter(Iterator.empty)
      val iter = iter0.zipWithIndex

      iter must beEmpty
    }
    "call close" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0.zipWithIndex

      ((iter size) must be equalTo(4)) and //(iter must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
  }

  "sliding" should{
    "call close after all elements have been traversed" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 sliding 2

      ((iter size) must be equalTo(3)) and //(iter must haveSize(3)) and
        (cnt must be greaterThan 0)
    }
    "work with an empty iterator" in{
      val iter0 = nonCounter(Iterator.empty)
      val iter = iter0 sliding 4

      iter must beEmpty //(iter must haveSize(0))
    }
  }

  "grouped" should{
    "call close after all elements have been traversed" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 grouped 4

      ((iter size) must be equalTo(1)) and //(iter must haveSize(1)) and
        (cnt must be greaterThan 0)
    }
    "segregate into expected groupings" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val iter = iter0 grouped 3

      ((iter size) must be equalTo(2))//iter must haveSize(2)
    }
    "handle empty iterators" in{
      val iter = nonCounter(Iterator.empty)

      iter must beEmpty
    }
  }

  "buffered" should{
    "handle empty iterators" in{
      val iter = nonCounter(Iterator.empty)

      iter.buffered must beEmpty
    }
    "throw an exception when head requested after exhaustion" in{
      val iter = nonCounter()
      val buffered = iter.buffered
      buffered.length
      buffered.head must throwA(new NoSuchElementException("next on empty iterator"))
    }
    "call close after all elements traversed" in{
      var cnt = 0
      val iter0 = counter(() => cnt += 1)
      val buffered = iter0.buffered

      ((buffered size) must be equalTo(4)) and //(buffered must haveSize(4)) and
        (cnt must be greaterThan 0)
    }
  }
}