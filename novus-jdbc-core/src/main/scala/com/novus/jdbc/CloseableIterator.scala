package com.novus.jdbc

import collection.GenTraversableOnce
import annotation.tailrec

//TODO: documentation
//TODO: test several methods chained together against closing
//TODO: sliding, grouped, perhaps a GroupedIterator
//TODO: scanRight
abstract class CloseableIterator[+A] extends Iterator[A]{
  self =>

  override def buffered = new CloseableIterator[A] with BufferedIterator[A]{
    private var hd: A = _
    private var hdDefined: Boolean = false

    def head: A = {
      if (!hdDefined) {
        hd = next()
        hdDefined = true
      }
      hd
    }

    def hasNext = hdDefined || self.hasNext

    def next() = if (hdDefined) {
      hdDefined = false
      hd
    }
    else self next ()

    def close(){
      self close ()
    }
  }

  /** Returns the number of elements in this iterator.
   *  $willNotTerminateInf
   *
   *  @note Reuse: $consumesIterator
   */
  override def length ={
    val output = super.length
    close()
    output
  }

  override def map[B](f: A => B) = new CloseableIterator[B] {
    def hasNext = self.hasNext

    def next() = f(self.next())

    def close(){
      self close ()
    }
  }

  override def flatMap[B](f: A => GenTraversableOnce[B]): Iterator[B] = new CloseableIterator[B] {
    private var cur: Iterator[B] = Iterator.empty

    @tailrec final def hasNext = cur.hasNext || self.hasNext && {
      cur = f(self next ()).toIterator
      hasNext
    }

    def next(): B = if(hasNext){
      val output = cur.next()
      if(!self.hasNext) close()
      output
    }
    else Iterator.empty.next()

    def close(){
      self close ()
    }
  }

  override def filter(pred: A => Boolean) = new CloseableIterator[A] {
    private var hd: A = _
    private var hdDefined = !self.hasNext

    def hasNext = {
      while(!hdDefined && self.hasNext) {
        hd = self next ()
        hdDefined = pred(hd)
      }

      hdDefined
    }

    def next() = if (hasNext){
      val output = hd
      hdDefined = false
      output
    }
    else Iterator.empty.next()

    def close(){
      self close ()
    }
  }

  override def collect[B](pf: PartialFunction[A, B]) = filter(pf isDefinedAt) map pf

  override def collectFirst[B](pf: PartialFunction[A, B]) = find(pf isDefinedAt) map pf

  //TODO: test that close is called under all circumstances
  override def slice(from: Int, to: Int) ={
    require(from <= to, "The sliced range must have a lower bound less than or equal to the upper bound.")

    var cnt = from
    while(cnt > 0 && self.hasNext){
      self next ()
      cnt -= 1
    }
    if(!self.hasNext) self close()

    new CloseableIterator[A]{
      private var until = to

      override def hasNext = self.hasNext && 0 <= until

      override def next() = if(hasNext){
        until -= 1
        val output = self next ()
        if(until < 0 || !self.hasNext) close()
        output
      }
      else Iterator.empty next ()

      def close(){
        self close ()
      }
    }
  }

  override def takeWhile(pred: A => Boolean) = new CloseableIterator[A] {
    private var hd: A = _
    private var hdDefined = self.hasNext

    def hasNext = {
      if(hdDefined && self.hasNext) {
        hd = self next ()
        hdDefined = pred(hd)
        if(!(hdDefined && self.hasNext)) close()
      }

      hdDefined
    }

    def next() = if (hasNext){
      val output = hd
      hdDefined = false
      output
    } else Iterator.empty.next()

    def close(){
      self close ()
    }
  }

  override def exists(pred: A => Boolean) ={
    val output = super.exists(pred)
    close()
    output
  }

  override def find(pred: A => Boolean) ={
    val output = super.find(pred)
    close()
    output
  }

  override def indexOf[B >: A](elem: B) ={
    val output = super.indexOf(elem)
    close()
    output
  }

  override def indexWhere(pred: A => Boolean) ={
    val output = super.indexWhere(pred)
    close()
    output
  }

  override def contains(elem: Any) ={
    val output = super.contains(elem)
    close()
    output
  }

  //TODO: test that close is called under all circumstances and combinations (i.e. CloseableIterator args)
  override def patch[B >: A](from: Int, patchElems: Iterator[B], replaced: Int) = new CloseableIterator[B] {
    private var indx = 0
    private val to = from + replaced

    def hasNext = if (indx < from || to <= indx) self.hasNext else patchElems.hasNext

    def next() ={
      val result = if (indx < from && to <= indx && self.hasNext){
        self next ()
      }
      else if(from <= indx && indx < to){
        if(self.hasNext) self next ()
        patchElems next ()
      }
      else Iterator.empty.next()

      indx += 1
      result
    }

    def close() {
      close(patchElems)
      self close ()
    }
  }

  /** Partitions this iterator into two iterators according to a predicate.
   *
   *  @param pred the predicate on which to partition
   *  @return  a pair of iterators: the iterator that satisfies the predicate `pred` and the iterator that does not. The
   *           relative order of the elements in the resulting iterators is the same as in the original iterator.
   *  @note    Reuse: $consumesOneAndProducesTwoIterators
   */
  override def partition(pred: A => Boolean) ={
    val output = self.toList.toIterator partition pred
    close()
    output
  }

  /** Splits this Iterator into a prefix/suffix pair according to a predicate.
   *
   *  @param pred the test predicate
   *  @return  a pair of Iterators consisting of the longest prefix of this whose elements all satisfy `pred`, and the
   *           rest of the Iterator.
   *  @note    Reuse: $consumesOneAndProducesTwoIterators
   */
  override def span(pred: A => Boolean) ={
    val output = self.toList.toIterator span pred
    close()
    output
  }

  /** Creates two new iterators that both iterate over the same elements
   *  as this iterator (in the same order).  The duplicate iterators are considered equal if they are positioned at the
   *  same element.
   *
   *  Given that most methods on iterators will make the original iterator unfit for further use, this methods provides
   *  a reliable way of calling multiple such methods on an iterator.
   *
   *  @return a pair of iterators
   *  @note   The implementation may allocate temporary storage for elements iterated by one iterator but not yet by the
   *          other.
   *  @note   Reuse: $consumesOneAndProducesTwoIterators
   */
  override def duplicate ={
    val output = self.toList.toIterator.duplicate
    close()
    output
  }

  /** Produces a collection containing cummulative results of applying the operator going left to right.
   *
   *  $willNotTerminateInf
   *  $orderDependent
   *
   *  @tparam B      the type of the elements in the resulting collection
   *  @param z       the initial value
   *  @param op      the binary operator applied to the intermediate result and the element
   *  @return        iterator with intermediate results
   *  @note          Reuse: $consumesAndProducesIterator
   */
  override def scanLeft[B](z: B)(op: (B,A) => B) = new CloseableIterator[B] {
    var hasNext = true
    var elem = z

    def next() = if(hasNext){
      val output = elem
      if(self.hasNext) elem = op(elem, self next ())
      else{
        hasNext = false
        close()
      }
      output
    }
    else Iterator.empty next ()

    def close() {
      self close ()
    }
  }

  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int) {
    super.copyToArray(xs, start, len)
    close()
  }

  override def copyToArray[B >: A](xs: Array[B]){
    super.copyToArray(xs)
    close()
  }

  override def copyToArray[B >: A](xs: Array[B], start: Int){
    super.copyToArray(xs, start)
    close()
  }

  //TODO: test that close is called under all circumstances
  override def zip[B](that: Iterator[B]): Iterator[(A, B)] = new CloseableIterator[(A, B)] {
    def hasNext = self.hasNext && that.hasNext

    def next = if(hasNext){
      val output = (self next (), that next ())
      if(!hasNext) close()
      output
    }
    else Iterator.empty.next()

    def close(){
      close(that)
      self close ()
    }
  }

  //TODO: test that close is called under all circumstances
  override def zipAll[B, A1 >: A, B1 >: B](that: Iterator[B], thisElem: A1, thatElem: B1): Iterator[(A1, B1)] =
    new CloseableIterator[(A1, B1)] {
      def hasNext = self.hasNext || that.hasNext

      def next() ={
        val output = if (self.hasNext) {
          if (that.hasNext) (self next (), that next ())
          else (self next (), thatElem)
        }
        else if(that.hasNext){
          (thisElem, that next ())
        }
        else Iterator.empty.next()

        if(!hasNext) close()
        output
      }

      def close(){
        close(that)
        self close ()
      }
    }

  override def zipWithIndex = new CloseableIterator[(A, Int)] {
    var idx = 0 //This is public because it's public in Scala source code!

    def hasNext = self.hasNext

    def next = {
      val ret = (self next (), idx)
      idx += 1
      ret
    }

    def close(){
      self close ()
    }
  }

  override def padTo[B >: A](len: Int, elem: B) = new CloseableIterator[B] {
    private var count = 0

    def hasNext = self.hasNext || count < len

    def next() = {
      count += 1
      if (self.hasNext) self next ()
      else if (count <= len) elem
      else Iterator.empty.next()
    }

    def close(){
      self close ()
    }
  }

  override def sameElements(that: Iterator[_]) ={
    val output = super.sameElements(that)
    close()
    close(that)
    output
  }

  protected[jdbc] def close(that: Iterator[_]){
    that match{
      case x:CloseableIterator[_] => x close ()
      case _ =>
    }
  }

//This Scala 2.10 and above.
//  override def corresponds[B](that: GenTraversableOnce[B])(pred: (A, B) => Boolean) ={
//    val output = super.corresponds(that)(pred)
//    close(that)
//    close()
//    output
//  }

  def close()

  /** This is defined as an unreliable fail-safe mechanism for closing the connection and returning it to the pool. */
  override def finalize(){
    close()
  }
}