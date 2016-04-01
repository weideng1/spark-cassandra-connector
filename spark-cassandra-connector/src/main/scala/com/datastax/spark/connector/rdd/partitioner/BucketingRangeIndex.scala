package com.datastax.spark.connector.rdd.partitioner

import scala.collection.mutable.ArrayBuffer

import com.datastax.spark.connector.rdd.partitioner.dht.Token

/** A mapping from T values to an integer range [0, n),
  * such that for any (t1: T) > (t2: T), bucket(t1) >= bucket(t2). */
trait MonotonicBucketing[-T] {
  def bucket(n: Int): T => Int
}

object MonotonicBucketing {

  val lnOf2 = scala.math.log(2)
  def log2(x: Double) = scala.math.log(x) / lnOf2

  implicit object IntBucketing extends MonotonicBucketing[Int] {
    override def bucket(n: Int): Int => Int = {
      val shift = 31 - log2(n).toInt
      x => (x / 2 - Int.MinValue / 2) >> shift
    }
  }

  implicit object LongBucketing extends MonotonicBucketing[Long] {
    override def bucket(n: Int): Long => Int = {
      val shift = 63 - log2(n).toInt
      x => ((x / 2 - Long.MinValue / 2) >> shift).toInt
    }
  }
}

/** Extracts rangeBounds of a range R.
  * This is to allow working with any representation of ranges.
  * The range must not wrap, that is `end` >= `start`. */
trait RangeBounds[-R, T] {
  def start(range: R): T
  def end(range: R): T
  def contains(range: R, point: T): Boolean
  def wrapsAround(range: R): Boolean
}

/** A special structure for fast lookup of ranges containing given point.*/
class BucketingRangeIndex[R, T](ranges: Seq[R])
  (implicit bounds: RangeBounds[R, T], ordering: Ordering[T], bucketing: MonotonicBucketing[T]) {

  private val sizeLog = MonotonicBucketing.log2(ranges.size).toInt + 1
  private val size = math.pow(2, sizeLog).toInt
  private val table = Array.fill(size)(new ArrayBuffer[R])
  private val bucket: T => Int = bucketing.bucket(size)

  private def add(r: R, startHash: Int, endHash: Int): Unit = {
    var i = startHash
    while (i <= endHash) {
      table(i) += r
      i += 1
    }
  }

  for (r <- ranges) {
    val startBucket = bucket(bounds.start(r))
    val endBucket = bucket(bounds.end(r))
    val wrapsAround = bounds.wrapsAround(r)
    if (wrapsAround && startBucket == endBucket)
      add(r, 0, size - 1)
    else if (wrapsAround) {
      add(r, startBucket, size - 1)
      add(r, 0, endBucket)
    }
    else
      add(r, startBucket, endBucket)
  }

  /** Finds ranges containing given point in O(1) time. */
  def ranges(point: T): IndexedSeq[R] =
    table(bucket(point)).filter(bounds.contains(_, point))
}
