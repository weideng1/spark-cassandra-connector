package com.datastax.spark.connector.rdd.partitioner

import scala.util.Random

import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import com.datastax.spark.connector.rdd.partitioner.dht.{BigIntToken, LongToken, Token}

class RangeIndexSpec extends FlatSpec with PropertyChecks {

  private val clusterSize = 1000 * 16

  private implicit object TupleBounds extends RangeBounds[(Int, Int), Int] {
    override def start(range: (Int, Int)): Int =
      range._1
    override def end(range: (Int, Int)): Int =
      range._2
    override def wrapsAround(range: (Int, Int)): Boolean =
      range._2 <= range._1
    override def contains(range: (Int, Int), point: Int): Boolean =
      if (wrapsAround(range))
        point >= range._1 || point < range._2
      else
        point >= range._1 && point < range._2
  }

  /** Creates non overlapping random ranges */
  private def randomRanges(n: Int): Seq[(Int, Int)] = {
    val randomInts =
      (Int.MinValue +: Iterator.continually(Random.nextInt()).take(n).toSeq :+ Int.MaxValue)
        .distinct
        .sortBy(identity)
    val randomRanges = for (Seq(a, b) <- randomInts.sliding(2).toSeq) yield (a, b)
    randomRanges
  }

  "BucketingRangeIndex" should "find ranges containing given point when ranges do not overlap" in {
    val index = new BucketingRangeIndex(randomRanges(clusterSize))
    for (x <- Iterator.continually(Random.nextInt()).take(100000)) {
      val containingRanges = index.ranges(x)
      assert(containingRanges.size == 1)
      assert(x >= containingRanges.head._1)
      assert(x < containingRanges.head._2)
    }
  }

  it should "find ranges containing given point when ranges do overlap" in {
    val index = new BucketingRangeIndex(
      randomRanges(clusterSize) ++ randomRanges(clusterSize) ++ randomRanges(clusterSize))
    for (x <- Iterator.continually(Random.nextInt()).take(100000)) {
      val containingRanges = index.ranges(x)
      assert(containingRanges.size == 3)
      assert(containingRanges forall (x >= _._1))
      assert(containingRanges forall (x < _._2))
    }
  }

  it should "find proper ranges when they wrap around" in {
    val range1 = (0, 0)                         // full range
    val range2 = (Int.MaxValue, Int.MinValue)   // only MaxValue included
    val range3 = (Int.MaxValue, 0)              // lower half
    val index = new BucketingRangeIndex(Seq(range1, range2, range3))
    assert(index.ranges(0) == Seq(range1))
    assert(index.ranges(Int.MaxValue) == Seq(range1, range2, range3))
    assert(index.ranges(Int.MaxValue / 2) == Seq(range1))
    assert(index.ranges(Int.MinValue) == Seq(range1, range3))
    assert(index.ranges(Int.MinValue / 2) == Seq(range1, range3))
  }

  def randomBigIntToken(): BigIntToken =
    BigIntToken(BigInt(127, Random).abs)

  val bigIntTokens = Gen.const(1).map(_ => randomBigIntToken())
  val longTokens = Gen.choose(Long.MinValue, Long.MaxValue).map(LongToken.apply)
  val positiveIntegers = Gen.choose(1, Int.MaxValue)

  "LongTokenBucketing" should "respect the required bucket range" in {
    val bucketing = implicitly[MonotonicBucketing[LongToken]]
    forAll(longTokens, positiveIntegers) { (t: LongToken, n: Int) =>
      val b = bucketing.bucket(n)(t)
      assert(b >= 0)
      assert(b < n)
    }
  }

  it should "be weakly monotonic" in {
    val bucketing = implicitly[MonotonicBucketing[LongToken]]
    forAll(longTokens, longTokens, positiveIntegers) { (t1: LongToken, t2: LongToken, n: Int) =>
      val b1 = bucketing.bucket(n)(t1)
      val b2 = bucketing.bucket(n)(t2)
      assert(t1 == t2 && b1 == b2 || t1 < t2 && b1 <= b2 || t1 > t2 && b1 >= b2)
    }
  }

  "BigIntTokenBucketing" should "respect the required bucket range" in {
    val bucketing = implicitly[MonotonicBucketing[BigIntToken]]
    forAll(bigIntTokens, positiveIntegers) { (t: BigIntToken, n: Int) =>
      val b = bucketing.bucket(n)(t)
      assert(b >= 0)
      assert(b < n)
    }
  }

  it should "be weakly monotonic" in {
    val bucketing = implicitly[MonotonicBucketing[BigIntToken]]
    forAll(bigIntTokens, bigIntTokens, positiveIntegers) { (t1: BigIntToken, t2: BigIntToken, n: Int) =>
      val b1 = bucketing.bucket(n)(t1)
      val b2 = bucketing.bucket(n)(t2)
      assert(t1 == t2 && b1 == b2 || t1 < t2 && b1 <= b2 || t1 > t2 && b1 >= b2)
    }
  }
}
