package com.datastax.spark.connector.rdd.partitioner.dht

import com.datastax.spark.connector.rdd.partitioner.MonotonicBucketing
import com.datastax.spark.connector.rdd.partitioner.MonotonicBucketing.LongBucketing

trait Token[T] extends Ordered[Token[T]] {
  def ord: Ordering[T]
  def value: T
}



case class LongToken(value: Long) extends Token[Long] {
  override def compare(that: Token[Long]) = value.compareTo(that.value)
  override def toString = value.toString
  override def ord: Ordering[Long] = implicitly[Ordering[Long]]
}

object LongToken {
  implicit object LongTokenBucketing extends MonotonicBucketing[Token[Long]] {
    override def bucket(n: Int): Token[Long] => Int = {
      val longBucket = LongBucketing.bucket(n)
      x => longBucket(x.value)
    }
  }
}

case class BigIntToken(value: BigInt) extends Token[BigInt] {
  override def compare(that: Token[BigInt]) = value.compare(that.value)
  override def toString = value.toString()

  override def ord: Ordering[BigInt] = implicitly[Ordering[BigInt]]
}

object BigIntToken {
  implicit object BigIntTokenBucketing extends MonotonicBucketing[Token[BigInt]] {
    override def bucket(n: Int): Token[BigInt] => Int = {
      val shift = 127 - MonotonicBucketing.log2(n).toInt
      def clamp(x: BigInt): BigInt = if (x == BigInt(-1)) BigInt(0) else x
      x => (clamp(x.value) >> shift).toInt
    }
  }
}

