package com.datastax.spark.connector.rdd.partitioner.dht

trait Token[T] extends Ordered[Token[T]] {
  def ord: Ordering[T]
  def value: T
}

case class LongToken(value: Long) extends Token[Long] {
  override def compare(that: Token[Long]) = value.compareTo(that.value)
  override def toString = value.toString

  override def ord: Ordering[Long] = implicitly[Ordering[Long]]
}

case class BigIntToken(value: BigInt) extends Token[BigInt] {
  override def compare(that: Token[BigInt]) = value.compare(that.value)
  override def toString = value.toString()

  override def ord: Ordering[BigInt] = implicitly[Ordering[BigInt]]
}


