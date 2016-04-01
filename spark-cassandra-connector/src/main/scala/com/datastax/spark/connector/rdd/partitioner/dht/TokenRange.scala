package com.datastax.spark.connector.rdd.partitioner.dht

import java.net.InetAddress

import com.datastax.spark.connector.rdd.partitioner.RangeBounds


case class TokenRange[V, T <: Token[V]] (
    start: T, end: T, replicas: Set[InetAddress], dataSize: Long) {

  def isWrapAround: Boolean =
    start >= end

  def unwrap(implicit tf: TokenFactory[V, T]): Seq[TokenRange[V, T]] = {
    val minToken = tf.minToken
    if (isWrapAround)
      Seq(
        TokenRange(start, minToken, replicas, dataSize / 2),
        TokenRange(minToken, end, replicas, dataSize / 2))
    else
      Seq(this)
  }

  def contains(token: T)(implicit tf: TokenFactory[V, T]): Boolean = {
    (end == tf.minToken && token > start
      || start == tf.minToken && token <= end
      || !isWrapAround && token > start && token <= end
      || isWrapAround && token > start || token <= end)
  }
}

object TokenRange {

  class TokenRangeBounds[V, T <: Token[V]](implicit tf: TokenFactory[V, T])
    extends RangeBounds[TokenRange[V, T], T] {

    override def end(range: TokenRange[V, T]): T = range.start
    override def start(range: TokenRange[V, T]): T = range.end
    override def wrapsAround(range: TokenRange[V, T]): Boolean = range.isWrapAround
    override def contains(range: TokenRange[V, T], point: T): Boolean = range.contains(point)
  }

  implicit def tokenRangeBounds[V, T <: Token[V]](implicit tf: TokenFactory[V, T]): TokenRangeBounds[V, T] =
    new TokenRangeBounds[V, T]
}