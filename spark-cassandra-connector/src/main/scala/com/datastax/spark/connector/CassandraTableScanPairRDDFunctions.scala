package com.datastax.spark.connector

import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.rdd.partitioner.CassandraRDDPartitioner
import org.apache.spark.Partitioner

class CassandraTableScanPairRDDFunctions[K, V](rdd: CassandraTableScanRDD[(K, V)]) extends
  Serializable {

  /**
    * Use the [[CassandraRDDPartitioner]] from another [[CassandraTableScanRDD]] which
    * shares the same key type. All Partition Keys columns must also be present in the keys of
    * the target RDD.
    */
  def applyPartitionerFrom[X](
    thatRdd: CassandraTableScanRDD[(K, X)]): CassandraTableScanRDD[(K, V)] = {

    val partitioner = thatRdd.partitioner match {
      case Some(part: CassandraRDDPartitioner[K, _]) => part

      case Some(other: Partitioner) =>
        throw new IllegalArgumentException(s"Partitioner $other is not a CassandraRDDPartitioner")
      case None => throw new IllegalArgumentException(s"$thatRdd has no partitioner to apply")
    }
    applyPartitioner(partitioner)
  }

  /**
    * Use a specific [[CassandraRDDPartitioner]] to use with this PairRDD.
    */
  def applyPartitioner(
    partitioner: CassandraRDDPartitioner[K, _]): CassandraTableScanRDD[(K, V)] = {
    rdd.withPartitioner(Some(partitioner))
  }

}
