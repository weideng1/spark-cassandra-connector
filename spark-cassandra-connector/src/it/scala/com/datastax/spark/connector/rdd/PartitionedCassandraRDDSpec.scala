package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import scala.concurrent.Future
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition
import java.lang.{Integer => JInt}

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class PKey(key: Int)

case class PKeyCKey(key: Int, ckey: Int)

class PartitionedCassandraRDDSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf.set("spark.cassandra.input.consistency.level", "ONE"))

  val conn = CassandraConnector(defaultConf)
  val rowCount = 100

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""CREATE TABLE $ks.table1 (key INT, ckey INT, value INT, PRIMARY KEY (key, ckey)
             |)""".stripMargin)
        val ps = session.prepare( s"""INSERT INTO $ks.table1 (key, ckey, value) VALUES (?, ?, ?)""")
        val results = for (value <- 1 to rowCount) yield {
          session.executeAsync(ps.bind(value: JInt, value: JInt, value: JInt))
        }
        results.map(_.get)
      },
      Future {
        session.execute(
          s"""CREATE TABLE $ks.table2 (key INT, ckey INT, value INT, PRIMARY KEY
             |(key, ckey))""".stripMargin)
        val ps = session.prepare( s"""INSERT INTO $ks.table2 (key, ckey, value) VALUES (?, ?, ?)""")
        val results = for (value <- 1 to rowCount) yield {
          session.executeAsync(ps.bind(value: JInt, value: JInt, value: JInt))
        }
        results.map(_.get)
      }
    )
  }

  val customReadConf = ReadConf(splitCount = Some(20))

  val testRdd: CassandraTableScanRDD[CassandraRow] = sc.cassandraTable[CassandraRow](ks, "table1").withReadConf(customReadConf)

  def getPartitionMap[T](rdd: RDD[T]): Map[T, Int] = {
    rdd.mapPartitionsWithIndex{ case (index, it) =>
      it.map(row => (row, index)) }.collect.toMap
  }

  def checkPartitionerKeys[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): Unit = {
    rdd.partitioner.isDefined should be(true)
    val partitioner = rdd.partitioner.get
    val realPartitionMap = getPartitionMap(rdd.keys)
    for ((row, partId) <- realPartitionMap) {
      partitioner.getPartition(row) should be(partId)
    }
  }

  "A CassandraRDDPartitioner" should " be creatable from a generic CassandraTableRDD" in {
    val rdd = testRdd
    val partitioner = rdd.partitionGenerator.getPartitioner[PKey]
    partitioner.numPartitions should be(rdd.partitions.length)
  }

  it should " be created by keyBy with out any parameters" in {
    val keyedRdd = testRdd.keyBy[CassandraRow]
    checkPartitionerKeys(keyedRdd)
  }

  it should " be created by keyBy when selecting the Partition Key" in {
    val keyedRdd = testRdd.keyBy[CassandraRow](PartitionKeyColumns)
    checkPartitionerKeys(keyedRdd)
  }

  it should " be created by keyBy when the partition key is mapped to something else" in {
    val keyedRdd = testRdd.keyBy[CassandraRow](SomeColumns("key" as "notkey"))
    checkPartitionerKeys(keyedRdd)
  }

  it should " be created by keyBy with a case class" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    checkPartitionerKeys(keyedRdd)
  }

  it should " be created by keyBy with a case class with more than the Partition Key" in {
    val keyedRdd = testRdd.keyBy[PKeyCKey]
    checkPartitionerKeys(keyedRdd)
  }

  it should " not be created with a keyBy that does not cover the Partition Key" in {
    val keyedRdd = testRdd.keyBy[Tuple1[Int]](SomeColumns("ckey"))
    keyedRdd.partitioner.isEmpty should be(true)
  }

  "CassandraTableScanRDD " should " not have a partitioner by default" in {
    testRdd.partitioner should be(None)
  }


}
