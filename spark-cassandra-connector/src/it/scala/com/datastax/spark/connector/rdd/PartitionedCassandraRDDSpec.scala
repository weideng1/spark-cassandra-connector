package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import scala.concurrent.Future
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import java.lang.{Integer => JInt}

import org.apache.spark.HashPartitioner
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
          session.executeAsync(ps.bind(value: JInt, value: JInt, (rowCount - value): JInt))
        }
        results.map(_.get)
      }
    )
  }

  // Make sure that all tests have enough partitions to make things interesting
  val customReadConf = ReadConf(splitCount = Some(20))

  val testRdd = sc.cassandraTable(ks, "table1").withReadConf(customReadConf)
  val joinTarget = sc.cassandraTable(ks, "table2")

  def getPartitionMap[T](rdd: RDD[T]): Map[T, Int] = {
    rdd.mapPartitionsWithIndex { case (index, it) =>
      it.map(row => (row, index))
    }.collect.toMap
  }

  def checkPartitionerKeys[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): Unit = {
    rdd.partitioner shouldBe defined
    val partitioner = rdd.partitioner.get
    val realPartitionMap = getPartitionMap(rdd.keys)
    for ((row, partId) <- realPartitionMap) {
      partitioner.getPartition(row) should be(partId)
    }
  }

  "A CassandraRDDPartitioner" should " be creatable from a generic CassandraTableRDD" in {
    val rdd = testRdd
    val partitioner = rdd.partitionGenerator.getPartitioner[PKey]
    partitioner.get.numPartitions should be(rdd.partitions.length)
  }

  "keyBy" should "create a partitioned RDD without any parameters" in {
    val keyedRdd = testRdd.keyBy[CassandraRow]
    checkPartitionerKeys(keyedRdd)
  }

  it should "create a partitioned RDD selecting the Partition Key" in {
    val keyedRdd = testRdd.keyBy[CassandraRow](PartitionKeyColumns)
    checkPartitionerKeys(keyedRdd)
  }

  it should "create a partitioned RDD when the partition key is mapped to something else" in {
    val keyedRdd = testRdd.keyBy[CassandraRow](SomeColumns("key" as "notkey"))
    checkPartitionerKeys(keyedRdd)
  }

  it should "create a partitioned RDD with a case class" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    checkPartitionerKeys(keyedRdd)
  }

  it should "create a partitioned RDD with a case class with more than the Partition Key" in {
    val keyedRdd = testRdd.keyBy[PKeyCKey]
    checkPartitionerKeys(keyedRdd)
  }

  it should "NOT create a partitioned RDD that does not cover the Partition Key" in {
    val keyedRdd = testRdd.keyBy[Tuple1[Int]](SomeColumns("ckey"))
    keyedRdd.partitioner.isEmpty should be(true)
  }

  "CassandraTableScanRDD " should " not have a partitioner by default" in {
    testRdd.partitioner should be(None)
  }

  it should " be able to be assigned a partititoner from RDD with the same key" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    val otherRdd = joinTarget.keyBy[PKey](SomeColumns("key")).applyPartitionerFrom(keyedRdd)
    checkPartitionerKeys(otherRdd)
  }

  it should " be joinable against an RDD without a partitioner" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    val joinedRdd = keyedRdd.join(sc.parallelize(1 to rowCount).map(x => (PKey(x), -x)))
    val results = joinedRdd.values.collect
    results should have length (rowCount)
    for (row <- results) {
      row._1.getInt("key") should be(-row._2)
    }
  }

  it should "not shuffle during a join with an RDD with the same partitioner" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    val otherRdd = joinTarget.keyBy[PKey](SomeColumns("key")).applyPartitionerFrom(keyedRdd)
    val joinRdd = keyedRdd.join(otherRdd)
    joinRdd.toDebugString should not contain ("+-")
    // "+-" in the debug string means there is more than 1 stage and thus a shuffle
  }

  it should "correctly join against an RDD with the same partitioner" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    val otherRdd = joinTarget.keyBy[PKey](SomeColumns("key")).applyPartitionerFrom(keyedRdd)
    val joinRdd = keyedRdd.join(otherRdd)
    val results = joinRdd.values.collect()
    results should have length (rowCount)
    for (row <- results) {
      row._1.getInt("key") should be(row._2.getInt("key"))
    }
  }

  it should "not shuffle in a keyed selfjoin" in {
    val keyedRdd = testRdd.keyBy[PKey](SomeColumns("key"))
    val joinRdd = keyedRdd.join(keyedRdd)
    val results = joinRdd.values.collect()
    results should have length (rowCount)
    joinRdd.toDebugString should not contain ("+-")
    for (row <- results) {
      row._1.getInt("key") should be(row._2.getInt("key"))
    }
  }

  "CassandraTableScanPairRDDFunctions" should "not apply an empty partitioner" in {
    val keyedRdd = testRdd.keyBy[Tuple1[Int]](SomeColumns("ckey"))
    intercept[IllegalArgumentException] {
      joinTarget.keyBy[Tuple1[Int]](SomeColumns("key")).applyPartitionerFrom(keyedRdd)
    }
  }

}
