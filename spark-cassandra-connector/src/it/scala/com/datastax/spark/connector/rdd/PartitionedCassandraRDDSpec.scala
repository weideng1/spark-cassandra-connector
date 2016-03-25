package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import scala.concurrent.Future

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition

case class PKey(key: Int)

case class PKeyCKey(key: Int, ckey: Int)

class PartitionedCassandraRDDSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(defaultConf.set("spark.cassandra.input.consistency.level", "ONE"))

  val conn = CassandraConnector(defaultConf)
  val bigTableRowCount = 100000

  conn.withSessionDo { session =>
    createKeyspace(session)

    awaitAll(
      Future {
        session.execute(
          s"""CREATE TABLE $ks.table1 (key INT, ckey INT, value SMALLINT, PRIMARY KEY (key, ckey)
             |)""".stripMargin)
        session.execute( s"""INSERT INTO $ks.table1 (key, ckey, value) VALUES (1, 1, 100)""")
        session.execute( s"""INSERT INTO $ks.table1 (key, ckey, value) VALUES (2, 2, 200)""")
        session.execute( s"""INSERT INTO $ks.table1 (key, ckey, value) VALUES (3, 3, 300)""")
      },
      Future {
        session.execute(
          s"""CREATE TABLE $ks.table2 (key INT, ckey INT, value INT, PRIMARY KEY
             |(key, ckey))""".stripMargin)
        session.execute( s"""INSERT INTO $ks.table2 (key, ckey, value) VALUES (1, 1, 100)""")
        session.execute( s"""INSERT INTO $ks.table2 (key, ckey, value) VALUES (2, 2, 200)""")
        session.execute( s"""INSERT INTO $ks.table2 (key, ckey, value) VALUES (3, 3, 300)""")
      }
    )
  }

  val expectedKey = 5
  val expectedPartition = 0

  "A CassandraRDDPartitioner" should " be creatable from a generic CassandraTableRDD" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val partitioner = rdd1.partitionGenerator.getPartitioner[PKey]
    partitioner.numPartitions should be(rdd1.partitions.length)
    partitioner.getPartition(PKey(expectedKey)) should be(expectedPartition)
  }

  it should " be created by keyBy with out any parameters" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val keyedRdd = rdd1.keyBy[CassandraRow]
    keyedRdd.partitioner.isDefined should be(true)
    val partitioner = keyedRdd.partitioner.get
    partitioner
      .getPartition(
        new CassandraRow(IndexedSeq("key"),
          IndexedSeq(expectedKey: java.lang.Integer))) should be(expectedPartition)
  }

  it should " be created by keyBy when selecting the Partition Key" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val keyedRdd = rdd1.keyBy[CassandraRow](PartitionKeyColumns)
    keyedRdd.partitioner.isDefined should be(true)
    val partitioner = keyedRdd.partitioner.get
    partitioner
      .getPartition(
        new CassandraRow(IndexedSeq("key"),
          IndexedSeq(expectedKey: java.lang.Integer))) should be(expectedPartition)
  }

  it should " be created by keyBy when the partition key is mapped to something else" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val keyedRdd = rdd1.keyBy[CassandraRow](SomeColumns("key" as "notkey"))
    keyedRdd.partitioner.isDefined should be(true)
    val partitioner = keyedRdd.partitioner.get
    partitioner
      .getPartition(
        new CassandraRow(IndexedSeq("key"),
          IndexedSeq(expectedPartition: java.lang.Integer))) should be(expectedPartition)
  }

  it should " be created by keyBy with a case class" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val keyedRdd = rdd1.keyBy[PKey](SomeColumns("key"))
    keyedRdd.partitioner.isDefined should be(true)
    val partitioner = keyedRdd.partitioner.get
    partitioner.getPartition(PKey(expectedKey)) should be(expectedPartition)
  }

  it should " be created by keyBy with a case class with more than the Partition Key" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val keyedRdd = rdd1.keyBy[PKeyCKey](PartitionKeyColumns)
    keyedRdd.partitioner.isDefined should be(true)
    val partitioner = keyedRdd.partitioner.get
    partitioner
      .getPartition(PKeyCKey(expectedKey, expectedKey)) should be(expectedPartition)
  }

  it should " not be created with a keyBy that does not cover the Partition Key" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val keyedRdd = rdd1.keyBy[Tuple1[Int]](SomeColumns("ckey"))
    keyedRdd.partitioner.isEmpty should be(true)
  }

  "CassandraTableScanRDD " should " not have a partitioner by default" in {
    val rdd1 = sc.cassandraTable(ks, "table1").partitioner should be(None)
  }


}
