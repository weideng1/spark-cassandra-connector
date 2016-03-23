package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import scala.concurrent.Future

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.rdd.partitioner.EndpointPartition

case class PKey(key: Int)

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
          s"""CREATE TABLE $ks.table1 (key INT, value SMALLINT, PRIMARY KEY (key))""")
        session.execute( s"""INSERT INTO $ks.table1 (key, value) VALUES (1,100)""")
        session.execute( s"""INSERT INTO $ks.table1 (key, value) VALUES (2,200)""")
        session.execute( s"""INSERT INTO $ks.table1 (key, value) VALUES (3,300)""")
      },
      Future {
        session.execute( s"""CREATE TABLE $ks.table2 (key INT, value INT, PRIMARY KEY (key))""")
        session.execute( s"""INSERT INTO $ks.table2 (key, value) VALUES (1,100)""")
        session.execute( s"""INSERT INTO $ks.table2 (key, value) VALUES (2,200)""")
        session.execute( s"""INSERT INTO $ks.table2 (key, value) VALUES (3,300)""")
      }
    )
  }
  "A CassandraRDDPartitioner" should " be creatable from a generic CassandraTableRDD" in {
    val rdd1 = sc.cassandraTable(ks, "table1")
    val partitioner = rdd1.partitionGenerator.getPartitioner[PKey]
    partitioner.numPartitions should be (rdd1.partitions.length)
    partitioner.getPartition(PKey(5)) should be(1)
  }

  "CassandraTableScanRDD " should " not have partitioners by default" in {
    val rdd1 = sc.cassandraTable(ks, "table1").partitioner should be(None)
  }


}
