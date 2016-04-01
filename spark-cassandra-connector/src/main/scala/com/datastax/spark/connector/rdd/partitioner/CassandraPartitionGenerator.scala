package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.existentials

import org.apache.spark.{Logging, Partitioner}

import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import com.datastax.spark.connector.{ColumnSelector, PartitionKeyColumns}
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.writer.RowWriterFactory
import scala.reflect.ClassTag
import scala.util.Try

/**
  * A [[org.apache.spark.Partitioner]] implementation which performs the inverse
  * operation of a traditional C* hashing. Requires the Key type and the token
  * value type V.
  *
  * Will take objects of type Key and determine given the token ragnes in `indexedRanges`
  * which range the Key would belong in given the C* schema in `TableDef`
  *
  * Under the hood uses a bound statement to generate routing keys which are then
  * used the driver's internal token factories to determine the token for the
  * routing key.
  */
private[connector] class CassandraPartitioner[Key : ClassTag, V, T <: Token[V]](
  private[connector] val connector: CassandraConnector,
  private[connector] val tableDef: TableDef,
  private[connector] val partitions: Seq[CassandraPartition[V, T]],
  val keyMapping: ColumnSelector = PartitionKeyColumns)(
implicit
  @transient
  rwf: RowWriterFactory[Key],
  tokenFactory: TokenFactory[V, T]) extends Partitioner with Logging {

  /** Changes the tableDef target of this partitioner. Can only be done within a keyspace
    * verification of key mapping will occur with the call to [[verify()]] */
  def withTableDef(tableDef: TableDef): CassandraPartitioner[Key, V, T] = {
    if (tableDef.keyspaceName != this.tableDef.keyspaceName) {
      throw new IllegalArgumentException(
        s"""Cannot apply partitioner from keyspace
           |${this.tableDef.keyspaceName} to table
           |${tableDef.keyspaceName}.${tableDef.tableName} because the keyspaces do
           |not match""".stripMargin)
    }
    new CassandraPartitioner[Key, V, T](connector, tableDef, partitions, keyMapping)
  }

  /** Changes the current key mapping for this partitioner. Verification of the mapping
    * occurs on call to [[verify()]] */
  def withKeyMapping(keyMapping: ColumnSelector): CassandraPartitioner[Key, V, T] =
    new CassandraPartitioner[Key, V, T](connector, tableDef, partitions, keyMapping)


  private lazy val partitionKeyNames =
    PartitionKeyColumns.selectFrom(tableDef).map(_.columnName).toSet

  private lazy val partitionKeyMapping = keyMapping
    .selectFrom(tableDef)
    .filter( colRef => partitionKeyNames.contains(colRef.columnName))

  private lazy val partitionKeyWriter = {
    logDebug(
      s"""Building Partitioner with mapping
         |${partitionKeyMapping.map(x => (x.columnName, x.selectedAs))}
         |for table $tableDef""".stripMargin)
    implicitly[RowWriterFactory[Key]]
      .rowWriter(tableDef, partitionKeyMapping)
  }

  /** Builds and makes sure we can make a rowWriter with the current TableDef and keyMapper */
  def verify(log: Boolean = true): Unit = {
    val attempt = Try(partitionKeyWriter)
    if (attempt.isFailure) {
      if (log)
        logError("Unable to build partition key writer CassandraPartitioner.", attempt.failed.get)
      throw attempt.failed.get
    }
  }

  /** Since the Token Generator relies on a (non-serializable) prepared statement we need to
    * make sure it is not serialized to executors and is made fresh on each executor */
  @transient
  private lazy val tokenGenerator =
    new TokenGenerator(connector, tableDef, partitionKeyWriter)

  case class IndexedTokenRange(index: Int, range: TokenRange[V, T])

  @transient
  private lazy val indexedTokenRanges: Seq[IndexedTokenRange] =
    for (p <- partitions; tr <- p.tokenRanges) yield
      IndexedTokenRange(p.index, tr.range)

  @transient
  private lazy val tokenRangeLookupTable: BucketingRangeIndex[IndexedTokenRange, T] = {

    implicit val rangeBounds = new RangeBounds[IndexedTokenRange, T] {
      override def start(range: IndexedTokenRange): T = range.range.start
      override def end(range: IndexedTokenRange): T = range.range.end
      override def wrapsAround(range: IndexedTokenRange): Boolean = range.range.isWrapAround
      override def contains(range: IndexedTokenRange, point: T): Boolean = range.range.contains(point)
    }

    implicit val tokenOrdering = tokenFactory.tokenOrdering
    implicit val tokenBucketing = tokenFactory.tokenBucketing

    new BucketingRangeIndex[IndexedTokenRange, T](indexedTokenRanges)
  }

  override def getPartition(key: Any): Int = {
    key match {
      case x: Key =>
        val driverToken = tokenGenerator.getTokenFor(x)
        val connectorToken = tokenFactory.tokenFromString(driverToken.getValue.toString)
        tokenRangeLookupTable.ranges(connectorToken).head.index
      case other =>
        throw new IllegalArgumentException(s"Couldn't determine the key from object $other")
    }
  }

  override def numPartitions: Int =
    partitions.length

  override def equals(that: Any): Boolean = that match {
    case that: CassandraPartitioner[Key, V, T] =>
      (this.indexedTokenRanges == that.indexedTokenRanges
        && this.tableDef.keyspaceName == that.tableDef.keyspaceName
        && this.connector == that.connector)
    case _ =>
      false
  }

  override def hashCode: Int = {
    partitions.hashCode() + tableDef.keyspaceName.hashCode * 31
  }

}
/** Creates CassandraPartitions for given Cassandra table */
class CassandraPartitionGenerator[V, T <: Token[V]](
    connector: CassandraConnector,
    tableDef: TableDef,
    splitCount: Option[Int],
    splitSize: Long)(
  implicit
    tokenFactory: TokenFactory[V, T]) extends Logging{

  type Token = com.datastax.spark.connector.rdd.partitioner.dht.Token[T]
  type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[V, T]

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName

  private val totalDataSize: Long = {
    // If we know both the splitCount and splitSize, we should pretend the total size of the data is
    // their multiplication. TokenRangeSplitter will try to produce splits of desired size, and this way
    // their number will be close to desired splitCount. Otherwise, if splitCount is not set,
    // we just go to C* and read the estimated data size from an appropriate system table
    splitCount match {
      case Some(c) => c * splitSize
      case None => new DataSizeEstimates(connector, keyspaceName, tableName).dataSizeInBytes
    }
  }

  def tokenRange(range: DriverTokenRange, metadata: Metadata): TokenRange = {
    val startToken = tokenFactory.tokenFromString(range.getStart.getValue.toString)
    val endToken = tokenFactory.tokenFromString(range.getEnd.getValue.toString)
    val replicas = metadata.getReplicas(Metadata.quote(keyspaceName), range).map(_.getAddress).toSet
    val dataSize = (tokenFactory.ringFraction(startToken, endToken) * totalDataSize).toLong
    new TokenRange(startToken, endToken, replicas, dataSize)
  }

  def fullRange: TokenRange = {

    val replicas = connector
      .withClusterDo{cluster => cluster.getMetadata}
      .getAllHosts
      .map(_.getAddress)
      .toSet

    new TokenRange(
      tokenFactory.minToken,
      tokenFactory.maxToken,
      replicas,
      0)
  }

  private def describeRing: Seq[TokenRange] = {
    connector.withClusterDo { cluster =>
      val metadata = cluster.getMetadata
      for (tr <- metadata.getTokenRanges.toSeq) yield tokenRange(tr, metadata)
    }
  }

  private def splitsOf(
      tokenRanges: Iterable[TokenRange],
      splitter: TokenRangeSplitter[V, T]): Iterable[TokenRange] = {

    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraPartitionGenerator.pool)
    (for (tokenRange <- parTokenRanges;
          split <- splitter.split(tokenRange, splitSize)) yield split).seq
  }

  private def createTokenRangeSplitter: TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        new RandomPartitionerTokenRangeSplitter(totalDataSize).asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        new Murmur3PartitionerTokenRangeSplitter(totalDataSize).asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported TokenFactory $tokenFactory")
    }
  }

  private val primaryKeyStr =
    tableDef.partitionKey.map(_.columnName).map(quote).mkString(", ")

  private def rangeToCql(range: TokenRange): Seq[CqlTokenRange[V, T]] =
    range.unwrap.map(CqlTokenRange(_, primaryKeyStr))

  def partitions: Seq[CassandraPartition[V, T]] = {
    val tokenRanges = splitCount match {
      case Some(1) => Seq(fullRange)
      case _ => describeRing
    }

    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val splitter = createTokenRangeSplitter
    val splits = splitsOf(tokenRanges, splitter).toSeq
    val maxGroupSize = tokenRanges.size / endpointCount
    val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
    val tokenRangeGroups = clusterer.group(splits).toArray

    val partitions = for (group <- tokenRangeGroups) yield {
      val replicas = group.map(_.replicas).reduce(_ intersect _)
      val rowCount = group.map(_.dataSize).sum
      val cqlRanges = group.flatMap(rangeToCql)
      // partition index will be set later
      CassandraPartition(0, replicas, cqlRanges, rowCount)
    }

    // sort partitions and assign sequential numbers so that
    // partition index matches the order of partitions in the sequence
    partitions
      .sortBy(p => (p.endpoints.size, -p.dataSize))
      .zipWithIndex
      .map { case (p, index) => p.copy(index = index) }
  }

  /**
    * Attempts to build a partitioner for this C* RDD if it was keyed with Type Key. If possible
    * returns a partitioner of type Key. The type is required so we know what kind of objects we
    * will need to bind to prepared statements when determining the token on new objects.
    */
  def getPartitioner[Key: ClassTag : RowWriterFactory](
      keyMapper: ColumnSelector): Option[CassandraPartitioner[Key, V, T]] = {

    val part = Try {
      val newPartitioner = new CassandraPartitioner(connector, tableDef, partitions, keyMapper)
      // This is guarenteed to succeed so we don't want to send out an ERROR message if it breaks
      newPartitioner.verify(log = false)
      newPartitioner
    }

    if (part.isFailure) {
      logDebug(s"Not able to automatically create a partitioner: ${part.failed.get.getMessage}")
    }

    part.toOption
  }
}

object CassandraPartitionGenerator {
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token ranges to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  /** Creates a `CassandraPartitionGenerator` for the given cluster and table.
    * Unlike the class constructor, this method does not take the generic `V` and `T` parameters,
    * and therefore you don't need to specify the ones proper for the partitioner used in the
    * Cassandra cluster. */
  def apply(
    conn: CassandraConnector,
    tableDef: TableDef,
    splitCount: Option[Int],
    splitSize: Int): CassandraPartitionGenerator[V, T] = {

    val tokenFactory = getTokenFactory(conn)
    new CassandraPartitionGenerator(conn, tableDef, splitCount, splitSize)(tokenFactory)
  }

  def getTokenFactory(conn: CassandraConnector) : TokenFactory[V, T] = {
    val partitionerName = conn.withSessionDo { session =>
      session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }
    TokenFactory.forCassandraPartitioner(partitionerName)
  }
}
