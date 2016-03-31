package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import org.apache.spark.{Logging, Partitioner}
import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import com.datastax.spark.connector.{ColumnSelector, PartitionKeyColumns}
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.writer.RowWriterFactory

import scala.reflect.ClassTag
import scala.util.Try



case class TokenBounds[V](minToken: Token[V], maxToken: Token[V])

case class PartitioningMetaData[V, T <: Token[V]](
  private val groupedTokenRanges: Seq[Seq[TokenRange[V, T]]],
  tableDef: TableDef,
  tokenBounds: TokenBounds[V],
  connector: CassandraConnector) {

  private def rangeToCqlTokenRange(range: TokenRange[V, T]) = {
    val startToken = range.start.value
    val endToken = range.end.value
    val wrapAround = range.isWrapAround
    val pk = tableDef.partitionKey.map(_.columnName).map(quote).mkString(", ")
    if (range.end == tokenBounds.minToken)
      List(CqlTokenRange(TokenGreaterThan(pk), startToken))
    else if (range.start == tokenBounds.minToken)
      List(CqlTokenRange(TokenLessThanOrEquals(pk), endToken))
    else if (!range.isWrapAround)
      List(CqlTokenRange(TokenGreaterThanAndLessThanOrEquals(pk), startToken, endToken))
    else
      List(
        CqlTokenRange(TokenGreaterThan(pk), startToken),
        CqlTokenRange(TokenLessThanOrEquals(pk), endToken))
    }

  private def sortedGroups = {
    val grouped = for (group <- groupedTokenRanges) yield {
       val replicas = group.map(_.replicas).reduce(_ intersect _)
       val rowCount = group.map(_.dataSize).sum
       (replicas, rowCount, group)
     }
    grouped.sortBy { case (replicas, rowCount, group) => (replicas.size, -rowCount) }
  }

  def getSortedTokenRanges: Seq[Seq[TokenRange[V, T]]] = {
    sortedGroups.map(_._3)
  }

  def getCassandraPartitions: Seq[CassandraPartition] = {
    sortedGroups
      .sortBy { case (replicas, rowCount, group) => (replicas.size, -rowCount) }
      .zipWithIndex
      .map { case ((replicas, rowCount, group), index) =>
        CassandraPartition(index, replicas, group.flatMap(rangeToCqlTokenRange), rowCount)
  }

  }
}

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
private[connector] class CassandraRDDPartitioner[Key : ClassTag, V, T <: Token[V]](
  partitioningMetaData: PartitioningMetaData[V, T],
  val keyMapping: ColumnSelector = PartitionKeyColumns)(
implicit
  @transient rwf:RowWriterFactory[Key]) extends Partitioner with Logging {

  /**
    * Changes the tableDef target of this partitioner. Can only be done within a keyspace
    * verification of key mapping will occur with the call to [[verify()]]
    */
  def withTableDef(tableDef: TableDef): CassandraRDDPartitioner[Key, V, T] = {
    if (tableDef.keyspaceName != partitioningMetaData.tableDef.keyspaceName) {
      throw new IllegalArgumentException(
        s"""Cannot apply partitioner from keyspace
           |${partitioningMetaData.tableDef.keyspaceName} to table
           |${tableDef.keyspaceName}.${tableDef.tableName} because the keyspaces do
           |not match""".stripMargin)
    }

    new CassandraRDDPartitioner[Key, V, T](
      partitioningMetaData.copy(tableDef = tableDef), keyMapping)
  }

  /**
    * Changes the current key mapping for this partitioner. Verification of mapping
    * occurs on call to [[verify()]]
    */
  def withKeyMapping(keyMapping: ColumnSelector): CassandraRDDPartitioner[Key, V, T] = {
    new CassandraRDDPartitioner[Key, V, T](
      partitioningMetaData,
      keyMapping)
  }


  def partitions: Seq[CassandraPartition] =
    partitioningMetaData.getCassandraPartitions


  private val connector = partitioningMetaData.connector
  private val tableDef = partitioningMetaData.tableDef

  private lazy val partitionKeyNames = PartitionKeyColumns.selectFrom(tableDef).map(_.columnName).toSet
  private lazy val partitionKeyMapping = keyMapping
    .selectFrom(tableDef)
    .filter( colRef => partitionKeyNames.contains(colRef.columnName))
  private lazy val partitionKeyWriter = {
    logDebug(
      s"""Building Partitioner with mapping
         |${partitionKeyMapping.map(x => (x.columnName, x.selectedAs))}
         |for table $tableDef""".stripPrefix("|"))
    implicitly[RowWriterFactory[Key]]
      .rowWriter (tableDef, partitionKeyMapping)
  }

  /**
    * Builds and makes sure we can make a rowWriter with the current TableDef and keyMapper
    */
  def verify(): Unit = {
    val attempt = Try(partitionKeyWriter)
    if (attempt.isFailure) {
      logError("Unable to build partition key writer CassandraRDDPartitioner.", attempt.failed.get)
      throw attempt.failed.get
    }
  }

  private val indexedRanges = partitioningMetaData.getSortedTokenRanges.zipWithIndex

  private val tokenBounds = partitioningMetaData.tokenBounds
  private val minTokenValue = tokenBounds.minToken.value
  private val maxTokenValue = tokenBounds.maxToken.value
  implicit val tokenOrder = tokenBounds.minToken.ord



  /**
   * Since the Token Generator relies on a (non-serializable) prepared statement we need to
   * make sure it is not serialized to executors and is made fresh on each executor
   */
  @transient lazy val tokenGenerator = {
    new TokenGenerator(connector, tableDef, partitionKeyWriter)
  }

  override def getPartition(key: Any): Int = {
    key match {
      case x: Key => {
        val token = tokenGenerator.getTokenFor(x)
        indexOfPartitionContaining(token)
      }
      case other => throw new IllegalArgumentException(s"Couldn't determine the key from object $other")
    }
  }

  /**
    * Since Driver TokenRange objects are not serializable we replicate the
    * logic for TokenRange.contains(Token) here.
    */
  def indexOfPartitionContaining(token: com.datastax.driver.core.Token): Int = {

    val tokenValue = token.getValue.asInstanceOf[V]

    indexedRanges.find { case (ranges, index) =>
      ranges.exists { case tokenRange =>
        val (startValue, endValue, wrap) =
          (tokenRange.start.value, tokenRange.end.value, tokenRange.isWrapAround)

        ((endValue == minTokenValue && tokenOrder.lt(startValue, tokenValue))
          || (startValue == minTokenValue && tokenOrder.gteq(endValue, tokenValue))
          || (!wrap && tokenOrder.lt(startValue, tokenValue) && tokenOrder.gteq(endValue, tokenValue))
          || (wrap && (tokenOrder.lt(startValue, tokenValue) || tokenOrder.gteq(endValue, tokenValue))))
      }
    }.getOrElse(throw new IllegalArgumentException(s"Could not find partition for token $tokenValue"))
      ._2
  }

  override def numPartitions: Int = partitions.length

  override def equals(that: Any): Boolean = that match {
    case that: CassandraRDDPartitioner[Key, V, T] => {
      (this.indexedRanges == that.indexedRanges
        && this.tableDef.keyspaceName == that.tableDef.keyspaceName
        && this.connector == that.connector)
    }
    case _ => {
      false
    }
  }

  override def hashCode: Int = {
    partitions.hashCode() + tableDef.keyspaceName.hashCode * 31
  }

}
/** Creates CassandraPartitions for given Cassandra table */
class CassandraRDDPartitionGenerator[V, T <: Token[V]](
    connector: CassandraConnector,
    val tableDef: TableDef,
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
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraRDDPartitionGenerator.pool)
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

  lazy val tokenBounds = TokenBounds(tokenFactory.minToken, tokenFactory.maxToken)

  def partitions = getPartitioningMetadata.getCassandraPartitions

  /**
    * Generates a list of the CassandraPartitions to be used in the TableScanRDD and a list
    * of the TokenRanges used to craft those partitions along with their indexes.
    */
  def getPartitioningMetadata: PartitioningMetaData[V, T] = {
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

    PartitioningMetaData[V, T](tokenRangeGroups, tableDef, tokenBounds, connector)
  }

  /**
    * Attempts to build a partitioner for this C* RDD if it was keyed with Type Key. If possible
    * returns a partitioner of type Key. The type is required so we know what kind of objects we
    * will need to bind to prepared statements when determining the token on new objects.
    */
  def getPartitioner[Key: ClassTag](keyMapper: ColumnSelector)(
    implicit rowWriterFactory: RowWriterFactory[Key]) : Option[CassandraRDDPartitioner[Key, V, T]] = {
    val part = Try {
      new CassandraRDDPartitioner[Key, V, T](
        partitioningMetaData = getPartitioningMetadata,
        keyMapper)
    }

    if (part.isFailure) {
      logDebug(s"Not able to automatically create a partitioner: ${part.failed.get.getMessage}")
    }

    part.toOption
  }

}

object CassandraRDDPartitionGenerator {
  /** Affects how many concurrent threads are used to fetch split information from cassandra nodes, in `getPartitions`.
    * Does not affect how many Spark threads fetch data from Cassandra. */
  val MaxParallelism = 16

  /** How many token ranges to sample in order to estimate average number of rows per token */
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }

  /** Creates a `CassandraRDDPartitionGenerator` for the given cluster and table.
    * Unlike the class constructor, this method does not take the generic `V` and `T` parameters,
    * and therefore you don't need to specify the ones proper for the partitioner used in the
    * Cassandra cluster. */
  def apply(
    conn: CassandraConnector,
    tableDef: TableDef,
    splitCount: Option[Int],
    splitSize: Int): CassandraRDDPartitionGenerator[V, T] = {

    val tokenFactory = getTokenFactory(conn)
    new CassandraRDDPartitionGenerator(conn, tableDef, splitCount, splitSize)(tokenFactory)
  }

  def getTokenFactory(conn: CassandraConnector) : TokenFactory[V, T] = {
    val partitionerName = conn.withSessionDo { session =>
      session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }
    TokenFactory.forCassandraPartitioner(partitionerName)
  }
}
