package com.sneaksanddata.arcane.framework
package services.consumers

import com.sneaksanddata.arcane.framework.services.streaming.BackfillDataGraphBuilder
import org.slf4j.{Logger, LoggerFactory}
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait JdbcConsumerOptions:
  val connectionUrl: String

  def isValid: Boolean = Try(DriverManager.getDriver(connectionUrl)) match {
    case Success(_) => true
    case Failure(_) => false
  }


class JdbcConsumer[Batch <: StagedVersionedBatch](val options: JdbcConsumerOptions) extends AutoCloseable:
  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private val logger: Logger = LoggerFactory.getLogger(classOf[BackfillDataGraphBuilder])
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  private def collectPartitionColumn(resultSet: ResultSet, columnName: String): Seq[String] =
    // do not fail on closed result sets
    if resultSet.isClosed then
      Seq.empty
    else
      val current = resultSet.getString(columnName)
      if resultSet.next() then
        collectPartitionColumn(resultSet, columnName) :+ current
      else
        resultSet.close()
        Seq(current)

  def getPartitionValues(batchName: String, partitionFields: List[String]): Future[Map[String, List[String]]] =
    Future.sequence(partitionFields
      .map(partitionField =>
        val query = s"SELECT DISTINCT $partitionField FROM $batchName"
        Future(sqlConnection.prepareStatement(query).executeQuery())
          .map(collectPartitionColumn(_, partitionField))
          .map(values => partitionField -> values.toList)
      )).map(_.toMap)

  def applyBatch(batch: Batch): Future[Boolean] =
    Future{
//      logger.debug(s"Executing batch query: ${batch.batchQuery.query}")
      sqlConnection.prepareStatement(batch.batchQuery.query).execute()
    }
    
  def archiveBatch(batch: Batch): Future[ResultSet] =
    Future(sqlConnection.prepareStatement(batch.archiveExpr).executeQuery())
      .flatMap(_ => Future(sqlConnection.prepareStatement(s"DROP TABLE ${batch.name}").executeQuery()))

  def close(): Unit = sqlConnection.close()

object JdbcConsumer:
  def apply[Batch <: StagedVersionedBatch](options: JdbcConsumerOptions): JdbcConsumer[Batch] =
    new JdbcConsumer[Batch](options)

  /**
   * The ZLayer that creates the MsSqlDataProvider.
   */
  val layer: ZLayer[JdbcConsumerOptions, Nothing, JdbcConsumer[StagedVersionedBatch]] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for connectionOptions <- ZIO.service[JdbcConsumerOptions] yield JdbcConsumer(connectionOptions)
      }
    }
