package com.sneaksanddata.arcane.framework
package services.consumers

import java.sql.{Connection, DriverManager, ResultSet}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class JdbcConsumerOptions(connectionUrl: String):
  def isValid: Boolean = Try(DriverManager.getDriver(connectionUrl)) match {
    case Success(_) => true
    case Failure(_) => false
  }


class JdbcConsumer[Batch <: Either[StagedBackfillBatch, StagedVersionedBatch]](val options: JdbcConsumerOptions) extends AutoCloseable:
  require(options.isValid, "Invalid JDBC url provided for the consumer")
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  def applyBatch(batch: Batch): Future[ResultSet] =
    val querySql = batch.fold(sbb => sbb.batchQuery, svb => svb.batchQuery).query
    val statement = sqlConnection.prepareStatement(querySql)
    Future(statement.executeQuery())
    
//  def archiveBatch(batch: Batch): Future[Unit]

  def close(): Unit = sqlConnection.close()


//case class SqlConsumer(val ) extends SqlConsumer[SqlServerChangeTrackingBatch]:
//  lazy val sqlConnection: Connection = DriverManager.getConnection(jdbcUrl)
//
//  def mergeBatch(batch: SqlServerChangeTrackingBatch): Future[SqlServerChangeTrackingBatch] =
//    require(sqlConnection.isValid(10000), "")
//    val statement = sqlConnection.prepareStatement(batch.applyBatchQuery())
//    Future(statement.executeQuery()).map(rs => batch.copy(batchResult = rs))
//
//  def archiveBatch(batch: SqlServerChangeTrackingBatch)(implicit sqlConnection: Connection): Future[Unit] = ???