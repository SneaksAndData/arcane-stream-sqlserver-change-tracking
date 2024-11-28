package com.sneaksanddata.arcane.framework
package services.consumers

import java.sql.{Connection, DriverManager}
import scala.concurrent.Future

trait SqlConsumer[Batch <: StagedBatch] extends AutoCloseable:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  
  def mergeBatch(batch: Batch)(implicit sqlConnection: Connection): Future[Batch]
  def archiveBatch(batch: Batch)(implicit sqlConnection: Connection): Future[Unit]


case class SqlConsumer(val ) extends SqlConsumer[SqlServerChangeTrackingBatch]:
  lazy val sqlConnection: Connection = DriverManager.getConnection(jdbcUrl)

  def mergeBatch(batch: SqlServerChangeTrackingBatch): Future[SqlServerChangeTrackingBatch] =
    require(sqlConnection.isValid(10000), "")
    val statement = sqlConnection.prepareStatement(batch.applyBatchQuery())
    Future(statement.executeQuery()).map(rs => batch.copy(batchResult = rs))

  def archiveBatch(batch: SqlServerChangeTrackingBatch)(implicit sqlConnection: Connection): Future[Unit] = ???