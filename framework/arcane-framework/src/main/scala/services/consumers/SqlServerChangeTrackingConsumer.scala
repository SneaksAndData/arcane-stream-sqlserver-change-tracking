package com.sneaksanddata.arcane.framework
package services.consumers

import java.sql.{Connection, DriverManager}
import scala.concurrent.Future

class SqlServerChangeTrackingConsumer(jdbcUrl: String) extends SqlConsumer[SqlServerChangeTrackingBatch]:
  implicit val sqlConnection: Connection = DriverManager.getConnection(jdbcUrl)

  def mergeBatch(batch: SqlServerChangeTrackingBatch)(implicit sqlConnection: Connection): Future[SqlServerChangeTrackingBatch] =
    require(sqlConnection.isValid(10000), "")
    val statement = sqlConnection.prepareStatement(batch.applyBatchQuery())
    Future(statement.executeQuery()).map(rs => batch.copy(batchResult = rs))

  def archiveBatch(batch: SqlServerChangeTrackingBatch)(implicit sqlConnection: Connection): Future[Unit] = ???


object SqlServerChangeTrackingConsumer:
  def apply(jdbcUrl: String): SqlConsumer[SqlServerChangeTrackingBatch] = new SqlServerChangeTrackingConsumer(jdbcUrl)
