package com.sneaksanddata.arcane.framework
package services.consumers

import java.sql.Connection
import scala.concurrent.Future

trait SqlConsumer[Batch <: StagedBatch]:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  
  def mergeBatch(batch: Batch)(implicit sqlConnection: Connection): Future[Batch]
  def archiveBatch(batch: Batch)(implicit sqlConnection: Connection): Future[Unit]
