package com.sneaksanddata.arcane.framework
package services.consumers

import scala.concurrent.Future

trait SqlConsumer[StagedBatch]:
  def mergeBatch(batch: StagedBatch): Future[StagedBatch]
  def archiveBatch(batch: StagedBatch): Future[Unit]
  
//object SqlServerConsumer extends SqlConsumer[SqlServerStagedBatch]
//val s = SqlConsumer[SqlServerStagedBatch]
  // s.mergeBatch(SqlServerStagedBatch(...))
