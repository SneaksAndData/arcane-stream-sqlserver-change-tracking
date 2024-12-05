package com.sneaksanddata.arcane.framework
package services.streaming

import models.querygen.{MergeQuery, StreamingBatchQuery}
import services.consumers.{JdbcConsumer, StagedBatch, StagedVersionedBatch}
import services.streaming.base.BatchProcessor

import zio.{ZIO, ZLayer}
import zio.stream.ZPipeline

import java.sql.ResultSet

class MergeProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch]) 
  extends BatchProcessor[StagedBatch[MergeQuery], Boolean]:
  
  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch, Boolean] =
    ZPipeline.mapZIO(batch => ZIO.fromFuture(implicit ec => jdbcConsumer.applyBatch(batch)))

object MergeProcessor:

  /**
   * Factory method to create IcebergConsumer
   *
   * @param streamContext  The stream context.
   * @param catalogWriter  The catalog writer.
   * @param schemaProvider The schema provider.
   * @return The initialized IcebergConsumer instance
   */
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch]): MergeProcessor =
    new MergeProcessor(jdbcConsumer)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = JdbcConsumer[StagedVersionedBatch]

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, MergeProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
      yield MergeProcessor(jdbcConsumer)
    }
