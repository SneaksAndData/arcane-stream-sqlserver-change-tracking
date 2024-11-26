package com.sneaksanddata.arcane.framework
package services.streaming

import models.DataRow
import services.app.base.StreamLifetimeService
import services.mssql.MsSqlConnection.BackFillBatch
import services.streaming.base.{BackfillDataProvider, BatchProcessor, StreamGraphBuilder}

import org.slf4j.{Logger, LoggerFactory}
import zio.{Chunk, ZIO, ZLayer}
import zio.stream.{ZSink, ZStream}

class BackfillDataGraphBuilder(backfillDataProvider: BackfillDataProvider,
                               streamLifetimeService: StreamLifetimeService,
                               batchProcessor: BatchProcessor[BackFillBatch, Chunk[DataRow]])
  extends StreamGraphBuilder:


  private val logger: Logger = LoggerFactory.getLogger(classOf[BackfillDataGraphBuilder])

  override type StreamElementType = Chunk[DataRow]

  override def create: ZStream[Any, Throwable, StreamElementType] =
    ZStream.fromZIO(backfillDataProvider.provideData)
    .takeUntil(_ => streamLifetimeService.cancelled)
    .via(batchProcessor.process)

  override def consume: ZSink[Any, Throwable, StreamElementType, Any, Unit] =
    ZSink.foreach { e =>
      logger.info(s"Received ${e.size} rows from the streaming source")
      ZIO.unit
    }

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object BackfillDataGraphBuilder:
  private type GraphBuilderLayerTypes = BackfillDataProvider
    & StreamLifetimeService
    & BatchProcessor[BackFillBatch, Chunk[DataRow]]

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[GraphBuilderLayerTypes, Nothing, StreamGraphBuilder] =
    ZLayer {
      for {
        dp <- ZIO.service[BackfillDataProvider]
        ls <- ZIO.service[StreamLifetimeService]
        bp <- ZIO.service[BatchProcessor[BackFillBatch, Chunk[DataRow]]]
      } yield new BackfillDataGraphBuilder(dp, ls, bp)
    }
