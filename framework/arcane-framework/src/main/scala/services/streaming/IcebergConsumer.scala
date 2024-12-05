package com.sneaksanddata.arcane.framework
package services.streaming

import models.app.StreamContext
import models.settings.SinkSettings
import models.{ArcaneSchema, DataRow}
import services.base.SchemaProvider
import services.consumers.{BatchApplicationResult, SqlServerChangeTrackingMergeBatch, StagedVersionedBatch}
import services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import services.streaming.IcebergConsumer.{getTableName, toStagedBatch}
import services.streaming.base.{BatchConsumer, BatchProcessor}

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

/**
 * A consumer that writes the data to the staging table.
 *
 * @param streamContext  The stream context.
 * @param catalogWriter  The catalog writer.
 * @param schemaProvider The schema provider.
 */
class IcebergConsumer(streamContext: StreamContext,
                      sinkSettings: SinkSettings,
                      catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                      schemaProvider: SchemaProvider[ArcaneSchema],
                      mergeProcessor: BatchProcessor[StagedVersionedBatch, BatchApplicationResult]) extends BatchConsumer[Chunk[DataRow]]:

  private val logger: Logger = LoggerFactory.getLogger(classOf[IcebergConsumer])

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] =
    writeStagingTable >>> mergeProcessor.process >>> logResults


  private def logResults: ZSink[Any, Throwable, Boolean, Nothing, Unit] = ZSink.foreach { e =>
    logger.info(s"Received the table $e from the streaming source")
    ZIO.unit
  }

  private def writeStagingTable = ZPipeline[Chunk[DataRow]]()
    .mapAccum(0L) { (acc, chunk) => (acc + 1, (chunk, acc.getTableName(streamContext.streamId))) }
    .mapZIO({
      case (rows, tableName) => writeWithWriter(rows, tableName)
    })


  private def writeWithWriter(rows: Chunk[DataRow], name: String): Task[StagedVersionedBatch] =
    for
      arcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, name, arcaneSchema))
    yield table.toStagedBatch(arcaneSchema, sinkSettings.sinkLocation)

object IcebergConsumer:
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")

  extension (batchNumber: Long) def getTableName(streamId: String): String =
    s"${streamId}_${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$batchNumber"

  extension (table: Table) def toStagedBatch(batchSchema: ArcaneSchema, targetName: String): StagedVersionedBatch =
    val batchName = table.name().split('.').last;
    // TODO: implement partition partitionValues
    SqlServerChangeTrackingMergeBatch(batchName, batchSchema, targetName, Map())


  /**
   * Factory method to create IcebergConsumer
   *
   * @param streamContext  The stream context.
   * @param sinkSettings   The stream sink settings.
   * @param catalogWriter  The catalog writer.
   * @param schemaProvider The schema provider.
   * @return The initialized IcebergConsumer instance
   */
  def apply(streamContext: StreamContext,
            sinkSettings: SinkSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            schemaProvider: SchemaProvider[ArcaneSchema],
            mergeProcessor: BatchProcessor[StagedVersionedBatch, Boolean]): IcebergConsumer =
    new IcebergConsumer(streamContext, sinkSettings, catalogWriter, schemaProvider, mergeProcessor)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & CatalogWriter[RESTCatalog, Table, Schema]
    & BatchProcessor[StagedVersionedBatch, Boolean]
    & StreamContext
    & SinkSettings

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergConsumer] =
    ZLayer {
      for
        streamContext <- ZIO.service[StreamContext]
        sinkSettings <- ZIO.service[SinkSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
        mergeProcessor <- ZIO.service[BatchProcessor[StagedVersionedBatch, Boolean]]
      yield IcebergConsumer(streamContext, sinkSettings, catalogWriter, schemaProvider, mergeProcessor)
    }
