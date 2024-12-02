package com.sneaksanddata.arcane.framework
package services.streaming

import models.app.StreamContext
import models.{ArcaneSchema, DataRow}
import services.base.SchemaProvider
import services.lakehouse.{CatalogWriter, SchemaConversions}
import services.streaming.IcebergConsumer.getTableName
import services.streaming.base.BatchConsumer

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

/**
 * Converts an Arcane schema to an Iceberg schema.
 */
given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = SchemaConversions.toIcebergSchema(schema)

/**
 * A consumer that writes the data to the staging table.
 *
 * @param streamContext  The stream context.
 * @param catalogWriter  The catalog writer.
 * @param schemaProvider The schema provider.
 */
class IcebergConsumer(streamContext: StreamContext,
                      catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                      schemaProvider: SchemaProvider[ArcaneSchema]) extends BatchConsumer[Chunk[DataRow]]:

  private val logger: Logger = LoggerFactory.getLogger(classOf[IcebergConsumer])

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] =
    writeStagingTable >>> logResults


  private def logResults: ZSink[Any, Throwable, Table, Nothing, Unit] = ZSink.foreach { e =>
    logger.info(s"Received the table ${e.name()} from the streaming source")
    ZIO.unit
  }

  private def writeStagingTable: ZPipeline[Any, Throwable, Chunk[DataRow], Table] = ZPipeline[Chunk[DataRow]]()
    .mapAccum(0L) { (acc, chunk) => (acc + 1, (chunk, acc.getTableName(streamContext.streamId))) }
    .mapZIO({
      case (rows, tableName) => writeWithWriter(rows, tableName)
    })


  private def writeWithWriter(rows: Chunk[DataRow], name: String): Task[Table] =
    for
      schema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, name, schema))
    yield table

object IcebergConsumer:
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")

  extension (batchNumber: Long) def getTableName(streamId: String): String =
    s"$streamId-${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}-$batchNumber"


  /**
   * Factory method to create IcebergConsumer
   *
   * @param streamContext  The stream context.
   * @param catalogWriter  The catalog writer.
   * @param schemaProvider The schema provider.
   * @return The initialized IcebergConsumer instance
   */
  def apply(streamContext: StreamContext,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            schemaProvider: SchemaProvider[ArcaneSchema]): IcebergConsumer =
    new IcebergConsumer(streamContext, catalogWriter, schemaProvider)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema] & CatalogWriter[RESTCatalog, Table, Schema] & StreamContext

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergConsumer] =
    ZLayer {
      for
        streamContext <- ZIO.service[StreamContext]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
      yield IcebergConsumer(streamContext, catalogWriter, schemaProvider)
    }
