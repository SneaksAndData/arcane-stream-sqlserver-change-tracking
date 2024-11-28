package com.sneaksanddata.arcane.framework
package services.streaming

import models.app.StreamContext
import models.settings.GroupingSettings
import models.{ArcaneSchema, DataRow}
import services.base.SchemaProvider
import services.lakehouse.{CatalogWriter, SchemaConversions}
import services.mssql.MsSqlConnection.DataBatch
import services.streaming.LazyListGroupingProcessor.getTableName
import services.streaming.base.BatchProcessor

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import scala.concurrent.duration.Duration
import scala.util.{Try, Using}


given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = SchemaConversions.toIcebergSchema(schema)
  
/**
 * The batch processor implementation that converts a lazy DataBatch to a Chunk of DataRow.
 * @param groupingSettings The grouping settings.
 */
class LazyListGroupingProcessor(groupingSettings: GroupingSettings,
                                streamContext: StreamContext,
                                catalogWriter: CatalogWriter[RESTCatalog, Table],
                                schemaProvider: SchemaProvider[ArcaneSchema])
  extends BatchProcessor[DataBatch, Table]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, DataBatch, Table] = ZPipeline
      .map(this.readBatch)
      // We use here unsafe get because we need to throw an exception if the data is not available.
      // Otherwise, we can get inconsistent data in the stream.
      .map(tryDataRow => tryDataRow.get)
      .map(list => Chunk.fromIterable(list))
      .flattenChunks
      .groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)
      .mapAccum(0L) { (acc, chunk) => (acc + 1, (chunk, acc.getTableName(streamContext.streamId))) }
      .mapZIO({
        case (rows, tableName) => writeWithWriter(rows, tableName)
      })

  private def readBatch(dataBatch: DataBatch): Try[List[DataRow]] = Using(dataBatch) { data => data.read.toList }

  private def writeWithWriter(rows: Chunk[DataRow], name: String): Task[Table] =
    for
      schema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, schema, name))
    yield table

/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object LazyListGroupingProcessor:
  implicit val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-ddTHH-mm-ss")

  extension (batchNumber: Long) def getTableName(streamId: String): String =
    s"${streamId}-${ZonedDateTime.now(ZoneOffset.UTC).format}-$batchNumber}"

  private type Environment = GroupingSettings
    & CatalogWriter[RESTCatalog, Table]
    & SchemaProvider[ArcaneSchema]
    & StreamContext
  
  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Nothing, LazyListGroupingProcessor] =
    ZLayer {
      for
        settings <- ZIO.service[GroupingSettings]
        writer <- ZIO.service[CatalogWriter[RESTCatalog, Table]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
        streamContext <- ZIO.service[StreamContext]
      yield LazyListGroupingProcessor(settings, streamContext, writer, schemaProvider)
    }

  def apply(groupingSettings: GroupingSettings,
            streamContext: StreamContext,
            writer: CatalogWriter[RESTCatalog, Table],
            schemaProvider: SchemaProvider[ArcaneSchema]): LazyListGroupingProcessor =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new LazyListGroupingProcessor(groupingSettings, streamContext, writer, schemaProvider)
