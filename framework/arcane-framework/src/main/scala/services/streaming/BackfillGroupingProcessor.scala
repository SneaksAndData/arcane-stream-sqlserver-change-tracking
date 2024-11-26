package com.sneaksanddata.arcane.framework
package services.streaming

import models.DataRow
import models.settings.GroupingSettings
import services.mssql.MsSqlConnection.{BackFillBatch, DataBatch}
import services.streaming.base.BatchProcessor

import org.slf4j.{Logger, LoggerFactory}
import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}

import scala.concurrent.duration.Duration
import scala.util.{Try, Using}

/**
 * The batch processor implementation that converts a lazy DataBatch to a Chunk of DataRow.
 * @param groupingSettings The grouping settings.
 */
class BackfillGroupingProcessor(groupingSettings: GroupingSettings) extends BatchProcessor[BackFillBatch, Chunk[DataRow]]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, BackFillBatch, Chunk[DataRow]] = ZPipeline
      .map(this.readBatch)
      .map(tryDataRow => tryDataRow.get)
      .map(list => Chunk.fromIterable(list))
      .flattenChunks
      .groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)

  private def readBatch(dataBatch: BackFillBatch): Try[List[DataRow]] = Using(dataBatch) { data => data.read.toList }

/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object BackfillGroupingProcessor:

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[GroupingSettings, Nothing, BatchProcessor[BackFillBatch, Chunk[DataRow]]] =
    ZLayer {
      for settings <- ZIO.service[GroupingSettings] yield BackfillGroupingProcessor(settings)
    }

  def apply(groupingSettings: GroupingSettings): BackfillGroupingProcessor =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new BackfillGroupingProcessor(groupingSettings)
