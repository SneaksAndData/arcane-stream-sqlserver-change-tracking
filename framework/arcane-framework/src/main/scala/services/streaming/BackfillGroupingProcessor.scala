package com.sneaksanddata.arcane.framework
package services.streaming

import models.DataRow
import models.settings.GroupingSettings
import services.mssql.MsSqlConnection.BackfillBatch
import services.streaming.base.BatchProcessor

import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}

import scala.concurrent.duration.Duration
import scala.util.{Try, Using}

/**
 * The batch processor implementation that converts a lazy DataBatch to a Chunk of DataRow.
 * @param groupingSettings The grouping settings.
 */
class BackfillGroupingProcessor(groupingSettings: GroupingSettings) extends BatchProcessor[BackfillBatch, Chunk[DataRow]]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, BackfillBatch, Chunk[DataRow]] = ZPipeline
      .map(this.readBatch)
      // We use here unsafe get because we need to throw an exception if the data is not available.
      // Otherwise, we can get inconsistent data in the stream.
      .map(tryDataRow => tryDataRow.get)
      .map(list => Chunk.fromIterable(list))
      .flattenChunks
      .groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)

  private def readBatch(dataBatch: BackfillBatch): Try[List[DataRow]] = Using(dataBatch) { data => data.read.toList }

/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object BackfillGroupingProcessor:

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[GroupingSettings, Nothing, BatchProcessor[BackfillBatch, Chunk[DataRow]]] =
    ZLayer {
      for
        settings <- ZIO.service[GroupingSettings]
      yield BackfillGroupingProcessor(settings)
    }

  def apply(groupingSettings: GroupingSettings): BackfillGroupingProcessor =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new BackfillGroupingProcessor(groupingSettings)
