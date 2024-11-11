package com.sneaksanddata.arcane.framework
package services.streaming

import models.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.mssql.MsSqlConnection.{DataBatch, VersionedBatch}
import services.mssql.given_HasVersion_VersionedBatch
import services.mssql.query.LazyQueryResult.OutputType
import services.streaming.base.{BatchProcessor, StreamGraphBuilder, StreamLifetimeService, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Chunk, ZIO, ZLayer}

/**
 * The stream graph builder that reads the changes from the database.
 * @param VersionedDataGraphBuilderSettings The settings for the stream source.
 * @param versionedDataProvider The versioned data provider.
 * @param streamLifetimeService The stream lifetime service.
 * @param batchProcessor The batch processor.
 */
class VersionedDataGraphBuilder(VersionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
                                versionedDataProvider: VersionedDataProvider[Long, VersionedBatch],
                                streamLifetimeService: StreamLifetimeService,
                                batchProcessor: BatchProcessor[DataBatch, Chunk[DataRow]])
  extends StreamGraphBuilder[OutputType, Chunk[DataRow]]:
  
  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  override def create: ZStream[Any, Throwable, Chunk[DataRow]] = this.createStream.via(this.batchProcessor.process)

  private def createStream = ZStream.unfoldZIO(versionedDataProvider.firstVersion) { previousVersion =>
      if streamLifetimeService.cancelled then ZIO.succeed(None) else continueStream(previousVersion)
  }

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(DataBatch, Option[Long])]] =
    versionedDataProvider.requestChanges(previousVersion, VersionedDataGraphBuilderSettings.lookBackInterval) map { versionedBatch  =>
      val latestVersion = versionedBatch.getLatestVersion
      val (queryResult, _) = versionedBatch
      Some(queryResult, latestVersion)
    }

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object VersionedDataGraphBuilder:
  private type GraphBuilderLayerTypes = VersionedDataProvider[Long, VersionedBatch]
    & StreamLifetimeService
    & BatchProcessor[DataBatch, Chunk[DataRow]]
    & VersionedDataGraphBuilderSettings

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[GraphBuilderLayerTypes, Nothing, StreamGraphBuilder[OutputType, Chunk[DataRow]]] =
    ZLayer {
      for {
        sss <- ZIO.service[VersionedDataGraphBuilderSettings]
        dp <- ZIO.service[VersionedDataProvider[Long, VersionedBatch]]
        ls <- ZIO.service[StreamLifetimeService]
        bp <- ZIO.service[BatchProcessor[DataBatch, Chunk[DataRow]]]
      } yield new VersionedDataGraphBuilder(sss, dp, ls, bp)
    }
