package com.sneaksanddata.arcane.framework
package services.streaming

import services.mssql.MsSqlConnection.VersionedBatch
import services.mssql.given_HasVersion_VersionedBatch
import services.mssql.query.LazyQueryResult.OutputType
import services.streaming.base.{StreamGraphBuilder, StreamLifetimeService, VersionedDataProvider}

import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import java.time.Duration


class VersionedDataGraphBuilder(versionedDataProvider: VersionedDataProvider[Long, VersionedBatch],
                                streamLifetimeService: StreamLifetimeService) extends StreamGraphBuilder[OutputType] {
  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  def create: ZStream[Any, Throwable, OutputType] =
    ZStream.unfoldZIO(versionedDataProvider.firstVersion) { previousVersion =>
      if streamLifetimeService.cancelled then ZIO.succeed(None) else continueStream(previousVersion)
  }

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(OutputType, Option[Long])]] =
    versionedDataProvider.requestChanges(previousVersion, Duration.ofDays(1)) map { versionedBatch  =>
      val latestVersion = versionedBatch.getLatestVersion
      val (queryResult, _) = versionedBatch
      Some(queryResult.read, latestVersion)
    }
}

object VersionedDataGraphBuilder:
  val layer: ZLayer[VersionedDataProvider[Long, VersionedBatch] & StreamLifetimeService, Nothing, StreamGraphBuilder[OutputType]] =
    ZLayer {
      for {
        dp <- ZIO.service[VersionedDataProvider[Long, VersionedBatch]]
        ls <- ZIO.service[StreamLifetimeService]
      } yield new VersionedDataGraphBuilder(dp, ls)
    }
