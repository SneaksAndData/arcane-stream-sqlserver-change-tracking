package com.sneaksanddata.arcane.framework
package services.streaming

import models.DataCell
import services.mssql.MsSqlConnection
import services.mssql.MsSqlConnection.VersionedBatch
import services.mssql.query.LazyQueryResult.OutputType
import services.mssql.query.QueryRunner
import services.streaming.base.{HasVersion, StreamLifetimeService}

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.Duration
import scala.util.{Failure, Try}


given HasVersion[VersionedBatch] with
  extension (result: VersionedBatch)
    def getLatestVersion: Option[Long] =
      val (queryResult, version) = result

      // If the database response is empty, we can't extract the version from it and return the old version.
      queryResult.read.headOption match
        case None => Some(version)
        case Some(row) =>
          // If the database response is not empty, we can extract the version from any row of the response.
          // Let's take the first row and try to extract the version from it.
          val dataVersion = row.filter(_.name == "ChangeTrackingVersion") match
            // For logging purposes will be used in the future.
            case Nil => Failure(new UnsupportedOperationException("No ChangeTrackingVersion found in row."))
            case version :: _ => Try(version.value.asInstanceOf[Long])
          dataVersion.toOption



class StreamGraphBuilder(msSqlConnection: MsSqlConnection) {
  implicit val queryRunner: QueryRunner = QueryRunner()

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @param isCancelled The service that determines if the stream should be cancelled.
   * @return The stream that reads the changes from the database.
   */
  def build(isCancelled: StreamLifetimeService): ZStream[Any, Throwable, OutputType] = ZStream.unfoldZIO(None) { previousVersion =>
    isCancelled.cancelled match
      case true => ZIO.succeed(None)
      case false => continueStream(previousVersion)
  }

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(OutputType, Option[Long])]] =
    requestChanges(previousVersion, Duration.ofDays(1)) map { versionedBatch  =>
      val latestVersion = versionedBatch.getLatestVersion
      val (queryResult, _) = versionedBatch
      Some(queryResult.read, latestVersion)
    }

  private def requestChanges(previousVersion: Option[Long], lookBackInterval: Duration): Task[VersionedBatch] =
    ZIO.fromFuture(_ => msSqlConnection.getChanges(previousVersion, lookBackInterval))

}
