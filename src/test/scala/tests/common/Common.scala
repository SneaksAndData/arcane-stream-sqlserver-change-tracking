package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.common

import main.appLayer
import models.app.SqlServerChangeTrackingStreamContext

import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.merging.{JdbcMergeServiceClient, MutableSchemaCache}
import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{GenericBackfillStreamingMergeDataProvider, GenericBackfillStreamingOverwriteDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{GenericGraphBuilderFactory, GenericStreamingGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}

/**
 * Common utilities for tests.
 */
object Common:

  type StreamLifeTimeServiceLayer = ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken]
  type StreamContextLayer = ZLayer[Any, Nothing, SqlServerChangeTrackingStreamContext.Environment]

  /**
   * Builds the test application from the provided layers.
   * @param lifetimeService The lifetime service layer.
   * @param streamContextLayer The stream context layer.
   * @return The test application.
   */
  def buildTestApp(lifetimeService: StreamLifeTimeServiceLayer, streamContextLayer: StreamContextLayer): ZIO[Any, Throwable, Unit] =
    appLayer.provide(
      streamContextLayer,
      lifetimeService,
      
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      MsSqlConnection.layer,
      MsSqlDataProvider.layer,
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      MsSqlStreamingDataProvider.layer,
      MsSqlHookManager.layer,
      ZLayer.succeed(MutableSchemaCache()),
      BackfillApplyBatchProcessor.layer,
      GenericBackfillStreamingOverwriteDataProvider.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      GenericStreamingGraphBuilder.backfillSubStreamLayer,
      MsSqlBackfillOverwriteBatchFactory.layer,
    )

  /**
   * Inserts data into the test table.
   * @param connection The connection to the database.
   * @param data The data to insert.
   * @return A ZIO effect that inserts the data.
   */
  def insertData(connection: Connection, data: Seq[(Int, String)]): ZIO[Any, Throwable, Unit] = ZIO.scoped {
      for
          statement <- ZIO.attempt(connection.prepareStatement("INSERT INTO dbo.TestTable (Id, Name) VALUES (?, ?)"))
          _ <- ZIO.foreachDiscard(data) { case (number, string) =>
            ZIO.attempt {
              statement.setInt(1, number)
              statement.setString(2, string)
              statement.executeUpdate()
            }
          }
      yield ()
    }


  /**
   * Updates the data in the test table.
   * @param connection The connection to the database.
   * @param data The data to update.
   * @return
   */
  def updateData(connection: Connection, data: Seq[(Int, String)]): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.attempt(connection.prepareStatement("UPDATE dbo.TestTable SET Name = ? WHERE Id = ?"))
      _ <- ZIO.foreachDiscard(data) { case (number, string) =>
        ZIO.attempt {
          statement.setString(1, string)
          statement.setInt(2, number)
          statement.executeUpdate()
        }
      }
    yield ()
  }

  /**
   * Deletes the data from the test table.
   * @param connection The connection to the database.
   * @param primaryKeys The primary keys of the data to delete.
   * @return
   */
  def deleteData(connection: Connection, primaryKeys: Seq[Int]): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.attempt(connection.prepareStatement("DELETE FROM dbo.TestTable WHERE Id = ?"))
      _ <- ZIO.foreachDiscard(primaryKeys) { number =>
        ZIO.attempt {
          statement.setInt(1, number)
          statement.executeUpdate()
        }
      }
    yield ()
  }

  /**
   * Gets the data from the *target* table. Using the connection string provided
   * in the `ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI` environment variable.
   * @param targetTableName The name of the target table.
   * @param decoder The decoder for the result set.
   * @tparam Result The type of the result.
   * @return A ZIO effect that gets the data.
   */
  def getData[Result](targetTableName: String, decoder: ResultSet => Result): ZIO[Any, Throwable, List[Result]] = ZIO.scoped {
        for
            connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
            statement <- ZIO.attempt(connection.createStatement())
            resultSet <- ZIO.fromAutoCloseable(ZIO.attempt(statement.executeQuery(s"SELECT Id, Name from $targetTableName")))
            data <- ZIO.attempt {
                Iterator.continually((resultSet.next(), resultSet))
                  .takeWhile(_._1)
                  .map { case (_, rs) => decoder(rs) }
                  .toList
            }
        yield data
    }
