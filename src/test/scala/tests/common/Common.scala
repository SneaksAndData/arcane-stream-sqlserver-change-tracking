package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.common

import main.appLayer
import models.app.SqlServerChangeTrackingStreamContext

import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.base.DeclaredMetrics
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.merging.{JdbcMergeServiceClient, MutableSchemaCache}
import com.sneaksanddata.arcane.framework.services.metrics.ArcaneDimensionsProvider
import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{GenericBackfillStreamingMergeDataProvider, GenericBackfillStreamingOverwriteDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{GenericGraphBuilderFactory, GenericStreamingGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import zio.metrics.connectors.{MetricsConfig, datadog}
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration

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
      ArcaneDimensionsProvider.layer,
      DeclaredMetrics.layer
    )

  /**
   * Inserts data into the test table.
   * @param connection The connection to the database.
   * @param data The data to insert.
   * @return A ZIO effect that inserts the data.
   */
  def insertData(connection: Connection, tableName: String, data: Seq[(Int, String)]): ZIO[Any, Throwable, Unit] = ZIO.scoped {
      for
          statement <- ZIO.attempt(connection.prepareStatement(s"INSERT INTO $tableName (Id, Name) VALUES (?, ?)"))
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
   * Inserts data into the test table.
   * @param connection The connection to the database.
   * @param data The data to insert.
   * @return A ZIO effect that inserts the data.
   */
  def insertUpdatedData(connection: Connection, tableName: String, data: Seq[(Int, String, String)]): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.attempt(connection.prepareStatement(s"INSERT INTO $tableName (Id, Name, NewName) VALUES (?, ?, ?)"))
      _ <- ZIO.foreachDiscard(data) { case (number, string, second_string) =>
        ZIO.attempt {
          statement.setInt(1, number)
          statement.setString(2, string)
          statement.setString(3, second_string)
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
  def getData[Result](targetTableName: String, columnList: String, decoder: ResultSet => Result): ZIO[Any, Throwable, List[Result]] = ZIO.scoped {
        for
            connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
            statement <- ZIO.attempt(connection.createStatement())
            resultSet <- ZIO.fromAutoCloseable(ZIO.attempt(statement.executeQuery(s"SELECT $columnList from $targetTableName")))
            data <- ZIO.attempt {
                Iterator.continually((resultSet.next(), resultSet))
                  .takeWhile(_._1)
                  .map { case (_, rs) => decoder(rs) }
                  .toList
            }
        yield data
    }

  /**
   * Inserts columns into the test table.
   *
   * @param connection The connection to the database.
   * @param tableName  The name of the table.
   * @param migrationExpression The migration expression to add the column.
   * @return A ZIO effect that inserts the data.
   */
  def addColumns(connection: Connection, tableName: String, migrationExpression: String): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.fromAutoCloseable(ZIO.attempt(connection.prepareStatement(s"ALTER TABLE $tableName ADD $migrationExpression")))
      _ <- ZIO.attempt(statement.execute())
    yield ()
  }

  /**
   * Inserts data into the test table.
   *
   * @param connection          The connection to the database.
   * @param tableName           The name of the table.
   * @param migrationExpression The migration expression to add the column.
   * @return A ZIO effect that inserts the data.
   */
  def removeColumns(connection: Connection, tableName: String, migrationExpression: String): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.fromAutoCloseable(ZIO.attempt(connection.prepareStatement(s"ALTER TABLE $tableName DROP COLUMN $migrationExpression")))
      _ <- ZIO.attempt(statement.execute())
    yield ()
  }
