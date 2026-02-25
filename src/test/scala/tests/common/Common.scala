package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.common

import main.appLayer
import models.app.SqlServerChangeTrackingStreamContext

import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
import com.sneaksanddata.arcane.framework.services.filters.{ColumnSummaryFieldsFilteringService, FieldsFilteringService}
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.mssql.base.MsSqlReader
import com.sneaksanddata.arcane.framework.services.mssql.versioning.MsSqlWatermark
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{
  GenericBackfillStreamingMergeDataProvider,
  GenericBackfillStreamingOverwriteDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{
  GenericGraphBuilderFactory,
  GenericStreamingGraphBuilder
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  type StreamLifeTimeServiceLayer = ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken]
  type StreamContextLayer         = ZLayer[Any, Nothing, SqlServerChangeTrackingStreamContext.Environment]

  /** Builds the test application from the provided layers.
    * @param lifetimeService
    *   The lifetime service layer.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def buildTestApp(
      lifetimeService: StreamLifeTimeServiceLayer,
      streamContextLayer: StreamContextLayer
  ): ZIO[Any, Throwable, Unit] =
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
      MsSqlReader.layer,
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
      ColumnSummaryFieldsFilteringService.layer,
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      WatermarkProcessor.layer,
      BackfillOverwriteWatermarkProcessor.layer,
      IcebergTablePropertyManager.layer
    )

  def getChangeTrackingVersion(dbName: String, connection: Connection): ZIO[Any, Throwable, Long] =
    ZIO.scoped {
      for
        statement <- ZIO.attempt(
          connection.prepareStatement(s"USE $dbName; SELECT CHANGE_TRACKING_CURRENT_VERSION() AS VALUE")
        )
        resultSet <- ZIO.attemptBlocking(statement.executeQuery())
        _         <- ZIO.attempt(resultSet.next())
      yield resultSet.getLong("VALUE")
    }

  /** Inserts data into the test table.
    * @param connection
    *   The connection to the database.
    * @param data
    *   The data to insert.
    * @return
    *   A ZIO effect that inserts the data.
    */
  def insertData(
      dbName: String,
      connection: Connection,
      tableName: String,
      data: Seq[(Int, String)]
  ): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.attempt(
          connection.prepareStatement(s"USE $dbName; INSERT INTO $tableName (Id, Name) VALUES (?, ?)")
        )
        _ <- ZIO.foreachDiscard(data) { case (number, string) =>
          ZIO.attempt {
            statement.setInt(1, number)
            statement.setString(2, string)
            statement.executeUpdate()
          }
        }
      yield ()
    }

  /** Inserts data into the test table.
    * @param connection
    *   The connection to the database.
    * @param data
    *   The data to insert.
    * @return
    *   A ZIO effect that inserts the data.
    */
  def insertUpdatedData(
      dbName: String,
      connection: Connection,
      tableName: String,
      data: Seq[(Int, String, String)]
  ): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.attempt(
        connection.prepareStatement(s"USE $dbName; INSERT INTO $tableName (Id, Name, NewName) VALUES (?, ?, ?)")
      )
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

  /** Updates the data in the test table.
    * @param connection
    *   The connection to the database.
    * @param data
    *   The data to update.
    * @return
    */
  def updateData(
      dbName: String,
      connection: Connection,
      tableName: String,
      data: Seq[(Int, String)]
  ): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.attempt(
          connection.prepareStatement(s"USE $dbName; UPDATE $tableName SET Name = ? WHERE Id = ?")
        )
        _ <- ZIO.foreachDiscard(data) { case (number, string) =>
          ZIO.attempt {
            statement.setString(1, string)
            statement.setInt(2, number)
            statement.executeUpdate()
          }
        }
      yield ()
    }

  /** Deletes the data from the test table.
    * @param connection
    *   The connection to the database.
    * @param primaryKeys
    *   The primary keys of the data to delete.
    * @return
    */
  def deleteData(
      dbName: String,
      connection: Connection,
      tableName: String,
      primaryKeys: Seq[Int]
  ): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.attempt(connection.prepareStatement(s"USE $dbName; DELETE FROM $tableName WHERE Id = ?"))
        _ <- ZIO.foreachDiscard(primaryKeys) { number =>
          ZIO.attempt {
            statement.setInt(1, number)
            statement.executeUpdate()
          }
        }
      yield ()
    }

  /** Gets the data from the *target* table. Using the connection string provided in the
    * `ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI` environment variable.
    * @param targetTableName
    *   The name of the target table.
    * @param decoder
    *   The decoder for the result set.
    * @tparam Result
    *   The type of the result.
    * @return
    *   A ZIO effect that gets the data.
    */
  def getData[Result](
      targetTableName: String,
      columnList: String,
      decoder: ResultSet => Result
  ): ZIO[Any, Throwable, List[Result]] = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet <- ZIO.fromAutoCloseable(
        ZIO.attemptBlocking(statement.executeQuery(s"SELECT $columnList from $targetTableName"))
      )
      data <- ZIO.attempt {
        Iterator
          .continually((resultSet.next(), resultSet))
          .takeWhile(_._1)
          .map { case (_, rs) => decoder(rs) }
          .toList
      }
    yield data
  }

  /** Inserts columns into the test table.
    *
    * @param connection
    *   The connection to the database.
    * @param tableName
    *   The name of the table.
    * @param migrationExpression
    *   The migration expression to add the column.
    * @return
    *   A ZIO effect that inserts the data.
    */
  def addColumns(
      dbName: String,
      connection: Connection,
      tableName: String,
      migrationExpression: String
  ): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.fromAutoCloseable(
          ZIO.attempt(connection.prepareStatement(s"USE $dbName; ALTER TABLE $tableName ADD $migrationExpression"))
        )
        _ <- ZIO.attempt(statement.execute())
      yield ()
    }

  def waitForColumns(
      dbName: String,
      connection: Connection,
      tableName: String,
      expectedCount: Int
  ): ZIO[Any, Throwable, Unit] =
    for _ <- ZIO
        .sleep(Duration.ofSeconds(1))
        .repeatUntilZIO(_ =>
          ZIO
            .scoped {
              for {
                _ <- ZIO.log("Waiting for table schema to be updated")
                statement <- ZIO.fromAutoCloseable(
                  ZIO.attempt(
                    connection.prepareStatement(
                      s"USE $dbName; select count(1) from information_schema.columns where table_name = N'$tableName'"
                    )
                  )
                )
                result <- ZIO.attempt(statement.executeQuery())
                _      <- ZIO.succeed(result.next())
              } yield result.getInt(1) == expectedCount
            }
            .orElseSucceed(false)
        )
    yield ()

  /** Inserts data into the test table.
    *
    * @param connection
    *   The connection to the database.
    * @param tableName
    *   The name of the table.
    * @param migrationExpression
    *   The migration expression to add the column.
    * @return
    *   A ZIO effect that inserts the data.
    */
  def removeColumns(
      dbName: String,
      connection: Connection,
      tableName: String,
      migrationExpression: String
  ): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.fromAutoCloseable(
          ZIO.attempt(
            connection.prepareStatement(s"USE $dbName; ALTER TABLE $tableName DROP COLUMN $migrationExpression")
          )
        )
        _ <- ZIO.attempt(statement.execute())
      yield ()
    }

  val IntStrDecoder: ResultSet => (Int, String) = (rs: ResultSet) => (rs.getInt(1), rs.getString(2))
  val IntStrStrDecoder: ResultSet => (Int, String, String) = (rs: ResultSet) =>
    (rs.getInt(1), rs.getString(2), rs.getString(3))
