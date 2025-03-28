package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.common

import main.appLayer
import models.app.{SqlServerChangeTrackingStreamContext, given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions}

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

import java.sql.{Connection, DriverManager}

object Common:

  type StreamLifeTimeServiceLayer = ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken]
  type StreamContextLayer = ZLayer[Any, Nothing, SqlServerChangeTrackingStreamContext.Environment]

  def createTestApp(lifetimeService: StreamLifeTimeServiceLayer, streamContextLayer: StreamContextLayer) = appLayer.provide(
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
      Services.restCatalog,
      GenericBackfillStreamingOverwriteDataProvider.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      GenericStreamingGraphBuilder.backfillSubStreamLayer,
      MsSqlBackfillOverwriteBatchFactory.layer,

      streamContextLayer,
      lifetimeService
    )

  val trinoConnectionString = "jdbc:trino://localhost:8080/iceberg/test?user=test"

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

  def getData(targetTableName: String): ZIO[Any, Throwable, List[(Int, String)]] = ZIO.scoped {
        for
            connection <- ZIO.attempt(DriverManager.getConnection(trinoConnectionString))
            statement <- ZIO.attempt(connection.createStatement())
            resultSet <- ZIO.fromAutoCloseable(ZIO.attempt(statement.executeQuery(s"SELECT Id, Name from $targetTableName")))
            data <- ZIO.attempt {
                Iterator.continually((resultSet.next(), resultSet))
                  .takeWhile(_._1)
                  .map { case (_, rs) => (rs.getInt("Id"), rs.getString("Name")) }
                  .toList
            }
        yield data
    }
