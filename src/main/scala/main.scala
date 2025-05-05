package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.SqlServerChangeTrackingStreamContext

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService, StreamRunnerServiceImpl}
import com.sneaksanddata.arcane.framework.services.filters.{ColumnSummaryFieldsFilteringService, FieldsFilteringService}
import com.sneaksanddata.arcane.framework.services.hooks.manager.EmptyHookManager
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.merging.{JdbcMergeServiceClient, MutableSchemaCache}
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.mssql.{ConnectionOptions, MsSqlBackfillOverwriteBatchFactory, MsSqlConnection, MsSqlDataProvider, MsSqlHookManager, MsSqlStreamingDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{GenericBackfillStreamingMergeDataProvider, GenericBackfillStreamingOverwriteDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{GenericGraphBuilderFactory, GenericStreamingGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import com.sneaksanddata.arcane.sql_server_change_tracking.metrics.StatsdUdsClient
import org.slf4j.MDC
import zio.Console.printLine
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{MetricsConfig, datadog, statsd}
import zio.{Runtime, ZIO, ZIOAppDefault, ZLayer}
import zio.metrics.connectors.datadog.DatadogConfig

import java.time.Duration

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _ <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _ <- streamRunner.run
  yield ()

  private val schemaCache = MutableSchemaCache()

  private lazy val streamRunner = appLayer.provide(
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      SqlServerChangeTrackingStreamContext.layer,
      PosixStreamLifetimeService.layer,
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
      ColumnSummaryFieldsFilteringService.layer,
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,

    ZLayer.succeed(DatadogConfig.default),
    ZLayer.succeed(MetricsConfig(Duration.ofMillis(1000))),
    datadog.datadogLayer,
    StatsdUdsClient.layer,
    ZLayer.succeed(statsd.StatsdUdsConfig(sys.env.getOrElse("SOCKET_PATH", "/tmp/datadog/datadog.2.socket"))),
  )

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = streamRunner

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(zio.ExitCode(1))
      } yield ()
    }
}
