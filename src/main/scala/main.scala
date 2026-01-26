package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.SqlServerChangeTrackingStreamContext

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
import com.sneaksanddata.arcane.framework.services.filters.{ColumnSummaryFieldsFilteringService, FieldsFilteringService}
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.DataDog.Environment
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DataDog, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.mssql.base.MsSqlReader
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
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{MetricsConfig, datadog}
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.{DatagramSocketConfig, StatsdClient, statsdUDS}
import zio.{Runtime, URLayer, ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

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
    WatermarkProcessor.layer,
    BackfillOverwriteWatermarkProcessor.layer,
    ArcaneDimensionsProvider.layer,
    DataDog.UdsPublisher.layer
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
