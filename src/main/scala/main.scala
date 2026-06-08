package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.MicrosoftSqlServerPluginStreamContext

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{
  GenericStreamRunnerService,
  PosixStreamLifetimeService,
  StreamGraphResolver
}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.graph.DefaultBackfillMergeGraphBuilder
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.{ColumnSummaryFieldsFilteringService, FieldsFilteringService}
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.cleanup.CatalogDisposeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.mssql.backfill.{
  MsSqlBackfillMergeStreamDataProvider,
  MsSqlBackfillSourceDataProvider,
  MsSqlShardFactory,
  MsSqlShardedBackfillStreamDataProvider
}
import com.sneaksanddata.arcane.framework.services.mssql.base.MsSqlStreamingSource
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import zio.logging.backend.SLF4J
import zio.metrics.connectors.datadog
import zio.metrics.connectors.statsd.statsdUDS
import zio.metrics.jvm.DefaultJvmMetrics
import zio.{Runtime, ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val streamingSourceLayer
      : ZLayer[MsSqlStreamingSource.Environment, Nothing, MsSqlStreamingSource & SchemaProvider[ArcaneSchema]] =
    MsSqlStreamingSource.getLayer(context =>
      context.asInstanceOf[MicrosoftSqlServerPluginStreamContext].source.configuration
    )

  private lazy val streamRunner = appLayer.provide(
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer,
    DisposeBatchProcessor.layer,
    FieldFilteringTransformer.layer,
    MergeBatchProcessor.layer,
    StagingProcessor.layer,
    FieldsFilteringService.layer,
    MicrosoftSqlServerPluginStreamContext.layer,
    PosixStreamLifetimeService.layer,
    MsSqlDataProvider.layer,
    IcebergS3CatalogWriter.layer,
    IcebergEntityManager.sinkLayer,
    IcebergEntityManager.stagingLayer,
    IcebergTablePropertyManager.stagingLayer,
    IcebergTablePropertyManager.sinkLayer,
    JdbcMergeServiceClient.layer,

    // source
    streamingSourceLayer,

    // streaming
    MsSqlStreamingDataProvider.layer.unit,
    MsSqlStagedBatchFactory.layer,

    // backfill
    MsSqlBackfillSourceDataProvider.layer,
    MsSqlShardFactory.layer,
    MsSqlShardedBackfillStreamDataProvider.layer,
    MsSqlBackfillMergeStreamDataProvider.layer,
    DefaultBackfillStateManager.layer,
    ShardStagingProcessor.layer,
    BackfillCompletionProcessor.layer,

    // schema
    SchemaMigrationProcessor.layer,

    // maintenance and cleanup
    TargetMaintenanceProcessor.layer,
    CatalogDisposeServiceClient.layer,
    DefaultNameGenerator.layer,
    ColumnSummaryFieldsFilteringService.layer,
    DeclaredMetrics.layer,
    WatermarkProcessor.layer,
    DefaultStreamBootstrapper.layer,
    ThroughputShaperBuilder.layer,
    GlobalMetricTagProvider.layer,
    (DefaultJvmMetrics.liveV2 >>> statsdUDS >>> datadog.live).unit
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
