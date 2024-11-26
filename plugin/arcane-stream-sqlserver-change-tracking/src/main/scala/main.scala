package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.StreamSpec

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.VersionedDataGraphBuilderSettings
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.{PosixStreamLifetimeService, StreamRunnerServiceImpl}
import com.sneaksanddata.arcane.framework.services.mssql.MsSqlConnection.{BackFillBatch, DataBatch, VersionedBatch}
import com.sneaksanddata.arcane.framework.services.mssql.{ConnectionOptions, MsSqlConnection, MsSqlDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.{BackfillDataGraphBuilder, BackfillGroupingProcessor, LazyListGroupingProcessor, VersionedDataGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillDataProvider, BatchProcessor, StreamGraphBuilder, VersionedDataProvider}
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.{Chunk, ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {

  type StreamGraphBuilderFactory = ZIO[StreamContext, Nothing, StreamGraphBuilder]
  
  override val bootstrap: ZLayer[Any, Nothing, Unit] = SLF4J.slf4j(LogFormat.colored)

  val appLayer: ZIO[StreamRunnerService & StreamContext, Throwable, Unit] = for
    _ <- ZIO.log("Application starting")
    context <- ZIO.service[StreamContext].debug("initialized stream context")
    streamRunner <- ZIO.service[StreamRunnerService].debug("initialized stream runner")
    _ <- streamRunner.run
  yield ()

  val versionedGraphBuilder = StreamSpec.layer ++ MsSqlDataProvider.layer ++ VersionedDataGraphBuilder.layer
  val backfillGraphBuilder = StreamSpec.layer ++ MsSqlDataProvider.layer ++ BackfillDataGraphBuilder.layer

  private val gbLayer = ZLayer.fromZIO( ZIO.service[StreamContext].flatMap { context =>
    if context.IsBackfilling then
      for
        _ <- ZIO.log("Running in backfill mode")
        dp <- ZIO.service[BackfillDataProvider]
        ls <- ZIO.service[StreamLifetimeService]
        bp <- ZIO.service[BatchProcessor[BackFillBatch, Chunk[DataRow]]]
      yield new BackfillDataGraphBuilder(dp, ls, bp)
    else
      for
        _ <- ZIO.log("Running in streaming mode")
        sss <- ZIO.service[VersionedDataGraphBuilderSettings]
        dp <- ZIO.service[VersionedDataProvider[Long, VersionedBatch]]
        ls <- ZIO.service[StreamLifetimeService]
        bp <- ZIO.service[BatchProcessor[DataBatch, Chunk[DataRow]]]
      yield new VersionedDataGraphBuilder(sss, dp, ls, bp)
  })


//    for context <- ZIO.service[StreamContext].debug("initialized stream context")
//  yield  if context.IsBackfilling then
//      backfillGraphBuilder
//    else
//      versionedGraphBuilder

  @main
  def run: ZIO[Any, Throwable, Unit] = appLayer
    .provide(
      StreamSpec.layer,
      PosixStreamLifetimeService.layer,
      MsSqlConnection.layer,
      MsSqlDataProvider.layer,
      LazyListGroupingProcessor.layer,
      StreamRunnerServiceImpl.layer,
      gbLayer,
      BackfillGroupingProcessor.layer
    )
    .orDie
}
