package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.StreamSpec
import services.StreamGraphBuilderFactory

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.{PosixStreamLifetimeService, StreamRunnerServiceImpl}
import com.sneaksanddata.arcane.framework.services.mssql.MsSqlConnection.BackfillBatch
import com.sneaksanddata.arcane.framework.services.mssql.{ConnectionOptions, MsSqlConnection, MsSqlDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.{BackfillGroupingProcessor, LazyListGroupingProcessor}
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.{Chunk, ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = SLF4J.slf4j(LogFormat.colored)

  private val appLayer  = for
    _ <- ZIO.log("Application starting")
    context <- ZIO.service[StreamContext].debug("initialized stream context")
    streamRunner <- ZIO.service[StreamRunnerService].debug("initialized stream runner")
    _ <- streamRunner.run
  yield ()

  @main
  def run: ZIO[Any, Throwable, Unit] = appLayer
    .provide(
      StreamSpec.layer,
      PosixStreamLifetimeService.layer,
      MsSqlConnection.layer,
      MsSqlDataProvider.layer,
      LazyListGroupingProcessor.layer,
      StreamRunnerServiceImpl.layer,
      StreamGraphBuilderFactory.layer,
      BackfillGroupingProcessor.layer
    )
    .orDie
}
