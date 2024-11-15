package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.StreamSpec

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.{PosixStreamLifetimeService, StreamRunnerServiceImpl}
import com.sneaksanddata.arcane.framework.services.mssql.{ConnectionOptions, MsSqlConnection, MsSqlDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.VersionedDataGraphBuilder
import com.sneaksanddata.arcane.framework.services.streaming.base.{LazyListGroupingProcessor, StreamGraphBuilder}
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.{ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {
  
  override val bootstrap: ZLayer[Any, Nothing, Unit] = SLF4J.slf4j(LogFormat.colored)

  private val appLayer = for
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
      VersionedDataGraphBuilder.layer,
      LazyListGroupingProcessor.layer,
      StreamRunnerServiceImpl.layer,
    )
    .orDie
}
