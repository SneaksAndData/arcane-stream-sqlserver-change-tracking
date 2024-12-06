package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.SqlServerChangeTrackingStreamContext
import services.StreamGraphBuilderFactory

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.logging.base.Enricher
import com.sneaksanddata.arcane.framework.services.app.{PosixStreamLifetimeService, StreamRunnerServiceImpl}
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumer
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.mssql.{ConnectionOptions, MsSqlConnection, MsSqlDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.{BackfillGroupingProcessor, IcebergConsumer, LazyListGroupingProcessor, MergeProcessor}
import org.slf4j.MDC
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.{ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {

  private val loggingProprieties = Enricher("Application", "Arcane.Stream.Scala")
    ++ Enricher("App", "Arcane.Stream.Scala")
    ++ Enricher.fromEnvironment("APPLICATION_VERSION", "0.0.0")

  override val bootstrap: ZLayer[Any, Nothing, Unit] = SLF4J.slf4j(
    LogFormat.make{ (builder, _, _, _, line, _, _, _, _) =>
      loggingProprieties.enrichLoggerWith(builder.appendKeyValue)
      loggingProprieties.enrichLoggerWith(MDC.put)
      builder.appendText(line())
    }
  )

  private val appLayer  = for
    _ <- ZIO.log("Application starting")
    context <- ZIO.service[StreamContext].debug("initialized stream context")
    streamRunner <- ZIO.service[StreamRunnerService].debug("initialized stream runner")
    _ <- streamRunner.run
  yield ()

  @main
  def run: ZIO[Any, Throwable, Unit] =
    appLayer.provide(
      SqlServerChangeTrackingStreamContext.layer,
      PosixStreamLifetimeService.layer,
      MsSqlConnection.layer,
      MsSqlDataProvider.layer,
      LazyListGroupingProcessor.layer,
      StreamRunnerServiceImpl.layer,
      StreamGraphBuilderFactory.layer,
      BackfillGroupingProcessor.layer,
      IcebergS3CatalogWriter.layer,
      IcebergConsumer.layer,
      MergeProcessor.layer,
      JdbcConsumer.layer
    )
    .orDie
}
