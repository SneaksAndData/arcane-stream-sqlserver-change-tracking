package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.MicrosoftSqlServerPluginStreamContext

import com.sneaksanddata.arcane.framework.extensions.ZExtensions.*
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.mssql.Services
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.mssql.base.MsSqlStreamingSource
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import zio.logging.backend.SLF4J
import zio.{Runtime, ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val streamingSourceLayer: ZLayer[PluginStreamContext, SecurityException, MsSqlStreamingSource] =
    DefaultNameGenerator.layer >>> MsSqlStreamingSource.getLayer(context =>
      context.asInstanceOf[MicrosoftSqlServerPluginStreamContext].source.configuration
    )

  private lazy val streamRunner = appLayer.provide(
    Services.mssqlSourceLayer,
    streamingSourceLayer,
    LayerAssemblies.frameworkPipelineServicesLayer,
    LayerAssemblies.frameworkStagingServicesLayer,
    MicrosoftSqlServerPluginStreamContext.layer,
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] = streamRunner.handleAppFailure(_ => ZIO.unit)
