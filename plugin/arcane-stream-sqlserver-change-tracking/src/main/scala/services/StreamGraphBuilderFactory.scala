package com.sneaksanddata.arcane.sql_server_change_tracking
package services

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{BackfillDataGraphBuilder, VersionedDataGraphBuilder}
import zio.{ZIO, ZLayer}

/**
 * Provides a layer that injects a stream graph builder resolved based on the stream context at runtime.
 */
object StreamGraphBuilderFactory:

  val layer: ZLayer[Environment, Nothing, StreamGraphBuilder] = ZLayer.fromZIO(getGraphBuilder)

  private type Environment = StreamContext
    & BackfillDataGraphBuilder.Environment
    & VersionedDataGraphBuilder.Environment

  private def getGraphBuilder =
    for
      context <- ZIO.service[StreamContext]
      _ <- ZIO.log("Start the graph builder type resolution")
      builder <- if context.IsBackfilling then BackfillDataGraphBuilder() else VersionedDataGraphBuilder()
    yield builder

