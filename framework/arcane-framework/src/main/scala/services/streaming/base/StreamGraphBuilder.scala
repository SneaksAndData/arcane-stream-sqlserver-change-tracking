package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZStream
import zio.stream.ZSink

/**
 * A trait that represents a stream graph builder.
 */
trait StreamGraphBuilder {

  /**
   * The type of the stream element produced by the source and consumed by the sink.
   */
  type StreamElementType

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def create: ZStream[Any, Throwable, StreamElementType]

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def consume: ZSink[Any, Throwable, StreamElementType, Any, Unit]
}
