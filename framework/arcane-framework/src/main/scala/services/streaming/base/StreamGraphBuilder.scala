package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZStream

/**
 * A trait that represents a stream graph builder.
 * @tparam IncomingType The type of the incoming data.
 */
trait StreamGraphBuilder[IncomingType] {

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def create: ZStream[Any, Throwable, IncomingType]
}
