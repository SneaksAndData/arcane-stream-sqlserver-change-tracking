package com.sneaksanddata.arcane.framework
package services.streaming.base

/**
 * A trait that represents a service that can be used to determine if a stream should be cancelled.
 */
trait StreamLifetimeService {
  /**
   * Returns true if the stream should be cancelled.
   */
  def cancelled: Boolean
}
