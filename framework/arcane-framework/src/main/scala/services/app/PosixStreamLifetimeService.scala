package com.sneaksanddata.arcane.framework
package services.app

import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}

class PosixStreamLifetimeService extends StreamLifetimeService with InterruptionToken:

  @volatile
  private var isCancelled = false
  
  @volatile
  private var isInterrupted = false

  /**
   * Returns true if the stream should be cancelled.
   */
  def cancelled: Boolean = this.isCancelled
  
  /**
   * Returns true if the stream has been interrupted.
   */
  def interrupted: Boolean = this.isInterrupted

  /**
   * Cancels the stream.
   */
  def cancel(): Unit = this.isCancelled = true

  def start() :Unit = sys.addShutdownHook({
    this.isInterrupted = true
    cancel()
  })

