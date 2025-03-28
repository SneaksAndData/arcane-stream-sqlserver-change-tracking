package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.common

import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}

import java.time.Duration

class TimeLimitLifetimeService(timeLimit: Duration) extends StreamLifetimeService with InterruptionToken:

  @volatile
  private var isCancelled = false

  private val startTime = System.currentTimeMillis()

  override def cancelled: Boolean = startTime + timeLimit.toMillis < System.currentTimeMillis() || this.isCancelled

  override def cancel(): Unit =  this.isCancelled = true

  override def start(): Unit = {
    /* does nothing */
  }

  override def interrupted: Boolean = isCancelled
