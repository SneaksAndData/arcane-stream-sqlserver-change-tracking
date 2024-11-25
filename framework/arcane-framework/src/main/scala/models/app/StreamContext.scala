package com.sneaksanddata.arcane.framework
package models.app

import models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}

/**
 * Provides the context for the stream.
 */
trait StreamContext:

  /**
   * The id of the stream.
   */
  def streamId: String = sys.env("STREAMCONTEXT__STREAM_ID")

  /**
   * True if the stream is running in backfill mode.
   */
  def IsBackfilling: Boolean = sys.env.getOrElse("STREAM CONTEXT__BACKFILL", "false").toLowerCase() == "true"

  /**
   * Kind of the stream
   */
  def streamKind: String = sys.env("STREAMCONTEXT__STREAM_KIND")
