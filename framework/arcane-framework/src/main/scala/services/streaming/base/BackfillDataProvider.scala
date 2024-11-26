package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.mssql.MsSqlConnection.BackFillBatch

import zio.Task

/**
 * A trait that represents a backfill data provider.
 */
trait BackfillDataProvider:

  /**
   * Provides the backfill data.
   *
   * @return A task that represents the backfill data.
   */
  def provideData: Task[BackFillBatch]
