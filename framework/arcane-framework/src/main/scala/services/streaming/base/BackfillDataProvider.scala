package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.mssql.MsSqlConnection.BackFillBatch

import zio.Task

trait BackfillDataProvider:
  def provideData: Task[BackFillBatch]
