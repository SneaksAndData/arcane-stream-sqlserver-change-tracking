package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, DataRow}

import scala.concurrent.Future

trait CatalogWriter[CatalogImpl, TableImpl]:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val catalog: CatalogImpl
  implicit val catalogProperties: Map[String, String]
  implicit val catalogName: String

  def initialize(): Unit
  def write(data: Iterable[DataRow], name: String): Future[TableImpl]
  def delete(tableName: String): Future[Unit]
