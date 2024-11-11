package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.DataRow
import scala.concurrent.Future

trait CatalogWriter[CatalogImpl, TableImpl, SchemaImpl]:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val catalog: CatalogImpl
  implicit val catalogProperties: Map[String, String]
  implicit val catalogName: String

  def initialize(): Unit
  def write(data: List[DataRow], schema: SchemaImpl, name: String): Future[TableImpl]
