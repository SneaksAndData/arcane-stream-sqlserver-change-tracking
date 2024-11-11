package com.sneaksanddata.arcane.framework
package services.lakehouse

import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog

import scala.concurrent.Future

trait TableWriter[LakehouseTable <: Table, TableCatalog <: Catalog]:
  implicit val catalog: TableCatalog
  implicit def init(): Unit = catalog.initialize("tmp", Map[String, String]())

  def write: Future[LakehouseTable]