package com.sneaksanddata.arcane.framework
package services.connectors.base

import org.apache.parquet.schema.MessageType
import org.apache.parquet.hadoop.ParquetReader

import scala.concurrent.Future
import scala.util.Try


trait SchemaProvider[Schema]:
  def getSchema: Future[Schema]
