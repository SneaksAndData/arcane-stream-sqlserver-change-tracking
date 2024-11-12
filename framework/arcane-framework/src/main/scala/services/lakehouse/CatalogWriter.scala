package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, DataRow}

import scala.concurrent.Future

sealed trait CatalogFileIO:
  val implClass: String

trait S3CatalogFileIO extends CatalogFileIO:
  override val implClass: String = "org.apache.iceberg.aws.s3.S3FileIO"
  val endpoint: String

  protected val pathStyleEnabled = "true"
  protected val accessKeyId: String
  protected val secretAccessKey: String
  protected val region: String

trait CatalogWriter[CatalogImpl, TableImpl]:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val catalog: CatalogImpl
  implicit val catalogProperties: Map[String, String]
  implicit val catalogName: String

  def initialize(): Unit
  def write(data: Iterable[DataRow], name: String): Future[TableImpl]
  def delete(tableName: String): Future[Unit]
