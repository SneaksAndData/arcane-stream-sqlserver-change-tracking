//package com.sneaksanddata.arcane.framework
//package services.lakehouse
//
//import models.{ArcaneSchema, DataRow}
//
//import scala.concurrent.Future
//
//
///**
// * CatalogFileIO marks a class that holds implementation of a filesystem used by the catalog
// */
//sealed trait CatalogFileIO:
//  val implClass: String
//
///**
// * S3CatalogFileIO implements S3-based filesystem when used by a catalog
// */
//trait S3CatalogFileIO extends CatalogFileIO:
//  override val implClass: String = "org.apache.iceberg.aws.s3.S3FileIO"
//  val endpoint: String
//
//  val pathStyleEnabled = "true"
//  val accessKeyId: String
//  val secretAccessKey: String
//  val region: String
//
///**
// * Singleton for S3CatalogFileIO
// */
//object S3CatalogFileIO extends S3CatalogFileIO:
//  override val accessKeyId: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_ACCESS_KEY_ID", "")
//  override val secretAccessKey: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_SECRET_ACCESS_KEY", "")
//  override val endpoint: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_ENDPOINT", "")
//  override val region: String = scala.util.Properties.envOrElse("ARCANE_FRAMEWORK__S3_CATALOG_REGION", "us-east-1")
//
//trait CatalogWriter[CatalogImpl, TableImpl]:
//  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
//  implicit val catalog: CatalogImpl
//  implicit val catalogProperties: Map[String, String]
//  implicit val catalogName: String
//
//  def initialize(): CatalogWriter[CatalogImpl, TableImpl]
//  def write(data: Iterable[DataRow], name: String): Future[TableImpl]
//  def delete(tableName: String): Future[Boolean]
