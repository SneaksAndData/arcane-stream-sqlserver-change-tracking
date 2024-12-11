package com.sneaksanddata.arcane.framework
package models.cdm

import models.{ArcaneSchema, ArcaneSchemaField, ArcaneType, Field, MergeKeyField}
import services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReader}

import upickle.default.*

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.language.implicitConversions

case class SimpleCdmAttribute(name: String, dataType: String, maxLength: Int)
  derives ReadWriter

case class SimpleCdmEntity(
                            @upickle.implicits.key("$type")
                            entityType: String,
                            name: String,
                            description: String,
                            attributes: Seq[SimpleCdmAttribute])
  derives ReadWriter


case class SimpleCdmModel(name: String, description: String, version: String, entities: Seq[SimpleCdmEntity])
  derives ReadWriter

given Conversion[SimpleCdmAttribute, ArcaneSchemaField] with
  override def apply(entity: SimpleCdmAttribute): ArcaneSchemaField = entity.dataType match
    case "guid" => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "string" => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "int64" => Field(name = entity.name, fieldType = ArcaneType.LongType)
    case "decimal" => Field(name = entity.name, fieldType = ArcaneType.DoubleType)
    case "dateTime" => Field(name = entity.name, fieldType = ArcaneType.TimestampType)
    case "dateTimeOffset" => Field(name = entity.name, fieldType = ArcaneType.DateTimeOffsetType)
    case "boolean" => Field(name = entity.name, fieldType = ArcaneType.BooleanType)
    case _ => Field(name = entity.name, fieldType = ArcaneType.StringType)

given Conversion[SimpleCdmEntity, ArcaneSchema] with
  override def apply(entity: SimpleCdmEntity): ArcaneSchema = entity.attributes.map(implicitly) :+ MergeKeyField

object SimpleCdmModel:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  // number of fields in the schema of each entity which do not originate from CDM
  // currently MergeKeyField only
  val systemFieldCount: Int = 1

  def apply(rootPath: String, reader: AzureBlobStorageReader): Future[SimpleCdmModel] =
    AdlsStoragePath(rootPath).map(_ + "model.json") match {
      case Success(modelPath) => reader.getBlobContent(modelPath).map(read[SimpleCdmModel](_))
      case Failure(ex) => Future.failed(ex)
    }
