package com.sneaksanddata.arcane.framework
package models.cdm

import models.{ArcaneSchema, ArcaneSchemaField, ArcaneType, Field, MergeKeyField}
import upickle.default.*

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

given Conversion[SimpleCdmEntity, ArcaneSchemaField] with
  override def apply(entity: SimpleCdmEntity): ArcaneSchemaField = entity.entityType match
    case "guid" => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "string" => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "int64" => Field(name = entity.name, fieldType = ArcaneType.LongType)
    case "decimal" => Field(name = entity.name, fieldType = ArcaneType.DoubleType)
    case "dateTime" => Field(name = entity.name, fieldType = ArcaneType.TimestampType)
    case "dateTimeOffset" => Field(name = entity.name, fieldType = ArcaneType.DateTimeOffsetType)
    case "boolean" => Field(name = entity.name, fieldType = ArcaneType.BooleanType)
    case _ => Field(name = entity.name, fieldType = ArcaneType.StringType)

given Conversion[SimpleCdmModel, ArcaneSchema] with
  override def apply(model: SimpleCdmModel): ArcaneSchema = model.entities.map(implicitly) :+ MergeKeyField
