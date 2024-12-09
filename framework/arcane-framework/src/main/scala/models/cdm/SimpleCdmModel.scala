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
