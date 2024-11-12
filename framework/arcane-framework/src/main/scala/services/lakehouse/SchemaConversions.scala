package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.ArcaneSchema
import org.apache.iceberg.Schema

import scala.language.implicitConversions

object SchemaConversions:
  implicit def toIcebergSchema(schema: ArcaneSchema): Schema = ???
