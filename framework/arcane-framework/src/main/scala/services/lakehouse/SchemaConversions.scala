package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.ArcaneType.*
import models.{ArcaneSchema, ArcaneType}

import org.apache.iceberg.Schema
import org.apache.iceberg.types.{Type, Types}
import scala.jdk.CollectionConverters.*

/**
 * Converts an Arcane schema to an Iceberg schema.
 */
given Conversion[ArcaneType, Type] with
  def apply(arcaneType:  ArcaneType): Type = arcaneType match
    case IntType => Types.IntegerType.get()
    case LongType => Types.LongType.get()
    case ByteArrayType => Types.BinaryType.get()
    case BooleanType => Types.BooleanType.get()
    case StringType => Types.StringType.get()
    case DateType => Types.DateType.get()
    case TimestampType => Types.TimestampType.withoutZone()
    case DateTimeOffsetType => Types.TimestampType.withZone()
    case BigDecimalType => Types.DecimalType.of(30, 6)
    case DoubleType => Types.DoubleType.get()
    case FloatType => Types.FloatType.get()
    case ShortType => Types.IntegerType.get()
    case TimeType => Types.TimeType.get()

/**
 * Converts an Arcane schema to an Iceberg schema.
 */
given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = new Schema(
    schema
      .zipWithIndex
      .map {
        (field, index) =>
          Types.NestedField.optional(index, field.name, field.fieldType)
      }.asJava
  )
