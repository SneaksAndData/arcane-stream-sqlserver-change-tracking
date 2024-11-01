package com.sneaksanddata.arcane.framework
package models

/**
 * ArcaneSchema is a type alias for a sequence of fields or structs.
 */
type ArcaneSchema = Seq[Field|Struct]

object ArcaneSchema:
  def empty(): ArcaneSchema = Seq.empty


/**
 * Struct is a case class that represents a struct in Arcane.
 * @param fields The fields of the struct.
 */
case class Struct(fields: ArcaneSchema)

/**
 * Types of fields in ArcaneSchema.
 */
enum ArcaneType:
  case LongType
  case ByteArrayType
  case BooleanType
  case StringType
  case DateType
  case TimestampType
  case DateTimeOffsetType
  case BigDecimalType
  case DoubleType
  case IntType
  case FloatType
  case ShortType
  case TimeType
  
/**
 * Field is a case class that represents a field in ArcaneSchema
 * @param name The name of the field.
 * @param fieldType The type of the field.
 */
case class Field(name: String, fieldType: ArcaneType)