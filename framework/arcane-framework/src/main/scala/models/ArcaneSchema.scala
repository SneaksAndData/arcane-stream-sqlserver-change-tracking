package com.sneaksanddata.arcane.framework
package models

import scala.language.implicitConversions

/**
 * A field in the schema definition
 */
trait ArcaneSchemaField:
  val name: String
  val fieldType: ArcaneType

/**
 * ArcaneSchema is a type alias for a sequence of fields or structs.
 */
class ArcaneSchema(fields: Seq[ArcaneSchemaField]) extends Seq[ArcaneSchemaField]:
  private def isValid: Boolean = fields.isEmpty || fields.exists {
    case _: PrimaryKeyField => true
    case _ => false
  }
  require(isValid, "Primary Key Field must be defined for the schema to be valid")

  def primaryKey: ArcaneSchemaField = fields.find {
    case _: PrimaryKeyField => true
    case _ => false
  }.get

  def apply(i: Int): ArcaneSchemaField = fields(i)

  def length: Int = fields.length

  def iterator: Iterator[ArcaneSchemaField] = fields.iterator

/**
 * Companion object for ArcaneSchema.
 */
object ArcaneSchema:
  implicit def fieldSeqToArcaneSchema(fields: Seq[ArcaneSchemaField]): ArcaneSchema = ArcaneSchema(fields)
  /**
   * Creates an empty ArcaneSchema.
   * @return An empty ArcaneSchema.
   */
  def empty(): ArcaneSchema = Seq.empty


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
 */
final case class Field(name: String, fieldType: ArcaneType) extends ArcaneSchemaField

/**
 * Field is a case class that represents a primary key field in ArcaneSchema
 */
final case class PrimaryKeyField(name: String, fieldType: ArcaneType) extends ArcaneSchemaField
