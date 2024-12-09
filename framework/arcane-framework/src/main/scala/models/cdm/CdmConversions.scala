package com.sneaksanddata.arcane.framework
package models.cdm

import models.{ArcaneSchema, ArcaneSchemaField, ArcaneType, DataCell, DataRow, Field, MergeKeyField}

import scala.annotation.tailrec
import scala.language.implicitConversions

object CSVParser {

  def parseCsvLine(line: String, headerCount: Int, delimiter: Char = ','): Array[String] = {
    @tailrec
    def loop(i: Int, quoteSum: Int, prevCharIndex: Int, fieldCounter: Int, result: Array[String]): Array[String] = {
      if (i >= line.length) {
        if (quoteSum != 0) throw new IllegalStateException(s"CSV line $line with delimiter $delimiter has mismatching field quotes")
        result
      } else {
        val newQuoteSum = if (line(i) == '"' && i < line.length - 1 && quoteSum == 0) quoteSum + 1
        else if (line(i) == '"') quoteSum - 1
        else quoteSum

        val isDelimiter = line(i) == delimiter || i == line.length - 1
        if (isDelimiter && newQuoteSum == 0) {
          val newPrevCharIndex = if (line(prevCharIndex) == '"') prevCharIndex + 1 else prevCharIndex
          val endIndex = if (i == line.length - 1 && line(i) != '"') i + 1
          else if (line(i - 1) == '"') i - 1
          else i

          result(fieldCounter) = line.slice(newPrevCharIndex, endIndex)
          loop(i + 1, newQuoteSum, i + 1, fieldCounter + 1, result)
        } else loop(i + 1, newQuoteSum, prevCharIndex, fieldCounter, result)
      }
    }

    loop(0, 0, 0, 0, new Array[String](headerCount))
  }

  def isComplete(csvLine: String): Boolean = {
    csvLine.count(_ == '"') % 2 == 0
  }

  def replaceQuotedNewlines(csvLine: String): String = {
    val regex = new Regex("\"[^\"]*(?:\"\"[^\"]*)*\"")
    regex.replaceSomeIn(csvLine, m => Some(m.matched.replace("\n", ""))).replace("\r", "")
  }

}


object CdmConversions:
  private val mergeKeyName: String = "Id"
  implicit def asField(entity: SimpleCdmEntity): ArcaneSchemaField = entity.entityType match
    case "guid" => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "string" => Field(name = entity.name, fieldType = ArcaneType.StringType)
    case "int64" => Field(name = entity.name, fieldType = ArcaneType.LongType)
    case "decimal" => Field(name = entity.name, fieldType = ArcaneType.DoubleType)
    case "dateTime" => Field(name = entity.name, fieldType = ArcaneType.TimestampType)
    case "dateTimeOffset" => Field(name = entity.name, fieldType = ArcaneType.DateTimeOffsetType)
    case "boolean" => Field(name = entity.name, fieldType = ArcaneType.BooleanType)
    case _ => Field(name = entity.name, fieldType = ArcaneType.StringType)

  implicit def asSchema(model: SimpleCdmModel): ArcaneSchema = model.entities.map(implicitly) :+ MergeKeyField
  
  implicit def asDataRow(csvLine: String)(implicit schema: ArcaneSchema): DataRow =
    val parsed = CSVParser.parseCsvLine(csvLine, schema.size)
    val mergeKeyValue = parsed(schema.zipWithIndex.find(v => v._1.name == mergeKeyName).get._2)
    parsed
      .zipWithIndex
      .map { (fieldValue, index) =>
        val field = schema(index)
        DataCell(name = field.name, Type = field.fieldType, value = fieldValue)  
      }.concat(
        Seq(DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = mergeKeyValue))
      ).toList
