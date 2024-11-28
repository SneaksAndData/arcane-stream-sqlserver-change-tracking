package com.sneaksanddata.arcane.framework
package models.querygen

case class OverwriteQuery(sourceQuery: String, targetName: String) extends StreamingBatchQuery:
  def query: String =
      s"""INSERT OVERWRITE $targetName
         |${sourceQuery}""".stripMargin

object OverwriteQuery:
  def apply(sourceQuery: String, targetName: String): OverwriteQuery = new OverwriteQuery(sourceQuery, targetName)
