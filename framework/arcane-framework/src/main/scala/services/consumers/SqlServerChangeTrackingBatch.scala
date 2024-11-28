package com.sneaksanddata.arcane.framework
package services.consumers

import java.sql.ResultSet

case class SqlServerChangeTrackingBatch(name: String, isBackfill: Boolean, columns: List[String], partitionValues: Map[String, List[String]], mergeKey: String, batchResult: ResultSet = null) extends StagedBatch:

  //override def mergeDeleteCondition(): String = s"$SOURCE_ALIAS.SYS_CHANGE_OPERATION = 'D'"

  private def mergeInsertCondition(): String = s"$SOURCE_ALIAS.SYS_CHANGE_OPERATION != 'D'"

//  private def mergeMatchCondition(): String =
//    def generateInClause(content: String, partName: String): String =
//      s"$TARGET_ALIAS.$partName IN ($content)"
//
//    val baseCondition = s"$TARGET_ALIAS.$mergeKey = $SOURCE_ALIAS.$mergeKey"
//    partitionValues
//      .map(values => generateInClause(values._2.map(part => s"'$part'").mkString(","), values._1)) match
//      case partExpr if partExpr.nonEmpty => s"$baseCondition AND ${partExpr.mkString(" AND ")}"
//      case _ => baseCondition

//  private def mergeUpdateCondition(): String =
//    s"$SOURCE_ALIAS.SYS_CHANGE_OPERATION != 'D' AND $SOURCE_ALIAS.SYS_CHANGE_VERSION > $TARGET_ALIAS.SYS_CHANGE_VERSION"

  private def mergeValueSet(): String =
    columns.map(col => s"$SOURCE_ALIAS.$col").mkString(",\n")

  override def applyBatchQuery(): String =
    val columnList = columns.map(col => s"$col").mkString(",")
    val columnSet = columns.map(col => s"$col = $SOURCE_ALIAS.$col").mkString(",\n")
    if isBackfill then
      s"""
         |INSERT OVERWRITE $name
         |${reduceBatchQuery()}
         |""".stripMargin
    else
      // upsert with deletes
      s"""
         |MERGE INTO $name $TARGET_ALIAS
         |USING (${reduceBatchQuery()}) $SOURCE_ALIAS
         |ON ${mergeMatchCondition()}
         |WHEN MATCHED AND ${mergeDeleteCondition()} THEN DELETE
         |WHEN MATCHED AND ${mergeUpdateCondition()} THEN UPDATE SET $columnSet
         |WHEN NOT MATCHED AND ${mergeInsertCondition()} THEN INSERT ($columnList) VALUES (${mergeValueSet()})
         |""".stripMargin

      // upsert, no deletes
      s"""
         |MERGE INTO $name $TARGET_ALIAS
         |USING (${reduceBatchQuery()}) $SOURCE_ALIAS
         |ON ${mergeMatchCondition()}
         |WHEN MATCHED THEN UPDATE SET $columnSet
         |WHEN NOT MATCHED THEN INSERT ($columnList) VALUES (${mergeValueSet()})
         |""".stripMargin

      // append-only
      s"""
         |MERGE INTO $name $TARGET_ALIAS
         |USING (${reduceBatchQuery()}) $SOURCE_ALIAS
         |ON ${mergeMatchCondition()}
         |WHEN NOT MATCHED THEN INSERT ($columnList) VALUES (${mergeValueSet()})
         |""".stripMargin

  override def reduceBatchQuery(): String =
    if !isBackfill then
      s"""
WITH VERSIONS AS (
SELECT $mergeKey, MAX(SYS_CHANGE_VERSION) AS LATEST_VERSION
FROM $name AS $SOURCE_ALIAS
GROUP BY $mergeKey),

SELECT
 $SOURCE_ALIAS.*
FROM $name AS $SOURCE_ALIAS inner join VERSIONS as v
 on $SOURCE_ALIAS.$mergeKey = v.$mergeKey AND $SOURCE_ALIAS.SYS_CHANGE_VERSION = v.LATEST_VERSION
""".stripMargin
    else
      s"""
SELECT * FROM $name AS $SOURCE_ALIAS WHERE $SOURCE_ALIAS.SYS_CHANGE_OPERATION != 'D'
""".stripMargin


object SqlServerChangeTrackingBatch:
  def apply(name: String, isBackfill: Boolean, columns: List[String], partitionValues: Map[String, List[String]], mergeKey: String): SqlServerChangeTrackingBatch =
    require(mergeKey != "", "mergeKey for the batch cannot be empty")
    require(name != "", "name for the batch cannot be empty")

    new SqlServerChangeTrackingBatch(name, isBackfill, columns, partitionValues, mergeKey)
