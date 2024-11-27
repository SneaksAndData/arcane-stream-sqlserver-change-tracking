package com.sneaksanddata.arcane.framework
package services.consumers

case class SqlServerChangeTrackingBatch(name: String, isBackfill: Boolean, columns: List[String], partitionValues: Map[String, List[String]], mergeKey: String) extends StagedBatch with BatchCanUpdate with BatchCanDelete:

  override def mergeDeleteCondition(): String = s"$SOURCE_ALIAS.SYS_CHANGE_OPERATION = 'D'"

  override def mergeInsertCondition(): String = s"$SOURCE_ALIAS.SYS_CHANGE_OPERATION != 'D'"

  override def mergeMatchCondition(): String =
    def generateInClause(content: String, partName: String): String =
      s"$TARGET_ALIAS.$partName IN ($content)"

    val baseCondition = s"$TARGET_ALIAS.$mergeKey = $SOURCE_ALIAS.$mergeKey"
    partitionValues
      .map(values => generateInClause(values._2.map(part => s"'$part'").mkString(","), values._1)) match
      case partExpr if partExpr.nonEmpty => s"$baseCondition AND ${partExpr.mkString(" AND ")}"
      case _ => baseCondition

  override def mergeUpdateCondition(): String =
    s"$SOURCE_ALIAS.SYS_CHANGE_OPERATION != 'D' AND $SOURCE_ALIAS.SYS_CHANGE_VERSION > $TARGET_ALIAS.SYS_CHANGE_VERSION"

  override def mergeValueSet(): String =
    columns.map(col => s"$SOURCE_ALIAS.$col").mkString(",\n")

  override def reduceBatchQuery(): String =
    if isBackfill then
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
