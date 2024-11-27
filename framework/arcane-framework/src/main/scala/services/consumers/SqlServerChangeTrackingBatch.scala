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

object SqlServerChangeTrackingBatch:
  def apply(name: String, isBackfill: Boolean, columns: List[String], partitionValues: Map[String, List[String]], mergeKey: String): SqlServerChangeTrackingBatch =
    require(mergeKey != "", "mergeKey for the batch cannot be empty")
    require(name != "", "name for the batch cannot be empty")

    new SqlServerChangeTrackingBatch(name, isBackfill, columns, partitionValues, mergeKey)
    