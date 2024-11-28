package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{MergeQuery, MergeQueryCommons, MergeQuerySegment, OnSegment, OverwriteQuery, WhenMatchedDelete, WhenMatchedUpdate, WhenNotMatchedInsert}
import models.ArcaneSchema

object WhenMatchedDelete extends WhenMatchedDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION = 'D'")
  }
  
object WhenMatchedUpdate extends WhenMatchedUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D' AND ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_VERSION > ${MergeQueryCommons.TARGET_ALIAS}.SYS_CHANGE_VERSION")
    override val columns: Seq[String] = cols
  }
}

object WhenNotMatchedInsert extends WhenNotMatchedInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String] = cols
  }
}

object SqlServerChangeTrackingMergeQuery:
  def apply(targetName: String, sourceQuery: String, partitionValues: Map[String, List[String]], mergeKey: String, columns: Seq[String]): MergeQuery =
    MergeQuery(targetName, sourceQuery) 
      ++ OnSegment(partitionValues, mergeKey)
      ++ WhenMatchedDelete
      ++ WhenMatchedUpdate(columns)
      ++ WhenNotMatchedInsert(columns)
      
object SqlServerChangeTrackingBackfillQuery:
  def apply(targetName: String, sourceQuery: String): OverwriteQuery = OverwriteQuery(sourceQuery, targetName)
  
object SqlServerChangeTrackingBackfillBatch extends StagedBackfillBatch:
  def apply(batchName: String, batchSchema: ArcaneSchema): StagedBackfillBatch = new StagedBackfillBatch {
    override val name: String = batchName
    override val schema: ArcaneSchema = batchSchema
    override def reduceBatchExpr(): String =
      s"""
        |SELECT * FROM $name AS ${MergeQueryCommons.SOURCE_ALIAS} WHERE ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D'
        |""".stripMargin
    override val batchQuery: OverwriteQuery = SqlServerChangeTrackingBackfillQuery(name, reduceBatchExpr())
  }
  
object SqlServerChangeTrackingMergeBatch extends StagedVersionedBatch:
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]]): StagedVersionedBatch =
    require(schema.exists(f => f.isMergeKey), "No merge key defined in the schema, cannot generate versioned batch")
    
    val mergeKey = schema.find(f => f.isMergeKey).get
    new StagedVersionedBatch {
      override val name: String = batchName
      override val schema: ArcaneSchema = batchSchema

      override def reduceBatchExpr(): String =
        s"""
          |WITH VERSIONS AS (
          |SELECT $mergeKey, MAX(SYS_CHANGE_VERSION) AS LATEST_VERSION
          |FROM $name AS ${MergeQueryCommons.SOURCE_ALIAS}
          |GROUP BY $mergeKey),
          |
          |SELECT
          |${MergeQueryCommons.SOURCE_ALIAS}.*
          |FROM $name AS ${MergeQueryCommons.SOURCE_ALIAS} inner join VERSIONS as v
          |on ${MergeQueryCommons.SOURCE_ALIAS}.$mergeKey = v.$mergeKey AND ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_VERSION = v.LATEST_VERSION
          |""".stripMargin

      override val batchQuery: MergeQuery = 
        SqlServerChangeTrackingMergeQuery(targetName = targetName, sourceQuery = reduceBatchExpr(), partitionValues = partitionValues, mergeKey = mergeKey.name, columns = schema.map(f => f.name))
  }
