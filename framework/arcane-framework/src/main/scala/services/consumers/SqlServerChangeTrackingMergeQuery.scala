package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{MergeQuery, MergeQueryCommons, MergeQuerySegment, OnSegment, WhenMatchedDelete, WhenMatchedUpdate, WhenNotMatchedInsert}

object WhenMatchedDelete extends WhenMatchedDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentValue: String = s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION = 'D'"
  }
  
object WhenMatchedUpdate extends WhenMatchedUpdate {
  def apply(): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentValue: String = s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D' AND ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_VERSION > ${MergeQueryCommons.TARGET_ALIAS}.SYS_CHANGE_VERSION"
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
      ++ WhenMatchedUpdate
      ++ WhenNotMatchedInsert(columns)
      
     