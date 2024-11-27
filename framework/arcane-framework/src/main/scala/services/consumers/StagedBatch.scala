package com.sneaksanddata.arcane.framework
package services.consumers

trait StagedBatch:
  /**
   * Alias for the batch table in all queries
   */
  final val SOURCE_ALIAS: String = "t_s"
  /**
   * Alias for the target (output) table in all queries
   */
  final val TARGET_ALIAS: String = "t_o"


  val name: String
  val isBackfill: Boolean
  val columns: List[String]
  val partitionValues: Map[String, List[String]]
  val mergeKey: String

  def reduceBatchQuery(): String
  def applyBatchQuery(): String
