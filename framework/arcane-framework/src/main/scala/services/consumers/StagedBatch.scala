package com.sneaksanddata.arcane.framework
package services.consumers

trait BatchCanDelete:
  def mergeDeleteCondition(): String

trait BatchCanUpdate:
  def mergeUpdateCondition(): String

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

  def mergeMatchCondition(): String
  def mergeValueSet(): String
  def mergeInsertCondition(): String
