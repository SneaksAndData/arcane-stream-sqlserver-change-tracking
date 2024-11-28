package com.sneaksanddata.arcane.framework
package models.querygen

object MergeQueryCommons:
  /**
   * Alias for the batch table in all queries
   */
  val SOURCE_ALIAS: String = "t_s"
  /**
   * Alias for the target (output) table in all queries
   */
  val TARGET_ALIAS: String = "t_o"
  
case class MergeQuery(baseQuery: String, segments: Seq[MergeQuerySegment]):
  def ++(segment: MergeQuerySegment): MergeQuery = copy(segments = segments :+ segment)
  def query(): String = segments.foldLeft(baseQuery)((result, segment) => Seq(result, "\n", segment.toString).mkString(""))

object MergeQuery:
  private def baseQuery(targetName: String, sourceQuery: String): String =
    s"""
       |MERGE INTO $targetName ${MergeQueryCommons.TARGET_ALIAS}
       |USING ($sourceQuery) ${MergeQueryCommons.SOURCE_ALIAS}
       |""".stripMargin

  def apply(targetName: String, sourceQuery: String): MergeQuery = new MergeQuery(baseQuery(targetName, sourceQuery), Seq())    
