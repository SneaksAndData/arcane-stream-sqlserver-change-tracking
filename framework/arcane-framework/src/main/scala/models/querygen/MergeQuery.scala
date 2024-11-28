package com.sneaksanddata.arcane.framework
package models.querygen

import com.sneaksanddata.arcane.framework.services.consumers.WhenNotMatchedInsert

import scala.annotation.targetName

object MergeQueryCommons:
  /**
   * Alias for the batch table in all queries
   */
  val SOURCE_ALIAS: String = "t_s"
  /**
   * Alias for the target (output) table in all queries
   */
  val TARGET_ALIAS: String = "t_o"

case class MergeQuery(baseQuery: String, segments: Seq[MergeQuerySegment]) extends StreamingBatchQuery:
  @targetName("plusplus")
  def ++(segment: MergeQuerySegment): MergeQuery = copy(segments = segments :+ segment)
  def query: String =
    require(segments.exists(seg => seg match
      case _: OnSegment => true
      case _ => false), "OnSegment is not defined for this query, unable to generate runnable SQL")
    require(segments.exists(seg => seg match
      case _: WhenNotMatchedInsert => true
      case _ => false
    ), "WhenNotMatchedInsert segment is not defined for this query, unable to generate runnable SQL")

    segments.foldLeft(baseQuery)((result, segment) => Seq(result, "\n", segment.toString).mkString(""))

object MergeQuery:
  private def baseQuery(targetName: String, sourceQuery: String): String =
    s"""
       |MERGE INTO $targetName ${MergeQueryCommons.TARGET_ALIAS}
       |USING ($sourceQuery) ${MergeQueryCommons.SOURCE_ALIAS}
       |""".stripMargin

  def apply(targetName: String, sourceQuery: String): MergeQuery = new MergeQuery(baseQuery(targetName, sourceQuery), Seq())
