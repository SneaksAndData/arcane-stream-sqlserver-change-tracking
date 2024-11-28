package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{MergeQuery, OverwriteQuery, StreamingBatchQuery}
import models.ArcaneSchema


trait StagedBatch[Query <: StreamingBatchQuery]:
  /**
   * Name of the table in the linked Catalog that holds batch data
   */
  val name: String
  /**
   * Schema for the table that holds batch data
   */
  val schema: ArcaneSchema
  val batchQuery: Query

  /**
   * Query that aggregates transactions in the batch to enable merge
   * @return SQL query text
   */
  def reduceBatchExpr(): String


/**
 * StagedBatch that overwrites the whole table and all partitions that it might have
 */
type StagedBackfillBatch = StagedBatch[OverwriteQuery]

/**
 * StagedBatch that updates data in the table
 */
type StagedVersionedBatch = StagedBatch[MergeQuery]
