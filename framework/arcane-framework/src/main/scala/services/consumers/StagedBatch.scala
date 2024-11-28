package com.sneaksanddata.arcane.framework
package services.consumers

import com.sneaksanddata.arcane.framework.models.ArcaneSchema

trait StagedBatch:
  /**
   * Name of the table in the linked Catalog that holds batch data
   */
  val name: String
  /**
   * Schema for the table that holds batch data
   */
  val schema: ArcaneSchema

  /**
   * Query that aggregates transactions in the batch to enable merge
   * @return SQL query text
   */
  def reduceBatchQuery(): String

  /**
   * Match condition for the batch
   *
   * @return match clause fragment for the MERGE INTO statement
   */
  def mergeMatchCondition(): String

  /**
   * Value SET array for the batch
   *
   * @return VALUES contents for the MERGE INTO ... VALUES (...)
   */
  def mergeValueSet(): String


/**
 * StagedBatch that overwrites the whole table and all partitions that it might have
 */
trait StagedBackfillBatch extends StagedBatch:
  val applyBatchQuery: String =
        s"""
           |INSERT OVERWRITE $name
           |${reduceBatchQuery()}
           |""".stripMargin

/**
 * StagedBatch that updates data in the table
 */
trait StagedVersionedBatch extends StagedBatch:
  val partitionValues: Map[String, List[String]]

  def reduceBatchQuery(): String
  val applyBatchQuery: String =
    val columnList = schema.map(field => s"${field.name}").mkString(",")
    //val columnSet = schema.map(field => s"${field.name} = $SOURCE_ALIAS.${field.name}").mkString(",\n")
    s"""
       |MERGE INTO $name $TARGET_ALIAS
       |USING (${reduceBatchQuery()}) $SOURCE_ALIAS
       |ON ${mergeMatchCondition()}
       |WHEN NOT MATCHED THEN INSERT ($columnList) VALUES (${mergeValueSet()})
       |""".stripMargin
//      s"""
//         |MERGE INTO $name $TARGET_ALIAS
//         |USING (${reduceBatchQuery()}) $SOURCE_ALIAS
//         |ON ${mergeMatchCondition()}
//         |WHEN MATCHED AND ${mergeDeleteCondition()} THEN DELETE
//         |WHEN MATCHED AND ${mergeUpdateCondition()} THEN UPDATE SET $columnSet
//         |WHEN NOT MATCHED AND ${mergeInsertCondition()} THEN INSERT ($columnList) VALUES (${mergeValueSet()})
//         |""".stripMargin
