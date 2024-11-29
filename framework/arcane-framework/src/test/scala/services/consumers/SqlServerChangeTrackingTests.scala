package com.sneaksanddata.arcane.framework
package services.consumers

import com.sneaksanddata.arcane.framework.models.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.Field
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

class SqlServerChangeTrackingTests extends AnyFlatSpec with Matchers:

  it should "generate a valid overwrite query" in {
    val query = SqlServerChangeTrackingBackfillQuery("test.table_a", "SELECT * FROM test.staged_a")
    val expected = Using(Source.fromURL(getClass.getResource("/generate_an_overwrite_query.sql"))) { _.getLines().mkString("\n") }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge query" in {
    val query = SqlServerChangeTrackingMergeQuery(
      "test.table_a",
      "SELECT * FROM test.staged_a",
      Map(),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query.sql"))) { _.getLines().mkString("\n") }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge with partitions" in {
    val query = SqlServerChangeTrackingMergeQuery(
      "test.table_a",
      "SELECT * FROM test.staged_a",
      Map(
        "colA" -> List("a", "b", "c")
      ),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query_with_partitions.sql"))) {
      _.getLines().mkString("\n")
    }.get

    query.query should equal(expected)
  }

  "SqlServerChangeTrackingBackfillBatch" should "generate a valid backfill batch" in {
    val batch = SqlServerChangeTrackingBackfillBatch("test.staged_a", Seq(
      Field(
        name = "ARCANE_MERGE_KEY",
        fieldType = StringType,
        isMergeKey = true
      ),
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      )
    ), "test.table_a")

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_sql_ct_backfill_batch_query.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }
