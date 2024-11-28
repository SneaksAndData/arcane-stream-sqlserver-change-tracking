package com.sneaksanddata.arcane.framework
package services.consumers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

class SqlServerChangeTrackingTests extends AnyFlatSpec with Matchers:

  it should "generate an overwrite query" in {
    val query = SqlServerChangeTrackingBackfillQuery("test.table_a", "SELECT * FROM test.staged_a")
    val expected = Using(Source.fromURL(getClass.getResource("/generate_an_overwrite_query.sql"))) { _.getLines().mkString("\n") }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge query" in {
    val query = SqlServerChangeTrackingMergeQuery("test.table_a", "SELECT * FROM test.staged_a", Map(), "ARCANE_MERGE_KEY", Seq("ARCANE_MERGE_KEY", "colA", "colB"))

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query.sql"))) { _.getLines().mkString("\n") }.get
    query.query should equal(expected)
  }
