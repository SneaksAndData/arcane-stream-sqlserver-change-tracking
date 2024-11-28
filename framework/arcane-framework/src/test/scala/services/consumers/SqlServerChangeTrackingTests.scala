package com.sneaksanddata.arcane.framework
package services.consumers

import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

class SqlServerChangeTrackingTests extends AnyFlatSpec with Matchers:

  it should "generate an overwrite query" in {
    val query = SqlServerChangeTrackingBackfillQuery("test.staged_a", "SELECT * FROM test.staged_a")
    Using(Source.fromURL(getClass.getResource("/generate_an_overwrite_query.sql"))) { src =>
      val expected = src.getLines().mkString("\n")
      query.query should equal(expected)
    }
  }
