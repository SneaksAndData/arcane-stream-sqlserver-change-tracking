package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.ArcaneType.{IntType, StringType}
import models.{DataCell, Field}
import services.streaming.given_Conversion_ArcaneSchema_Schema

import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*

import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

class IcebergS3CatalogWriterTests extends flatspec.AsyncFlatSpec with Matchers:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val s3CatalogFileIO = S3CatalogFileIO
  private val schema = Seq(Field(name = "colA", fieldType = IntType), Field(name = "colB", fieldType = StringType))
  private val icebergSettings = IcebergSettings(
    "test",
    "polaris",
    catalogUri = "http://localhost:8181/api/catalog",
    additionalProperties = IcebergCatalogCredential.oAuth2Properties,
    s3CatalogFileIO = s3CatalogFileIO,
    locationOverride = Some("s3://tmp/polaris/test")
  )
  private val icebergWriter = IcebergS3CatalogWriter(icebergSettings)

  it should "create a table when provided schema and rows" in {
    val rows = Seq(List(
      DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc"),
      DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "def"),
      DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "iop"),
      DataCell(name = "colA", Type = IntType, value = 3), DataCell(name = "colB", Type = StringType, value = "tyr")
    ))

    icebergWriter.write(
      data = rows,
      schema = schema,
      name = UUID.randomUUID.toString
    ).map(tbl => tbl.history().asScala.isEmpty should equal(false))
  }

  it should "create an empty table" in {
    icebergWriter.write(
      data = Seq(),
      schema = schema,
      name = UUID.randomUUID.toString
    ).map(tbl => tbl.history().asScala.isEmpty should equal(false))
  }

  it should "delete table successfully after creating it" in {
    val tblName = UUID.randomUUID.toString
    icebergWriter.write(
      data = Seq(List(
        DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc"),
      )),
      schema = schema,
      name = tblName
    ).flatMap { _ => icebergWriter.delete(tblName) }.map {
      _ should equal(true)
    }
  }

  it should "create a table and then append rows to it" in {
    val tblName = UUID.randomUUID.toString
    val initialData = Seq(List(
      DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc"),
    ))
    val appendData = Seq(List(
      DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "def"),
    ))
    icebergWriter.write(
      data = initialData,
      schema = schema,
      name = tblName
    ).flatMap { _ => icebergWriter.append(appendData, schema, tblName) }.map {
      // expect 2 data transactions: append initialData, append appendData
      // table creation has no data so no data snapshot there
      _.currentSnapshot().sequenceNumber() should equal(2)
    }
  }
