package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.ArcaneType.{IntType, StringType}
import models.{ArcaneSchema, DataCell, DataRow, Field}

import scala.language.postfixOps
import scala.jdk.CollectionConverters.*
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*

import java.util.UUID
import scala.concurrent.Future

/*
TODO: document needed env settings
export ARCANE.FRAMEWORK__S3_CATALOG_ACCESS_KEY_ID=minioadmin
export ARCANE.FRAMEWORK__S3_CATALOG_SECRET_ACCESS_KEY=minioadmin
export ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID=<value generated by compose> - WIP
export ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET=<value generated by compose> - WIP
export ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI=http://localhost:8181/api/catalog/v1/oauth/tokens
export ARCANE.FRAMEWORK__S3_CATALOG_AUTH_SCOPE=PRINCIPAL_ROLE:ALL
export ARCANE.FRAMEWORK__S3_CATALOG_ENDPOINT=http://localhost:9000
 */
class IcebergS3CatalogWriterTests extends flatspec.AsyncFlatSpec with Matchers:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val s3CatalogFileIO = S3CatalogFileIO

  it should "create a table when provided schema and rows" in {
    val writer = IcebergS3CatalogWriter(
      "test",
      "polaris",
      catalogUri = "http://localhost:8181/api/catalog",
      additionalProperties = IcebergCatalogCredential.oAuth2Properties,
      s3CatalogFileIO = s3CatalogFileIO,
      schema = Seq(Field(name = "colA", fieldType = IntType), Field(name = "colB", fieldType = StringType)),
      locationOverride = Some("s3://tmp/polaris/test")
    )
    val rows = Seq(List(
      DataCell(name = "colA", Type = IntType, value = 1), DataCell(name = "colB", Type = StringType, value = "abc"),
      DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "def"),
      DataCell(name = "colA", Type = IntType, value = 2), DataCell(name = "colB", Type = StringType, value = "iop"),
      DataCell(name = "colA", Type = IntType, value = 3), DataCell(name = "colB", Type = StringType, value = "tyr")
    ))

    writer.write(
      data = rows,
      name = UUID.randomUUID.toString
    ).map(tbl => tbl.history().asScala.isEmpty should equal(false))
  }