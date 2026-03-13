package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.MicrosoftSqlServerPluginStreamContext
import tests.common.Common

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, Field}
import com.sneaksanddata.arcane.framework.services.mssql.versioning.MsSqlWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.streaming.TimeLimitLifetimeService
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  IntStrStrDecoder,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, TestSystem, ZIOSpecDefault, assertTrue}
import zio.{Duration, Scope, ZIO, ZLayer}

import java.sql.ResultSet

object SchemaMigrationTests extends ZIOSpecDefault:
  val sourceTableName = "SchemaEvolutionTests"
  val targetTableName = "iceberg.test.schema_evolution"

  private val streamContextStr =
    s"""
       |
       | {
       |  "groupingIntervalSeconds": 1,
       |  "tableProperties": {
       |    "partitionExpressions": [],
       |    "format": "PARQUET",
       |    "sortedBy": [],
       |    "parquetBloomFilterColumns": []
       |  },
       |  "rowsPerGroup": 10000,
       |  "sinkSettings": {
       |    "optimizeSettings": {
       |      "batchThreshold": 60,
       |      "fileSizeThreshold": "512MB"
       |    },
       |    "orphanFilesExpirationSettings": {
       |      "batchThreshold": 60,
       |      "retentionThreshold": "6h"
       |    },
       |    "snapshotExpirationSettings": {
       |      "batchThreshold": 60,
       |      "retentionThreshold": "6h"
       |    },
       |    "analyzeSettings": {
       |      "batchThreshold": 60,
       |      "includedColumns": []
       |    },
       |    "targetTableName": "$targetTableName",
       |    "sinkCatalogSettings": {
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "catalogUri": "http://localhost:20001/catalog"
       |    }
       |  },
       |  "sourceSettings": {
       |    "changeCaptureIntervalSeconds": 1,
       |    "commandTimeout": 3600,
       |    "schema": "dbo",
       |    "table": "$sourceTableName",
       |    "fetchSize": 1024
       |   },
       |  "stagingDataSettings": {
       |    "catalog": {
       |      "warehouse": "demo",
       |      "catalogName": "iceberg",
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "schemaName": "test"
       |    },
       |    "maxRowsPerFile": 1,
       |    "tableNamePrefix": "staging_integration_tests"
       |  },
       |  "fieldSelectionRule": {
       |    "ruleType": "all",
       |    "fields": []
       |  },
       |  "observabilitySettings": {
       |    "metricTags": {
       |      "key1": "value1",
       |      "key2": "value2"
       |    }
       |  }
       |}
       |
       |""".stripMargin

  private val streamingStreamContext = MicrosoftSqlServerPluginStreamContext(streamContextStr)
  private val streamingStreamContextLayer =
    ZLayer.succeed[MicrosoftSqlServerPluginStreamContext](streamingStreamContext)
  private val dbName = "SchemaMigrationTests"

  private def before = TestAspect.before(Fixtures.withFreshTablesZIO(dbName, sourceTableName, targetTableName))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("SchemaMigrationTests")(
    test("handle the schema migration (column insertions)") {
      val streamingData  = List.range(1, 4).map(i => (i, s"Test$i"))
      val afterEvolution = List.range(4, 7).map(i => (i, s"Test$i", s"Updated $i"))

      val afterEvolutionExpected = List.range(1, 4).map(i => (i, s"Test$i", null))
        ++ List.range(4, 7).map(i => (i, s"Test$i", s"Updated $i"))

      for {
        _ <- TestSystem.putEnv(
          "ARCANE_FRAMEWORK__MICROSOFT_SQL_SERVER_CONNECTION_URI",
          s"jdbc:sqlserver://localhost:1433;databaseName=$dbName;user=sa;password=tMIxN11yGZgMC;encrypt=false;trustServerCertificate=true"
        )
        _ <- prepareWatermark(
          targetTableName.split("\\.").last,
          ArcaneSchema(Seq(Field("test", StringType))),
          MsSqlWatermark.epoch
        )

        sourceConnection <- ZIO.succeed(Fixtures.getConnection)

        // launch stream and wait for it to create target with streamingData rows (initial table)
        streamRunner <- Common.getTestApp(Duration.fromSeconds(15), streamingStreamContextLayer).fork
        _            <- Common.insertData(dbName, sourceConnection, sourceTableName, streamingData)

        _ <- ZIO.sleep(Duration.fromSeconds(10))

        // update SOURCE (SQL) schema with a new column
        _ <- Common.addColumns(dbName, sourceConnection, sourceTableName, "NewName VARCHAR(100)")
        // let it propagate
        _ <- Common.waitForColumns(dbName, sourceConnection, sourceTableName, 3)
        // INSERT data with a new schema
        _ <- Common.insertUpdatedData(dbName, sourceConnection, sourceTableName, afterEvolution)

        // overall test timeout
        _ <- streamRunner.runOrFail(Duration.fromSeconds(10))

        // read target table after schema migration
        afterStream <- readTarget(
          streamingStreamContext.sink.targetTableFullName,
          "Id, Name, NewName",
          (rs: ResultSet) => (rs.getInt(1), rs.getString(2), rs.getString(3))
        )
      } yield assertTrue(
        afterStream.sorted == afterEvolutionExpected
      )
    },
    test("handle the schema migration (column deletions)") {
      val streamingData  = List.range(1, 4).map(i => (i, s"Test$i", s"Updated $i"))
      val afterEvolution = List.range(4, 7).map(i => (i, s"Test$i"))

      val afterEvolutionExpected = streamingData ++ List.range(4, 7).map(i => (i, s"Test$i", null))

      for {
        _ <- TestSystem.putEnv(
          "ARCANE_FRAMEWORK__MICROSOFT_SQL_SERVER_CONNECTION_URI",
          s"jdbc:sqlserver://localhost:1433;databaseName=$dbName;user=sa;password=tMIxN11yGZgMC;encrypt=false;trustServerCertificate=true"
        )
        _ <- prepareWatermark(
          targetTableName.split("\\.").last,
          ArcaneSchema(Seq(Field("test", StringType))),
          MsSqlWatermark.epoch
        )

        sourceConnection <- ZIO.succeed(Fixtures.getConnection)
        _                <- Common.addColumns(dbName, sourceConnection, sourceTableName, "NewName VARCHAR(100)")

        streamRunner <- Common.getTestApp(Duration.fromSeconds(180), streamingStreamContextLayer).fork
        _            <- Common.insertUpdatedData(dbName, sourceConnection, sourceTableName, streamingData)

        _ <- ZIO.sleep(Duration.fromSeconds(10))

        _ <- Common.removeColumns(dbName, sourceConnection, sourceTableName, "NewName")
        _ <- Common.waitForColumns(dbName, sourceConnection, sourceTableName, 2)

        _ <- Common.insertData(dbName, sourceConnection, sourceTableName, afterEvolution)

        _ <- streamRunner.runOrFail(Duration.fromSeconds(10))

        afterEvolution <- readTarget(
          streamingStreamContext.sink.targetTableFullName,
          "Id, Name, NewName",
          IntStrStrDecoder
        )

      } yield assertTrue(
        afterEvolution.sorted == afterEvolutionExpected
      )
    }
  ) @@ before @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
