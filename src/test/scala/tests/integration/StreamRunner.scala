package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.MicrosoftSqlServerPluginStreamContext
import tests.common.Common
import tests.integration.SchemaMigrationTests.streamContextStr

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, Field}
import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.mssql.versioning.MsSqlWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  IntStrDecoder,
  getWatermark,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
import org.scalatest.matchers.should.Matchers.should
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, TestSystem, ZIOSpecDefault, assertTrue}
import zio.{Cause, Duration, Scope, Unsafe, ZIO, ZLayer}

import scala.language.postfixOps

object StreamRunner extends ZIOSpecDefault:

  val sourceTableName = "StreamRunner"
  val targetTableName = "iceberg.test.stream_run"

  private val streamContextStr = s"""
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
    |      "namespace": "test",
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
  private val dbName = "StreamRunnerTests"

  private val streamingData = List.range(1, 3).map(i => (i, s"Test$i"))
  private val backfillData  = List.range(4, 7).map(i => (i, s"Test$i"))

  private val updatedData = List.range(4, 7).map(i => (i, s"Update$i"))
  private val deletedData = List(5)

  private val resultData = streamingData ++ updatedData.filterNot(e => deletedData.contains(e._1))

  private def before = TestAspect.before(Fixtures.withFreshTablesZIO(dbName, sourceTableName, targetTableName))

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    test("fail stream when watermark is not set") {
      for
        _ <- TestSystem.putEnv(
          "ARCANE_FRAMEWORK__MICROSOFT_SQL_SERVER_CONNECTION_URI",
          s"jdbc:sqlserver://localhost:1433;databaseName=$dbName;user=sa;password=tMIxN11yGZgMC;encrypt=false;trustServerCertificate=true"
        )
        sourceConnection <- ZIO.succeed(Fixtures.getConnection)

        // Start streaming WITHOUT preparing watermark
        runner <- Common.getTestApp(Duration.fromSeconds(10), streamingStreamContextLayer).fork
        _ <- Common.insertData(
          dbName,
          sourceConnection,
          streamingStreamContext.source.configuration.tableName,
          streamingData
        )

        exitVal <- runner.runOrFail(Duration.fromSeconds(5)).exit
      yield exitVal.causeOption match
        case Some(Cause.Fail(value, _)) =>
          assertTrue(value.squash.getMessage.contains("Target contains invalid watermark: 'null'"))
        case _ => assertTrue(false) // unexpected: it succeeded or timed out
    },
    test("stream, backfill and stream again successfully") {
      for
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

        // Testing the stream runner in the streaming mode
        insertRunner <- Common.getTestApp(Duration.fromSeconds(10), streamingStreamContextLayer).fork
        _ <- Common.insertData(
          dbName,
          sourceConnection,
          streamingStreamContext.source.configuration.tableName,
          streamingData
        )

        _ <- insertRunner.runOrFail(Duration.fromSeconds(5))

        afterStream <- readTarget(streamingStreamContext.sink.targetTableFullName, "Id, Name", IntStrDecoder)

        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")

        // Testing the stream runner in the backfill mode
        backfillRunner <- Common.getTestApp(Duration.fromSeconds(10), streamingStreamContextLayer).fork
        _ <- Common.insertData(
          dbName,
          sourceConnection,
          streamingStreamContext.source.configuration.tableName,
          backfillData
        )

        _ <- backfillRunner.runOrFail(Duration.fromSeconds(5))

        afterBackfill <- readTarget(streamingStreamContext.sink.targetTableFullName, "Id, Name", IntStrDecoder)

        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "false")
        // Testing the update and delete operations
        deleteUpdateRunner <- Common.getTestApp(Duration.fromSeconds(10), streamingStreamContextLayer).fork
        _ <- Common.updateData(
          dbName,
          sourceConnection,
          streamingStreamContext.source.configuration.tableName,
          updatedData
        )
        _ <- ZIO.sleep(Duration.fromSeconds(5))
        _ <- Common.deleteData(
          dbName,
          sourceConnection,
          streamingStreamContext.source.configuration.tableName,
          deletedData
        )

        _ <- deleteUpdateRunner.runOrFail(Duration.fromSeconds(10))

        afterUpdateDelete <- readTarget(
          streamingStreamContext.sink.targetTableFullName,
          "Id, Name",
          IntStrDecoder
        )

        watermark <- getWatermark(streamingStreamContext.sink.targetTableFullName.split('.').last)(MsSqlWatermark.rw)
        latestVersion <- Common.getChangeTrackingVersion(dbName, sourceConnection)
      yield assertTrue(afterStream.sorted == streamingData.sorted) implies assertTrue(
        afterBackfill.sorted == (streamingData ++ backfillData).sorted
      ) implies assertTrue(afterUpdateDelete.sorted == resultData.sorted) implies assertTrue(
        watermark.version.toLong == latestVersion
      )
    }
  ) @@ before @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
