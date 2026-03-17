package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.MicrosoftSqlServerPluginStreamContext
import tests.common.Common
import tests.integration.Fixtures.initialSchema

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
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, TestSystem, ZIOSpecDefault, assertTrue}
import zio.{Cause, Duration, Scope, Unsafe, ZIO, ZLayer}

import scala.language.postfixOps

object StreamRunner extends ZIOSpecDefault:

  val sourceTableName = "StreamRunner"
  val targetTableName = "iceberg.test.stream_run"
  private val dbName  = "StreamRunnerTests"

  private val streamContextStr = s"""
                                    |       {
                                    |  "backfillJobTemplateRef": {
                                    |    "apiGroup": "streaming.sneaksanddata.com",
                                    |    "kind": "StreamingJobTemplate",
                                    |    "name": "arcane-stream-mssql-large-job"
                                    |  },
                                    |  "jobTemplateRef": {
                                    |    "apiGroup": "streaming.sneaksanddata.com",
                                    |    "kind": "StreamingJobTemplate",
                                    |    "name": "arcane-stream-mssql-standard-job"
                                    |  },
                                    |  "observability": {
                                    |    "metricTags": {}
                                    |  },
                                    |  "staging": {
                                    |    "table": {
                                    |      "stagingTablePrefix": "staging_mssql_test",
                                    |      "maxRowsPerFile": 10000,
                                    |      "stagingCatalogName": "iceberg",
                                    |      "stagingSchemaName": "test",
                                    |      "isUnifiedSchema": true
                                    |    },
                                    |    "icebergCatalog": {
                                    |      "catalogProperties": {},
                                    |      "catalogUri": "http://localhost:20001/catalog",
                                    |      "namespace": "test",
                                    |      "warehouse": "demo",
                                    |      "maxCatalogInstanceLifetime": "3600 second"
                                    |    }
                                    |  },
                                    |  "streamMode": {
                                    |    "backfill": {
                                    |      "backfillBehavior": "Overwrite",
                                    |      "backfillStartDate": "2026-01-01T00:00:00Z"
                                    |    },
                                    |    "changeCapture": {
                                    |      "changeCaptureInterval": "5 second",
                                    |      "changeCaptureJitterVariance": 0.1,
                                    |      "changeCaptureJitterSeed": 0
                                    |    }
                                    |  },
                                    |  "sink": {
                                    |    "mergeServiceClient": {
                                    |      "extraConnectionParameters": {
                                    |        "clientTags": "test"
                                    |      },
                                    |      "queryRetryMode": "Never",
                                    |      "queryRetryBaseDuration": "100 millisecond",
                                    |      "queryRetryOnMessageContents": [],
                                    |      "queryRetryScaleFactor": 0.1,
                                    |      "queryRetryMaxAttempts": 3
                                    |    },
                                    |    "targetTableProperties": {
                                    |      "format": "PARQUET",
                                    |      "sortedBy": [],
                                    |      "parquetBloomFilterColumns": []
                                    |    },
                                    |    "targetTableFullName": "$targetTableName",
                                    |    "maintenanceSettings": {
                                    |      "targetOptimizeSettings": {
                                    |        "batchThreshold": 60,
                                    |        "fileSizeThreshold": "512MB"
                                    |      },
                                    |      "targetOrphanFilesExpirationSettings": {
                                    |        "batchThreshold": 60,
                                    |        "retentionThreshold": "6h"
                                    |      },
                                    |      "targetSnapshotExpirationSettings": {
                                    |        "batchThreshold": 60,
                                    |        "retentionThreshold": "6h"
                                    |      },
                                    |      "targetAnalyzeSettings": {
                                    |        "includedColumns": [],
                                    |        "batchThreshold": 60
                                    |      }
                                    |    },
                                    |    "icebergCatalog": {
                                    |      "catalogProperties": {},
                                    |      "catalogUri": "http://localhost:20001/catalog",
                                    |      "namespace": "test",
                                    |      "warehouse": "demo",
                                    |      "maxCatalogInstanceLifetime": "3600 second"
                                    |    }
                                    |  },
                                    |  "throughput": {
                                    |    "shaperImpl": {
                                    |      "memoryBound": {
                                    |        "meanStringTypeSizeEstimate": 500,
                                    |        "meanObjectTypeSizeEstimate": 4096,
                                    |        "burstEstimateDivisionFactor": 2,
                                    |        "rateEstimateDivisionFactor": 2,
                                    |        "chunkCostScale": 1,
                                    |        "chunkCostMax": 2,
                                    |        "tableRowCountWeight": 0.5,
                                    |        "tableSizeWeight": 0.5,
                                    |        "tableSizeScaleFactor": 1
                                    |      },
                                    |      "static": null
                                    |    },
                                    |    "advisedRatePeriod": "1 second",
                                    |    "advisedChunksBurst": 1,
                                    |    "advisedChunkSize": 1,
                                    |    "advisedRateChunks": 1
                                    |  },
                                    |  "source": {
                                    |    "configuration": {
                                    |      "extraConnectionParameters": {
                                    |        "databaseName": "$dbName"
                                    |      },
                                    |      "connectionUrl": null,
                                    |      "schemaName": "dbo",
                                    |      "tableName": "$sourceTableName",
                                    |      "fetchSize": 128
                                    |    },
                                    |    "buffering": {
                                    |      "enabled": false,
                                    |      "strategy": {
                                    |        "unbounded": null,
                                    |        "buffered": null
                                    |      }
                                    |    },
                                    |    "fieldSelectionRule": {
                                    |      "essentialFields": [],
                                    |      "rule":{
                                    |        "all": {},
                                    |        "include": null,
                                    |        "exclude": null
                                    |      },
                                    |      "isServerSide": true
                                    |    }
                                    |  }
                                    |}""".stripMargin

  private val streamingStreamContext = MicrosoftSqlServerPluginStreamContext(streamContextStr)
  private val streamingStreamContextLayer =
    ZLayer.succeed[MicrosoftSqlServerPluginStreamContext](streamingStreamContext)

  private val streamingData = List.range(1, 3).map(i => (i, s"Test$i"))
  private val backfillData  = List.range(4, 7).map(i => (i, s"Test$i"))

  private val updatedData = List.range(4, 7).map(i => (i, s"Update$i"))
  private val deletedData = List(5)

  private val resultData = streamingData ++ updatedData.filterNot(e => deletedData.contains(e._1))

  private def before = TestAspect.before(Fixtures.withFreshTablesZIO(dbName, sourceTableName, targetTableName))

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    test("fail stream when watermark is not set") {
      for
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
        _ <- prepareWatermark(
          targetTableName.split("\\.").last,
          initialSchema,
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
