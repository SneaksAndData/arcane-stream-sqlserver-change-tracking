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
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.{liveSeed, runOrFail}
import org.scalatest.matchers.should.Matchers.should
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, TestSystem, ZIOSpecDefault, assertTrue}
import zio.{Cause, Duration, Scope, Unsafe, ZIO, ZLayer}

import java.util.UUID
import scala.language.postfixOps
import scala.util.Random

object StreamRunner extends ZIOSpecDefault:

  private val dbName = "StreamRunnerTests"

  private def getStreamContextStr(sourceTableName: String, targetTableName: String) = s"""
                                    |{
                                    |  "observability": {
                                    |    "metricTags": {}
                                    |  },
                                    |  "staging": {
                                    |    "table": {
                                    |      "maxRowsPerFile": 10000,
                                    |      "stagingCatalogName": "iceberg",
                                    |      "stagingSchemaName": "test",
                                    |      "isUnifiedSchema": true
                                    |    },
                                    |    "icebergCatalog": {
                                    |      "catalogProperties": {},
                                    |      "catalogUri": "http://localhost:20001/catalog",
                                    |      "namespace": "test",
                                    |      "warehouse": "demo"
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
                                    |      "changeCaptureJitterSeed": 0,
                                    |      "changeCaptureRangeLimit": 10
                                    |    }
                                    |  },
                                    |  "sink": {
                                    |    "mergeServiceClient": {
                                    |      "connectionUrl": "jdbc:trino://localhost:8080",
                                    |      "credentialType": {
                                    |          "basic": {}
                                    |      },
                                    |      "extraConnectionParameters": {
                                    |        "clientTags": "test"
                                    |      },
                                    |      "queryRetryMode": {
                                    |        "never": {}
                                    |      },
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
                                    |      "warehouse": "demo"
                                    |    }
                                    |  },
                                    |  "throughput": {
                                    |    "shaperImpl": {
                                    |      "memoryBound": {
                                    |        "fallbackStringTypeSizeEstimate": 50,
                                    |        "objectTypeSizeEstimate": 4096,
                                    |        "chunkCostScale": 1,
                                    |        "chunkCostMax": 2,
                                    |        "tableRowCountWeight": 0.05,
                                    |        "tableSizeWeight": 0.05,
                                    |        "tableSizeScaleFactor": 2,
                                    |        "chunkSizeCap": 1000000,
                                    |        "maxStatisticsAge": 604800
                                    |      }
                                    |    },
                                    |    "advisedRate": "1000 per 1 second",
                                    |    "advisedBurst": 1000,
                                    |    "advisedChunkSize": 1
                                    |  },
                                    |  "source": {
                                    |    "configuration": {
                                    |      "extraConnectionParameters": {
                                    |        "databaseName": "$dbName"
                                    |      },
                                    |      "schemaName": "dbo",
                                    |      "backfillShardSchemaName": "shards",
                                    |      "tableName": "$sourceTableName",
                                    |      "fetchSize": 128
                                    |    },
                                    |    "buffering": {
                                    |      "enabled": false,
                                    |      "strategy": {}
                                    |    },
                                    |    "modifications": {
                                    |      "modifications": {}
                                    |    }
                                    |  }
                                    |}""".stripMargin

  private def getStreamingStreamContext(source: String, target: String) = MicrosoftSqlServerPluginStreamContext(
    getStreamContextStr(source, target)
  )
  private def getStreamingStreamContextLayer(source: String, target: String) =
    ZLayer.succeed[MicrosoftSqlServerPluginStreamContext](getStreamingStreamContext(source, target)) ++ ZLayer
      .succeed[DatagramSocketConfig](getStreamingStreamContext(source, target)) ++ ZLayer
      .succeed[MetricsConfig](getStreamingStreamContext(source, target)) ++ ZLayer.succeed(DatadogPublisherConfig())

  private val streamingData = List.range(1, 3).map(i => (i, s"Test$i"))
  private val backfillData  = List.range(4, 7).map(i => (i, s"Test$i"))

  private val updatedData = List.range(4, 7).map(i => (i, s"Update$i"))
  private val deletedData = List(5)

  private val resultData = streamingData ++ updatedData.filterNot(e => deletedData.contains(e._1))

  private def before(source: String, target: String) =
    TestAspect.before(liveSeed *> Fixtures.withFreshTablesZIO(dbName, source, target))

  private def runMigration(source: String, target: String) =
    for
      _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "migrate")
      migrator <- Common.getTestApp(Duration.fromSeconds(10), getStreamingStreamContextLayer(source, target)).fork
      _        <- migrator.runOrFail(Duration.fromSeconds(5))
      _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "stream")
    yield ()

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    {
      val runId      = UUID.randomUUID().toString.replace("-", "_")
      val sourceName = s"stream_run_$runId"
      val targetName = s"iceberg.test.$sourceName"

      test("stream, backfill and stream again successfully") {
        for
          _ <- prepareWatermark(
            targetName.split("\\.").last,
            initialSchema,
            MsSqlWatermark.epoch
          )
          _ <- runMigration(sourceName, targetName)

          sourceConnection <- ZIO.succeed(Fixtures.getConnection)
          context          <- ZIO.succeed(getStreamingStreamContext(sourceName, targetName))
          layer            <- ZIO.succeed(getStreamingStreamContextLayer(sourceName, targetName))

          // Testing the stream runner in the streaming mode
          insertRunner <- Common.getTestApp(Duration.fromSeconds(10), layer).fork
          _ <- Common.insertData(
            dbName,
            sourceConnection,
            context.source.configuration.tableName,
            streamingData
          )

          _ <- insertRunner.runOrFail(Duration.fromSeconds(5))

          afterStream <- readTarget(context.sink.targetTableFullName, "Id, Name", IntStrDecoder)

          _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
          _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL_ID", Random.alphanumeric.take(10).mkString(""))

          // Testing the stream runner in the backfill mode
          backfillRunner <- Common.getTestApp(Duration.fromSeconds(10), layer).fork
          _ <- Common.insertData(
            dbName,
            sourceConnection,
            context.source.configuration.tableName,
            backfillData
          )

          _ <- backfillRunner.runOrFail(Duration.fromSeconds(5))

          afterBackfill <- readTarget(context.sink.targetTableFullName, "Id, Name", IntStrDecoder)

          _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "false")
          // Testing the update and delete operations
          deleteUpdateRunner <- Common.getTestApp(Duration.fromSeconds(10), layer).fork
          _ <- Common.updateData(
            dbName,
            sourceConnection,
            context.source.configuration.tableName,
            updatedData
          )
          _ <- ZIO.sleep(Duration.fromSeconds(5))
          _ <- Common.deleteData(
            dbName,
            sourceConnection,
            context.source.configuration.tableName,
            deletedData
          )

          _ <- deleteUpdateRunner.runOrFail(Duration.fromSeconds(10))

          afterUpdateDelete <- readTarget(
            context.sink.targetTableFullName,
            "Id, Name",
            IntStrDecoder
          )

          watermark <- getWatermark(context.sink.targetTableFullName.split('.').last)(using
            MsSqlWatermark.rw
          )
          latestVersion <- Common.getChangeTrackingVersion(dbName, sourceConnection)
        yield assertTrue(afterStream.sorted == streamingData.sorted) implies assertTrue(
          afterBackfill.sorted == (streamingData ++ backfillData).sorted
        ) implies assertTrue(afterUpdateDelete.sorted == resultData.sorted) implies assertTrue(
          watermark.version.toLong == latestVersion
        )
      } @@ before(sourceName, targetName)
    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock
