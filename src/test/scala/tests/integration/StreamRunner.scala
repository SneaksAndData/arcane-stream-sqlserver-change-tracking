package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.{SqlServerChangeTrackingStreamContext, StreamSpec, given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions}
import tests.common.{Common, TimeLimitLifetimeService}

import com.sneaksanddata.arcane.framework.services.mssql.*
import tests.integration.Fixtures.withFreshTables

import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import zio.{Runtime, Unsafe, ZIO, ZLayer}

import java.time.Duration

class StreamRunner  extends AsyncFlatSpec with Matchers:

  private val runtime = Runtime.default

  private val streamContextStr = """
    |
    | {
    |  "database": "IntegrationTests",
    |  "schema": "dbo",
    |  "table": "TestTable",
    |  "commandTimeout": 3600,
    |  "backfillJobTemplateRef": {
    |    "apiGroup": "streaming.sneaksanddata.com",
    |    "kind": "StreamingJobTemplate",
    |    "name": "arcane-stream-microsoft-synapse-link-large-job"
    |  },
    |  "groupingIntervalSeconds": 1,
    |  "groupsPerFile": 1,
    |  "httpClientMaxRetries": 3,
    |  "httpClientRetryDelaySeconds": 1,
    |  "jobTemplateRef": {
    |    "apiGroup": "streaming.sneaksanddata.com",
    |    "kind": "StreamingJobTemplate",
    |    "name": "arcane-stream-microsoft-synapse-link-standard-job"
    |  },
    |  "lookBackInterval": 21000,
    |  "tableProperties": {
    |    "partitionExpressions": [],
    |    "format": "PARQUET",
    |    "sortedBy": [],
    |    "parquetBloomFilterColumns": []
    |  },
    |  "rowsPerGroup": 10000,
    |  "sinkSettings": {
    |    "archiveTableName": "iceberg.test.archive_test",
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
    |    "targetTableName": "iceberg.test.test"
    |  },
    |  "sourceSettings": {
    |    "baseLocation": "abfss://cdm-e2e@devstoreaccount1.dfs.core.windows.net/",
    |    "changeCaptureIntervalSeconds": 1,
    |    "changeCapturePeriodSeconds": 60,
    |    "name": "synapsetable"
    |  },
    |  "stagingDataSettings": {
    |    "catalog": {
    |      "catalogName": "iceberg",
    |      "catalogUri": "http://localhost:20001/catalog",
    |      "namespace": "test",
    |      "schemaName": "test",
    |      "warehouse": "demo"
    |    },
    |    "dataLocation": "s3://tmp/initial-warehouse",
    |    "tableNamePrefix": "staging_inventtrans"
    |  },
    |  "fieldSelectionRule": {
    |    "ruleType": "all",
    |    "fields": []
    |  },
    |  "backfillBehavior": "overwrite",
    |  "backfillStartDate": "2025-03-04T07.00.00Z"
    |}
    |
    |""".stripMargin



  it should "run a stream in backfill mode" in withFreshTables("TestTable", "iceberg.test.test") { sourceConnection =>
    val testTimeout = Duration.ofSeconds(600)

    val parsedSpec = StreamSpec.fromString(streamContextStr)
    val streamingStreamContext = new SqlServerChangeTrackingStreamContext(parsedSpec):
      override val IsBackfilling: Boolean = false

    val backfillStreamContext = new SqlServerChangeTrackingStreamContext(parsedSpec):
      override val IsBackfilling: Boolean = true

    val streamingStreamContextLayer = ZLayer.succeed[SqlServerChangeTrackingStreamContext](streamingStreamContext)
      ++ ZLayer.succeed[ConnectionOptions](streamingStreamContext)

    val backfillStreamContextLayer = ZLayer.succeed[SqlServerChangeTrackingStreamContext](streamingStreamContext)
      ++ ZLayer.succeed[ConnectionOptions](streamingStreamContext)

    val timeLimiter = new TimeLimitLifetimeService(Duration.ofSeconds(15))

    val streamingData = List.range(1, 3).map(i => (i, s"Test$i"))
    val backfillData = List.range(4, 7).map(i => (i, s"Test$i"))

    val updatedData = List.range(4, 7).map(i => (i, s"Update$i"))
    val deletedData = List(5)

    val test = for
      // Testing the stream runner in the streaming mode
      streamRunner <- Common.createTestApp(ZLayer.succeed(timeLimiter), streamingStreamContextLayer).fork
      _ <- Common.insertData(sourceConnection, streamingData)
      _ <- streamRunner.await.timeout(Duration.ofSeconds(30))
      afterStream <- Common.getData(streamingStreamContext.targetTableFullName)

      // Testing the stream runner in the backfill mode
      streamRunner <- Common.createTestApp(ZLayer.succeed(timeLimiter), backfillStreamContextLayer).fork
      _ <- Common.insertData(sourceConnection, backfillData)
      _ <- streamRunner.await.timeout(Duration.ofSeconds(30))
      afterBackfill <- Common.getData(streamingStreamContext.targetTableFullName)

      // Testing the stream runner in the backfill mode
      streamRunner <- Common.createTestApp(ZLayer.succeed(timeLimiter), streamingStreamContextLayer).fork
      _ <- Common.updateData(sourceConnection, updatedData)
      _ <- ZIO.sleep(Duration.ofSeconds(5))
      _ <- Common.deleteData(sourceConnection, deletedData)
      _ <- ZIO.sleep(Duration.ofSeconds(5))
      _ <- streamRunner.await.timeout(Duration.ofSeconds(30))
      afterUpdateDelete <- Common.getData(streamingStreamContext.targetTableFullName)
    yield (afterStream, afterBackfill, afterUpdateDelete)

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(test.timeout(testTimeout))).map {
      case None => fail("Test timed out")
      case Some(afterStream, afterBackfill, afterUpdateDelete) =>
        val cp = new Checkpoint()
        cp { afterStream should equal(streamingData) }
        cp { afterBackfill should equal(streamingData ++ backfillData) }
        cp { afterUpdateDelete should equal(backfillData ++ updatedData.filter(e => !deletedData.contains(e._1))) }
        cp.reportAll()
        succeed
    }
  }

class TestStreamContext(spec: StreamSpec) extends SqlServerChangeTrackingStreamContext(spec):
  override val IsBackfilling: Boolean = false
