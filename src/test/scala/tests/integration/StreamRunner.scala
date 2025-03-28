package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.{SqlServerChangeTrackingStreamContext, StreamSpec, given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions}
import tests.common.{Common, TimeLimitLifetimeService}

import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.sql_server_change_tracking.tests.integration.Fixtures.withFreshTable
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
    |    "changeCaptureIntervalSeconds": 5,
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



  it should "run a stream in backfill mode" in withFreshTable("TestTable") { case (connection, tableName) =>
    val parsedSpec = StreamSpec.fromString(streamContextStr)
    val streamingStreamContext = new SqlServerChangeTrackingStreamContext(parsedSpec):
      override val IsBackfilling: Boolean = false

    val backfillStreamContext = new SqlServerChangeTrackingStreamContext(parsedSpec):
      override val IsBackfilling: Boolean = true

    val streamingStreamContextLayer = ZLayer.succeed[SqlServerChangeTrackingStreamContext](streamingStreamContext)
      ++ ZLayer.succeed[ConnectionOptions](streamingStreamContext)

    val timeLimiter = new TimeLimitLifetimeService(Duration.ofSeconds(3))

    val streamingData = List.range(1, 100).map(i => (i, s"Test$i"))
    val test = for
      streamRunner <- Common.createTestApp(ZLayer.succeed(timeLimiter), streamingStreamContextLayer).fork
      _ <- Common.insertData(connection, streamingData)
      _ <- streamRunner.await
      afterInsert <- Common.getData(streamingStreamContext.targetTableFullName)
    yield afterInsert

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(test.timeout(Duration.ofSeconds(10)))).map { result =>
      // Assert
      result.get should contain theSameElementsAs streamingData
    }
  }

class TestStreamContext(spec: StreamSpec) extends SqlServerChangeTrackingStreamContext(spec):
  override val IsBackfilling: Boolean = false
