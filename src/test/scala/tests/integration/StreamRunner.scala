package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.{SqlServerChangeTrackingStreamContext, StreamSpec, given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions}
import tests.common.{Common, TimeLimitLifetimeService}

import com.sneaksanddata.arcane.framework.services.mssql.*
import tests.integration.Fixtures.withFreshTables

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import zio.{Runtime, Unsafe, ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration

class StreamRunner  extends AsyncFlatSpec with Matchers:

  private val runtime = Runtime.default

  private val streamContextStr = """
    |
    | {
    |  "groupingIntervalSeconds": 1,
    |  "lookBackInterval": 21000,
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
    |    "targetTableName": "iceberg.test.test"
    |  },
    |  "sourceSettings": {
    |    "changeCaptureIntervalSeconds": 1,
    |    "commandTimeout": 3600,
    |    "schema": "dbo",
    |    "table": "TestTable"
    |   },
    |  "stagingDataSettings": {
    |    "catalog": {
    |      "catalogName": "iceberg",
    |      "catalogUri": "http://localhost:20001/catalog",
    |      "namespace": "test",
    |      "schemaName": "test",
    |      "warehouse": "demo"
    |    },
    |    "maxRowsPerFile": 1,
    |    "tableNamePrefix": "staging_integration_tests"
    |  },
    |  "fieldSelectionRule": {
    |    "ruleType": "all",
    |    "fields": []
    |  }
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

    val streamingData = List.range(1, 3).map(i => (i, s"Test$i"))
    val backfillData = List.range(4, 7).map(i => (i, s"Test$i"))

    val updatedData = List.range(4, 7).map(i => (i, s"Update$i"))
    val deletedData = List(5)

    val resultSetDecoder = (rs: ResultSet) => (rs.getInt(1), rs.getString(2))

    val test = for
      // Testing the stream runner in the streaming mode
      streamRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
      _ <- Common.insertData(sourceConnection, "dbo.TestTable", streamingData)
      _ <- streamRunner.await.timeout(Duration.ofSeconds(15))
      afterStream <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", resultSetDecoder)

      // Testing the stream runner in the backfill mode
      streamRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, backfillStreamContextLayer).fork
      _ <- Common.insertData(sourceConnection, "dbo.TestTable", backfillData)
      _ <- streamRunner.await.timeout(Duration.ofSeconds(15))
      afterBackfill <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", resultSetDecoder)

      // Testing the update and delete operations
      streamRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
      _ <- Common.updateData(sourceConnection, updatedData)
      _ <- Common.deleteData(sourceConnection, deletedData)
      _ <- zlog("data deleted")
      _ <- streamRunner.await.timeout(Duration.ofSeconds(15))
      afterUpdateDelete <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", resultSetDecoder)
    yield (afterStream, afterBackfill, afterUpdateDelete)

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(test.timeout(testTimeout))).map {
      case None => fail("Test timed out")
      case Some(afterStream, afterBackfill, afterUpdateDelete) =>
        val cp = new Checkpoint()
        cp { afterStream.sorted should equal(streamingData.sorted) }
        cp { afterBackfill.sorted should equal((streamingData ++ backfillData).sorted) }
        cp { afterUpdateDelete.sorted should equal( streamingData ++ updatedData.filterNot(e => deletedData.contains(e._1)).sorted) }
        cp.reportAll()
        succeed
    }
  }
