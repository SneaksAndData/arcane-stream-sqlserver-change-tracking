package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.{
  SqlServerChangeTrackingStreamContext,
  StreamSpec,
  given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions
}
import tests.common.{Common, TimeLimitLifetimeService}

import com.sneaksanddata.arcane.framework.services.mssql.*
import tests.integration.Fixtures.withFreshTables

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Runtime, Scope, Unsafe, ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration
import scala.language.postfixOps

object StreamRunner extends ZIOSpecDefault:

  val sourceTableName = "StreamRunner"
  val targetTableName = "iceberg.test.stream_run"

  private val streamContextStr = s"""
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
    |    "targetTableName": "$targetTableName"
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

  private val parsedSpec = StreamSpec.fromString(streamContextStr)

  private val streamingStreamContext = new SqlServerChangeTrackingStreamContext(parsedSpec):
    override val IsBackfilling: Boolean = false

  private val backfillStreamContext = new SqlServerChangeTrackingStreamContext(parsedSpec):
    override val IsBackfilling: Boolean = true

  private val streamingStreamContextLayer = ZLayer.succeed[SqlServerChangeTrackingStreamContext](streamingStreamContext)
    ++ ZLayer.succeed[ConnectionOptions](streamingStreamContext)

  private val backfillStreamContextLayer = ZLayer.succeed[SqlServerChangeTrackingStreamContext](backfillStreamContext)
    ++ ZLayer.succeed[ConnectionOptions](backfillStreamContext)

  private val streamingData = List.range(1, 3).map(i => (i, s"Test$i"))
  private val backfillData  = List.range(4, 7).map(i => (i, s"Test$i"))

  private val updatedData = List.range(4, 7).map(i => (i, s"Update$i"))
  private val deletedData = List(5)

  private val resultData = streamingData ++ updatedData.filterNot(e => deletedData.contains(e._1))

  private def before = TestAspect.before(Fixtures.withFreshTablesZIO(sourceTableName, targetTableName))

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    test("stream, backfill and stream again successfully") {
      for
        sourceConnection <- ZIO.succeed(Fixtures.getConnection)
        // Testing the stream runner in the streaming mode
        insertRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
        _            <- Common.insertData(sourceConnection, parsedSpec.sourceSettings.table, streamingData)

        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          streamingData.length
        )
        _ <- insertRunner.await.timeout(Duration.ofSeconds(10))

        afterStream <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", Common.IntStrDecoder)

        // Testing the stream runner in the backfill mode
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, backfillStreamContextLayer).fork
        _              <- Common.insertData(sourceConnection, parsedSpec.sourceSettings.table, backfillData)
        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          backfillData.length + streamingData.length
        )

        _ <- backfillRunner.await.timeout(Duration.ofSeconds(10))

        afterBackfill <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", Common.IntStrDecoder)

        // Testing the update and delete operations
        deleteUpdateRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
        _                  <- Common.updateData(sourceConnection, parsedSpec.sourceSettings.table, updatedData)
        _                  <- ZIO.sleep(Duration.ofSeconds(1))
        _                  <- Common.deleteData(sourceConnection, parsedSpec.sourceSettings.table, deletedData)
        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          resultData.length
        )

        _ <- deleteUpdateRunner.await.timeout(Duration.ofSeconds(10))

        afterUpdateDelete <- Common.getData(
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder
        )
      yield assertTrue(afterStream.sorted == streamingData.sorted) implies assertTrue(
        afterBackfill.sorted == (streamingData ++ backfillData).sorted
      ) implies assertTrue(afterUpdateDelete.sorted == resultData.sorted)
    }
  ) @@ before @@ timeout(zio.Duration.fromSeconds(120)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
