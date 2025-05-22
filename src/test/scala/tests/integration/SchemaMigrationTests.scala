package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.{
  SqlServerChangeTrackingStreamContext,
  StreamSpec,
  given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions
}
import tests.common.{Common, TimeLimitLifetimeService}

import com.sneaksanddata.arcane.framework.services.mssql.ConnectionOptions
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration

object SchemaMigrationTests extends ZIOSpecDefault:
  val sourceTableName = "SchemaEvolutionTests"
  val targetTableName = "iceberg.test.schema_evolution"

  private val streamContextStr =
    s"""
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
       |    "database": "IntegrationTests",
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

  private val streamingStreamContextLayer = ZLayer.succeed[SqlServerChangeTrackingStreamContext](streamingStreamContext)
    ++ ZLayer.succeed[ConnectionOptions](streamingStreamContext)

  private def before = TestAspect.before(Fixtures.withFreshTablesZIO(sourceTableName, targetTableName))

  def spec: Spec[TestEnvironment & Scope, Throwable] = suite("SchemaMigrationTests")(
    test("handle the schema migration (column insertions)") {
      val streamingData  = List.range(1, 4).map(i => (i, s"Test$i"))
      val afterEvolution = List.range(4, 7).map(i => (i, s"Test$i", s"Updated $i"))

      val afterEvolutionExpected = List.range(1, 4).map(i => (i, s"Test$i", null))
        ++ List.range(4, 7).map(i => (i, s"Test$i", s"Updated $i"))

      for {
        sourceConnection <- ZIO.succeed(Fixtures.getConnection)

        lifetimeService = ZLayer.succeed(TimeLimitLifetimeService(Duration.ofSeconds(15)))
        // launch stream and wait for it to create target with streamingData rows (initial table)
        streamRunner <- Common.buildTestApp(lifetimeService, streamingStreamContextLayer).fork
        _            <- Common.insertData(sourceConnection, sourceTableName, streamingData)
        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          streamingData.length
        )

        // update SOURCE (SQL) schema with a new column
        _ <- Common.addColumns(sourceConnection, sourceTableName, "NewName VARCHAR(100)")
        // let it propagate
        _ <- Common.waitForColumns(sourceConnection, sourceTableName, 3)
        // INSERT data with a new schema
        _ <- Common.insertUpdatedData(sourceConnection, sourceTableName, afterEvolution)

        _ <- Common.waitForData[(Int, String, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name, NewName",
          Common.IntStrStrDecoder,
          streamingData.length + afterEvolution.length
        )

        // read target table after schema migration
        afterStream <- Common.getData(
          streamingStreamContext.targetTableFullName,
          "Id, Name, NewName",
          (rs: ResultSet) => (rs.getInt(1), rs.getString(2), rs.getString(3))
        )

        // overall test timeout
        _ <- streamRunner.await.timeout(Duration.ofSeconds(10))
      } yield assertTrue(
        afterStream.sorted == afterEvolutionExpected
      )
    },
    test("handle the schema migration (column deletions)") {
      val streamingData  = List.range(1, 4).map(i => (i, s"Test$i", s"Updated $i"))
      val afterEvolution = List.range(4, 7).map(i => (i, s"Test$i"))

      val afterEvolutionExpected = streamingData ++ List.range(4, 7).map(i => (i, s"Test$i", null))

      for {
        sourceConnection <- ZIO.succeed(Fixtures.getConnection)
        _                <- Common.addColumns(sourceConnection, sourceTableName, "NewName VARCHAR(100)")

        lifetimeService = ZLayer.succeed(TimeLimitLifetimeService(Duration.ofSeconds(35)))
        streamRunner <- Common.buildTestApp(lifetimeService, streamingStreamContextLayer).fork
        _            <- Common.insertUpdatedData(sourceConnection, sourceTableName, streamingData)
        _ <- Common.waitForData[(Int, String, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name, NewName",
          Common.IntStrStrDecoder,
          streamingData.length
        )

        _ <- Common.removeColumns(sourceConnection, sourceTableName, "NewName")
        _ <- Common.waitForColumns(sourceConnection, sourceTableName, 2)

        _ <- Common.insertData(sourceConnection, sourceTableName, afterEvolution)
        _ <- Common.waitForData[(Int, String, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name, NewName",
          Common.IntStrStrDecoder,
          streamingData.length + afterEvolution.length
        )

        afterEvolution <- Common.getData(
          streamingStreamContext.targetTableFullName,
          "Id, Name, NewName",
          Common.IntStrStrDecoder
        )

        _ <- streamRunner.await.timeout(Duration.ofSeconds(10))

      } yield assertTrue(
        afterEvolution.sorted == afterEvolutionExpected
      )
    }
  ) @@ before @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
