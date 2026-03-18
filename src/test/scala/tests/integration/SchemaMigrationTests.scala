package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import models.app.MicrosoftSqlServerPluginStreamContext
import tests.common.Common
import tests.integration.Fixtures.initialSchema

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, Field}
import com.sneaksanddata.arcane.framework.services.iceberg.base.SinkEntityManager
import com.sneaksanddata.arcane.framework.services.mssql.versioning.MsSqlWatermark
import com.sneaksanddata.arcane.framework.testkit.iceberg.TestEntityManager
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  IntStrStrDecoder,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Duration, Scope, ZIO, ZLayer}

import java.sql.ResultSet

object SchemaMigrationTests extends ZIOSpecDefault:
  val sourceTableName = "SchemaEvolutionTests"
  val targetTableName = "iceberg.test.schema_evolution"

  private def getStreamContext = MicrosoftSqlServerPluginStreamContext(streamContextStr)

  private def getStreamContextLayer =
    ZLayer.succeed[MicrosoftSqlServerPluginStreamContext](getStreamContext)

  private val dbName = "SchemaMigrationTests"

  private val streamContextStr =
    s"""
       {
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
       |      "isUnifiedSchema": false
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
       |        "meanStringTypeSizeEstimate": 50,
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

  private def before = TestAspect.before(Fixtures.withFreshTablesZIO(dbName, sourceTableName, targetTableName))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("SchemaMigrationTests")(
    test("handle the schema migration (column insertions)") {
      val streamingData  = List.range(1, 4).map(i => (i, s"Test$i"))
      val afterEvolution = List.range(4, 7).map(i => (i, s"Test$i", s"Updated $i"))

      val afterEvolutionExpected = List.range(1, 4).map(i => (i, s"Test$i", null))
        ++ List.range(4, 7).map(i => (i, s"Test$i", s"Updated $i"))

      for {
        context <- ZIO.succeed(getStreamContext)
        fakeSchema = ArcaneSchema(Seq(Field("test", StringType)))
        entityManager <- ZIO.service[SinkEntityManager]
        _ <- prepareWatermark(
          targetTableName.split("\\.").last,
          fakeSchema,
          MsSqlWatermark.epoch
        )
        // manually migrate
        _ <- entityManager.migrateSchema(fakeSchema, initialSchema, targetTableName.split("\\.").last)

        sourceConnection <- ZIO.succeed(Fixtures.getConnection)

        // launch stream and wait for it to create target with streamingData rows (initial table)
        streamRunner <- Common.getTestApp(Duration.fromSeconds(15), getStreamContextLayer).fork
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
          context.sink.targetTableFullName,
          "Id, Name, NewName",
          (rs: ResultSet) => (rs.getInt(1), rs.getString(2), rs.getString(3))
        )
      } yield assertTrue(
        afterStream.sorted == afterEvolutionExpected
      )
    }.provideLayer(TestEntityManager.sinkEntityManagerLayer),
    test("handle the schema migration (column deletions)") {
      val streamingData  = List.range(1, 4).map(i => (i, s"Test$i", s"Updated $i"))
      val afterEvolution = List.range(4, 7).map(i => (i, s"Test$i"))

      val afterEvolutionExpected = streamingData ++ List.range(4, 7).map(i => (i, s"Test$i", null))

      for {
        context <- ZIO.succeed(getStreamContext)
        fakeSchema = ArcaneSchema(Seq(Field("test", StringType)))
        entityManager <- ZIO.service[SinkEntityManager]
        _ <- prepareWatermark(
          targetTableName.split("\\.").last,
          fakeSchema,
          MsSqlWatermark.epoch
        )
        // manually migrate
        _ <- entityManager.migrateSchema(fakeSchema, initialSchema, targetTableName.split("\\.").last)

        sourceConnection <- ZIO.succeed(Fixtures.getConnection)
        _                <- Common.addColumns(dbName, sourceConnection, sourceTableName, "NewName VARCHAR(100)")

        streamRunner <- Common.getTestApp(Duration.fromSeconds(180), getStreamContextLayer).fork
        _            <- Common.insertUpdatedData(dbName, sourceConnection, sourceTableName, streamingData)

        _ <- ZIO.sleep(Duration.fromSeconds(10))

        _ <- Common.removeColumns(dbName, sourceConnection, sourceTableName, "NewName")
        _ <- Common.waitForColumns(dbName, sourceConnection, sourceTableName, 2)

        _ <- Common.insertData(dbName, sourceConnection, sourceTableName, afterEvolution)

        _ <- streamRunner.runOrFail(Duration.fromSeconds(10))

        afterEvolution <- readTarget(
          context.sink.targetTableFullName,
          "Id, Name, NewName",
          IntStrStrDecoder
        )

      } yield assertTrue(
        afterEvolution.sorted == afterEvolutionExpected
      )
    }.provideLayer(TestEntityManager.sinkEntityManagerLayer)
  ) @@ before @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
