package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.common

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import models.app.{
  SqlServerChangeTrackingStreamContext,
  StreamSpec,
  given_Conversion_SqlServerChangeTrackingStreamContext_ConnectionOptions
}

import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, Field}
import com.sneaksanddata.arcane.framework.models.settings.{
  AnalyzeSettings,
  IcebergSinkSettings,
  IcebergStagingSettings,
  OptimizeSettings,
  OrphanFilesExpirationSettings,
  SinkSettings,
  SnapshotExpirationSettings,
  TableMaintenanceSettings
}
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergCatalogCredential,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager,
  given_Conversion_ArcaneSchema_Schema
}
import com.sneaksanddata.arcane.framework.services.iceberg.base.S3CatalogFileIO
import com.sneaksanddata.arcane.framework.services.mssql.versioning.MsSqlWatermark
import zio.{Cause, Fiber, Task, ZIO}

import java.time.Duration

object FrameworkCommon {
  def runOrFail(runner: Fiber.Runtime[Throwable, Unit], timeout: Duration): zio.ZIO[Any, Cause[Throwable], Unit] =
    for
      result <- runner.join.timeout(Duration.ofSeconds(10)).exit
      _      <- ZIO.when(result.causeOption.isDefined)(ZIO.fail(result.causeOption.get))
    yield ()

  /** [START] Code copied from the Framework */
  object IcebergCatalogInfo:
    val defaultNamespace  = "test"
    val defaultWarehouse  = "demo"
    val defaultCatalogUri = "http://localhost:20001/catalog"

    val defaultStagingSettings: IcebergStagingSettings = new IcebergStagingSettings:
      override val namespace: String  = defaultNamespace
      override val warehouse: String  = defaultWarehouse
      override val catalogUri: String = defaultCatalogUri
      override val additionalProperties: Map[String, String] =
        S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties
      override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
      override val stagingLocation: Option[String]  = None
      override val maxRowsPerFile: Option[Int]      = Some(1000)

    val defaultSinkSettings: IcebergSinkSettings = new IcebergSinkSettings:
      override val namespace: String                         = defaultNamespace
      override val warehouse: String                         = defaultWarehouse
      override val catalogUri: String                        = defaultCatalogUri
      override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties

  val writer: IcebergS3CatalogWriter = IcebergS3CatalogWriter(IcebergCatalogInfo.defaultStagingSettings)

  object EmptyTestTableMaintenanceSettings extends TableMaintenanceSettings:
    override val targetOptimizeSettings: Option[OptimizeSettings]                           = None
    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]       = None
    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None
    override val targetAnalyzeSettings: Option[AnalyzeSettings]                             = None

  class TestDynamicSinkSettings(name: String) extends SinkSettings:
    override val targetTableFullName: String                   = name
    override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings
    override val icebergSinkSettings: IcebergSinkSettings      = IcebergCatalogInfo.defaultSinkSettings

  def prepareWatermark(tableName: String): Task[Unit] = {
    val pm = IcebergTablePropertyManager(TestDynamicSinkSettings(tableName))
    for
      _ <- writer.createTable(tableName, ArcaneSchema(Seq(Field("test", StringType))), true)
      _ <- pm.comment(tableName, MsSqlWatermark.epoch.toJson)
    yield ()
  }

  /** [END] Code copied from the Framework */
}
