package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{BackfillBehavior, BackfillSettings, FieldSelectionRule, FieldSelectionRuleSettings, GroupingSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, StagingDataSettings, TableFormat, TableMaintenanceSettings, TablePropertiesSettings, TargetTableSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergCatalogCredential
import com.sneaksanddata.arcane.framework.services.lakehouse.base.{IcebergCatalogSettings, S3CatalogFileIO}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClientOptions
import com.sneaksanddata.arcane.framework.services.mssql.ConnectionOptions
import zio.ZLayer

import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.util.Try

given Conversion[TablePropertiesSettingsSpec, TablePropertiesSettings] with
  def apply(x: TablePropertiesSettingsSpec): TablePropertiesSettings = new TablePropertiesSettings:
    val partitionExpressions: Array[String] = x.partitionExpressions
    val format: TableFormat = TableFormat.valueOf(x.format)
    val sortedBy: Array[String] = x.sortedBy
    val parquetBloomFilterColumns: Array[String] = x.parquetBloomFilterColumns

/**
 * The context for the SQL Server Change Tracking stream.
 * @param spec The stream specification
 */
case class SqlServerChangeTrackingStreamContext(spec: StreamSpec) extends StreamContext
  with GroupingSettings
  with IcebergCatalogSettings
  with JdbcMergeServiceClientOptions
  with VersionedDataGraphBuilderSettings
  with TargetTableSettings
  with StagingDataSettings
  with TablePropertiesSettings
  with BackfillSettings
  with FieldSelectionRuleSettings:

  override val rowsPerGroup: Int = spec.rowsPerGroup
  override val lookBackInterval: Duration = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.sourceSettings.changeCaptureIntervalSeconds)
  override val changeCapturePeriod: Duration = Duration.ofSeconds(1)
  override val groupingInterval: Duration = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String = spec.stagingDataSettings.catalog.namespace
  override val warehouse: String = spec.stagingDataSettings.catalog.warehouse
  override val catalogUri: String = spec.stagingDataSettings.catalog.catalogUri
  override val stagingLocation: Option[String] = spec.stagingDataSettings.dataLocation

  override val stagingCatalogName: String = spec.stagingDataSettings.catalog.catalogName
  override val stagingSchemaName: String = spec.stagingDataSettings.catalog.schemaName

  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => Map()
    case None => IcebergCatalogCredential.oAuth2Properties

  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  val connectionString: String = sys.env("ARCANE_CONNECTIONSTRING")

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.sinkSettings.targetTableName

  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings:
    override val targetOptimizeSettings: Option[OptimizeSettings] = Some(new OptimizeSettings {
      override val batchThreshold: Int = spec.sinkSettings.optimizeSettings.batchThreshold
      override val fileSizeThreshold: String = spec.sinkSettings.optimizeSettings.fileSizeThreshold
    })

    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(new SnapshotExpirationSettings {
      override val batchThreshold: Int = spec.sinkSettings.snapshotExpirationSettings.batchThreshold
      override val retentionThreshold: String = spec.sinkSettings.snapshotExpirationSettings.retentionThreshold
    })

    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(new OrphanFilesExpirationSettings {
      override val batchThreshold: Int = spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold
      override val retentionThreshold: String = spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold

    })

  override val stagingTablePrefix: String = spec.stagingDataSettings.tableNamePrefix

  val partitionExpressions: Array[String] = spec.tableProperties.partitionExpressions
  val tableProperties: TablePropertiesSettings = spec.tableProperties

  val stagingCatalog: String = s"${spec.stagingDataSettings.catalog.catalogName}.${spec.stagingDataSettings.catalog.schemaName}"

  val format: TableFormat = TableFormat.valueOf(spec.tableProperties.format)
  val sortedBy: Array[String] = spec.tableProperties.sortedBy
  val parquetBloomFilterColumns: Array[String] = spec.tableProperties.parquetBloomFilterColumns

  override val backfillTableFullName: String = s"$stagingCatalog.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}".replace('-', '_')

  override val rule: FieldSelectionRule = spec.fieldSelectionRule.ruleType match
    case "include" => FieldSelectionRule.IncludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case "exclude" => FieldSelectionRule.ExcludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case _ => FieldSelectionRule.AllFields

  override val backfillBehavior: BackfillBehavior = spec.backfillBehavior match
    case "merge" => BackfillBehavior.Merge
    case "overwrite" => BackfillBehavior.Overwrite
    case _ => throw new IllegalArgumentException(s"Unknown backfill behavior: ${spec.backfillBehavior}")

  override val backfillStartDate: Option[OffsetDateTime] = parseBackfillStartDate(spec.backfillStartDate)

  private def parseBackfillStartDate(str: String): Option[OffsetDateTime] =
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ss'Z'").withZone(ZoneOffset.UTC)
    Try(OffsetDateTime.parse(str, formatter)) match
      case scala.util.Success(value) => Some(value)
      case scala.util.Failure(e) => throw new IllegalArgumentException(s"Invalid backfill start date: $str. The backfill start date must be in the format 'yyyy-MM-dd'T'HH.mm.ss'Z'", e)

given Conversion[SqlServerChangeTrackingStreamContext, ConnectionOptions] with
  def apply(context: SqlServerChangeTrackingStreamContext): ConnectionOptions =
    ConnectionOptions(context.connectionString,
      context.spec.sourceSettings.database,
      context.spec.sourceSettings.schema,
      context.spec.sourceSettings.table,
      None)

object SqlServerChangeTrackingStreamContext:
  type Environment = StreamContext
    & ConnectionOptions
    & GroupingSettings
    & IcebergCatalogSettings
    & JdbcMergeServiceClientOptions
    & VersionedDataGraphBuilderSettings
    & TargetTableSettings
    & StagingDataSettings
    & TablePropertiesSettings
    & BackfillSettings
    & FieldSelectionRuleSettings

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
    .fromEnvironment("STREAMCONTEXT__SPEC")
    .map(combineSettingsLayer)
    .getOrElse(ZLayer.fail(new Exception("The stream context is not specified.")))

  private def combineSettingsLayer(spec: StreamSpec): ZLayer[Any, Throwable, Environment] =
    val context = SqlServerChangeTrackingStreamContext(spec)

    ZLayer.succeed(context) ++ ZLayer.succeed[ConnectionOptions](context)
