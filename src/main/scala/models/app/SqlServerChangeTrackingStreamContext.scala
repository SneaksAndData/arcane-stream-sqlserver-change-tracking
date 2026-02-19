package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.*
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergCatalogCredential
import com.sneaksanddata.arcane.framework.services.iceberg.base.S3CatalogFileIO
import com.sneaksanddata.arcane.framework.services.mssql.base.ConnectionOptions
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

import java.time.{Duration, OffsetDateTime}
import java.util.UUID

given Conversion[TablePropertiesSettingsSpec, TablePropertiesSettings] with
  def apply(x: TablePropertiesSettingsSpec): TablePropertiesSettings = new TablePropertiesSettings:
    val partitionExpressions: Array[String]      = x.partitionExpressions
    val format: TableFormat                      = TableFormat.valueOf(x.format)
    val sortedBy: Array[String]                  = x.sortedBy
    val parquetBloomFilterColumns: Array[String] = x.parquetBloomFilterColumns

/** The context for the SQL Server Change Tracking stream.
  * @param spec
  *   The stream specification
  */
case class SqlServerChangeTrackingStreamContext(spec: StreamSpec)
    extends StreamContext
    with GroupingSettings
    with IcebergStagingSettings
    with JdbcMergeServiceClientSettings
    with VersionedDataGraphBuilderSettings
    with SinkSettings
    with StagingDataSettings
    with TablePropertiesSettings
    with BackfillSettings
    with FieldSelectionRuleSettings
    with SourceBufferingSettings:

  override val rowsPerGroup: Int = sys.env.get("STREAMCONTEXT__ROWS_PER_GROUP") match
    case Some(value) => value.toInt
    case None        => spec.rowsPerGroup
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.sourceSettings.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration      = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String               = spec.stagingDataSettings.catalog.schemaName
  override val warehouse: String               = spec.stagingDataSettings.catalog.warehouse
  override val catalogUri: String              = spec.stagingDataSettings.catalog.catalogUri
  override val stagingLocation: Option[String] = spec.stagingDataSettings.dataLocation

  override val stagingCatalogName: String = spec.stagingDataSettings.catalog.catalogName
  override val stagingSchemaName: String  = spec.stagingDataSettings.catalog.schemaName

  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => S3CatalogFileIO.properties
    case None    => S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties

  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  val sourceConnectionString: String = sys.env("ARCANE__CONNECTIONSTRING")

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.sinkSettings.targetTableName

  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings:
    override val targetOptimizeSettings: Option[OptimizeSettings] = Some(new OptimizeSettings {
      override val batchThreshold: Int       = spec.sinkSettings.optimizeSettings.batchThreshold
      override val fileSizeThreshold: String = spec.sinkSettings.optimizeSettings.fileSizeThreshold
    })

    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(
      new SnapshotExpirationSettings {
        override val batchThreshold: Int        = spec.sinkSettings.snapshotExpirationSettings.batchThreshold
        override val retentionThreshold: String = spec.sinkSettings.snapshotExpirationSettings.retentionThreshold
      }
    )

    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(
      new OrphanFilesExpirationSettings {
        override val batchThreshold: Int        = spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold
        override val retentionThreshold: String = spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold

      }
    )

    override val targetAnalyzeSettings: Option[AnalyzeSettings] = Some(
      new AnalyzeSettings {
        override val batchThreshold: Int          = spec.sinkSettings.analyzeSettings.batchThreshold
        override val includedColumns: Seq[String] = spec.sinkSettings.analyzeSettings.includedColumns
      }
    )

  override val stagingTablePrefix: String = spec.stagingDataSettings.tableNamePrefix

  val partitionExpressions: Array[String]      = spec.tableProperties.partitionExpressions
  val tableProperties: TablePropertiesSettings = spec.tableProperties

  private val stagingCatalog: String =
    s"${spec.stagingDataSettings.catalog.catalogName}.${spec.stagingDataSettings.catalog.schemaName}"

  val format: TableFormat                      = TableFormat.valueOf(spec.tableProperties.format)
  val sortedBy: Array[String]                  = spec.tableProperties.sortedBy
  val parquetBloomFilterColumns: Array[String] = spec.tableProperties.parquetBloomFilterColumns

  override val backfillTableFullName: String =
    s"$stagingCatalog.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}".replace('-', '_')

  override val rule: FieldSelectionRule = spec.fieldSelectionRule.ruleType.toLowerCase match
    case "include" => FieldSelectionRule.IncludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case "exclude" => FieldSelectionRule.ExcludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case _         => FieldSelectionRule.AllFields

  override val isServerSide: Boolean = true

  override val essentialFields: Set[String] =
    Set("sys_change_version", "sys_change_operation", "changetrackingversion", "arcane_merge_key")

  override val backfillBehavior: BackfillBehavior = BackfillBehavior.Overwrite

  override val backfillStartDate: Option[OffsetDateTime] = None
  override val maxRowsPerFile: Option[Int]               = Some(spec.stagingDataSettings.maxRowsPerFile)

  override val bufferingEnabled: Boolean = IsBackfilling || spec.sourceSettings.buffering.isDefined

  override val bufferingStrategy: BufferingStrategy = spec.sourceSettings.buffering match
    case None if IsBackfilling => BufferingStrategy.Unbounded
    case None                  => BufferingStrategy.Buffering(0)
    case Some(buffering) =>
      buffering.strategy.toLowerCase match
        case "bounded"   => BufferingStrategy.Buffering(buffering.maxBufferSize)
        case "unbounded" => BufferingStrategy.Unbounded
        case _           => throw new IllegalArgumentException(s"Unknown buffering strategy: ${buffering.strategy}")

  /** SQL Server stream always emits the same schema. Schema change normally causes CDC to break. There are, however,
    * cases when this doesn't seem to happen - to be investigated.
    */
  val isUnifiedSchema: Boolean = true

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")
  val metricsPublisherInterval: Duration = Duration.ofMillis(
    sys.env.getOrElse("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS", "100").toInt
  )

  override val icebergSinkSettings: IcebergSinkSettings = new IcebergSinkSettings {
    override val namespace: String                         = spec.sinkSettings.sinkCatalogSettings.namespace
    override val warehouse: String                         = spec.sinkSettings.sinkCatalogSettings.warehouse
    override val catalogUri: String                        = spec.sinkSettings.sinkCatalogSettings.catalogUri
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
  }

given Conversion[SqlServerChangeTrackingStreamContext, ConnectionOptions] with
  def apply(context: SqlServerChangeTrackingStreamContext): ConnectionOptions =
    ConnectionOptions(
      context.sourceConnectionString,
      context.spec.sourceSettings.schema,
      context.spec.sourceSettings.table,
      Some(context.spec.sourceSettings.fetchSize)
    )

given Conversion[SqlServerChangeTrackingStreamContext, DatagramSocketConfig] with
  def apply(context: SqlServerChangeTrackingStreamContext): DatagramSocketConfig =
    DatagramSocketConfig(context.datadogSocketPath)

given Conversion[SqlServerChangeTrackingStreamContext, MetricsConfig] with
  def apply(context: SqlServerChangeTrackingStreamContext): MetricsConfig =
    MetricsConfig(context.metricsPublisherInterval)

object SqlServerChangeTrackingStreamContext:
  type Environment = StreamContext & ConnectionOptions & GroupingSettings & IcebergStagingSettings &
    JdbcMergeServiceClientSettings & VersionedDataGraphBuilderSettings & SinkSettings & StagingDataSettings &
    TablePropertiesSettings & BackfillSettings & FieldSelectionRuleSettings & SourceBufferingSettings &
    DatagramSocketConfig & DatadogPublisherConfig & MetricsConfig

  /** The ZLayer that creates the VersionedDataGraphBuilder.
    */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
    .fromEnvironment("STREAMCONTEXT__SPEC")
    .map(combineSettingsLayer)
    .getOrElse(ZLayer.fail(new Exception("The stream context is not specified.")))

  private def combineSettingsLayer(spec: StreamSpec): ZLayer[Any, Throwable, Environment] =
    val context = SqlServerChangeTrackingStreamContext(spec)

    ZLayer.succeed(context)
      ++ ZLayer.succeed[ConnectionOptions](context)
      ++ ZLayer.succeed[DatagramSocketConfig](context)
      ++ ZLayer.succeed[MetricsConfig](context)
      ++ ZLayer.succeed(DatadogPublisherConfig())
