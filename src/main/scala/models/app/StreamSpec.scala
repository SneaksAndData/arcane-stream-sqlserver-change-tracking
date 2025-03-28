package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import upickle.default.*

/**
  * The configuration of Iceberg sink.
 */
case class CatalogSettings(namespace: String,
                           warehouse: String,
                           catalogUri: String,
                           catalogName: String,
                           schemaName: String)derives ReadWriter

case class StagingDataSettingsSpec(tableNamePrefix: String, catalog: CatalogSettings, dataLocation: Option[String]) derives ReadWriter

/**
 * The configuration of Iceberg sink.
 */
case class OptimizeSettingsSpec(batchThreshold: Int,
                                fileSizeThreshold: String) derives ReadWriter
/**
 * The configuration of Iceberg sink.
 */
case class SnapshotExpirationSettingsSpec(batchThreshold: Int,
                                          retentionThreshold: String) derives ReadWriter
/**
 * The configuration of Iceberg sink.
 */
case class OrphanFilesExpirationSettings(batchThreshold: Int,
                                         retentionThreshold: String) derives ReadWriter
/**
 * The configuration of Iceberg sink.
 */
case class SinkSettings(targetTableName: String,
                        archiveTableName: String,
                        optimizeSettings: OptimizeSettingsSpec,
                        snapshotExpirationSettings: SnapshotExpirationSettingsSpec,
                        orphanFilesExpirationSettings: OrphanFilesExpirationSettings) derives ReadWriter

case class TablePropertiesSettingsSpec(partitionExpressions: Array[String], sortedBy: Array[String], parquetBloomFilterColumns: Array[String], format: String) derives ReadWriter

case class FieldSelectionRuleSpec(ruleType: String, fields: Array[String]) derives ReadWriter

case class SourceSettings(name: String,
                          baseLocation: String,
                          changeCaptureIntervalSeconds: Int,
                          changeCapturePeriodSeconds: Int) derives ReadWriter

/**
 * The specification for the stream.
 *
 * @param database The database name
 * @param schema The schema name
 * @param table The table name
 * @param rowsPerGroup The number of rows per group in the staging table
 * @param groupingIntervalSeconds The grouping interval in seconds
 * @param groupsPerFile The number of groups per file
 * @param lookBackInterval The look back interval in seconds
 * @param commandTimeout Timeout for the SQL command
 * @param changeCaptureIntervalSeconds The change capture interval in seconds
 * @param partitionExpression Partition expression for partitioning the data in the staging table (optional)
 */
case class StreamSpec(database: String,
                      schema: String,
                      table: String,
                      rowsPerGroup: Int,
                      groupsPerFile: Int,
                      lookBackInterval: Int,
                      commandTimeout: Int,

                      // Iceberg settings
                      stagingDataSettings: StagingDataSettingsSpec,
                      groupingIntervalSeconds: Int,
                      sourceSettings: SourceSettings,
                      sinkSettings: SinkSettings,
                      backfillStartDate: String,
                      backfillBehavior: String,
                      fieldSelectionRule: FieldSelectionRuleSpec,
                      tableProperties: TablePropertiesSettingsSpec) derives ReadWriter


object StreamSpec:

  def fromEnvironment(envVarName: String): Option[StreamSpec] =
    sys.env.get(envVarName).map(env => fromString(env))

  def fromString(source: String): StreamSpec =
    read[StreamSpec](source)
