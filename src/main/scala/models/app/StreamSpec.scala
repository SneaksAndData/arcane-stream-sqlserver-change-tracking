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

case class StagingDataSettingsSpec(tableNamePrefix: String,
                                   catalog: CatalogSettings,
                                   maxRowsPerFile: Int,
                                   dataLocation: Option[String]=None) derives ReadWriter

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
                        optimizeSettings: OptimizeSettingsSpec,
                        snapshotExpirationSettings: SnapshotExpirationSettingsSpec,
                        orphanFilesExpirationSettings: OrphanFilesExpirationSettings) derives ReadWriter

case class TablePropertiesSettingsSpec(partitionExpressions: Array[String], sortedBy: Array[String], parquetBloomFilterColumns: Array[String], format: String) derives ReadWriter

case class FieldSelectionRuleSpec(ruleType: String, fields: Array[String]) derives ReadWriter

case class SourceSettings(schema: String, table: String, changeCaptureIntervalSeconds: Int) derives ReadWriter

/**
 * The specification for the stream.
 *
 * @param rowsPerGroup The number of rows per group in the staging table
 * @param groupingIntervalSeconds The grouping interval in seconds
 * @param lookBackInterval The look back interval in seconds
 */
case class StreamSpec(rowsPerGroup: Int,
                      lookBackInterval: Int,
                      groupingIntervalSeconds: Int,
                      stagingDataSettings: StagingDataSettingsSpec,
                      sourceSettings: SourceSettings,
                      sinkSettings: SinkSettings,
                      fieldSelectionRule: FieldSelectionRuleSpec,
                      tableProperties: TablePropertiesSettingsSpec) derives ReadWriter


object StreamSpec:

  def fromEnvironment(envVarName: String): Option[StreamSpec] =
    sys.env.get(envVarName).map(env => fromString(env))

  def fromString(source: String): StreamSpec =
    read[StreamSpec](source)
