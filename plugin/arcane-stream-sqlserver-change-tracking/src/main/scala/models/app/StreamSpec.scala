package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

/**
  * The configuration of Iceberg sink.
 */
case class IcebergSettings(namespace: String, warehouse: String, catalogUri: String)

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
                      groupingIntervalSeconds: Int,
                      groupsPerFile: Int,
                      lookBackInterval: Int,
                      commandTimeout: Int,
                      changeCaptureIntervalSeconds: Int,

                      // Iceberg settings
                      icebergSettings: IcebergSettings,

                      stagingLocation: Option[String],
                      sinkLocation: String,
                      partitionExpression: Option[String])


