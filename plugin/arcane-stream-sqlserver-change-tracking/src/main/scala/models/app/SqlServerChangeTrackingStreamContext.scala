package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.lakehouse.{IcebergCatalogCredential, S3CatalogFileIO}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.mssql.ConnectionOptions
import zio.ZLayer
import zio.json.*

import java.time.Duration


/**
 * The context for the SQL Server Change Tracking stream.
 * @param spec The stream specification
 */
case class SqlServerChangeTrackingStreamContext(spec: StreamSpec) extends StreamContext
  with GroupingSettings
  with IcebergCatalogSettings
  with VersionedDataGraphBuilderSettings:

  override val rowsPerGroup: Int = spec.rowsPerGroup
  override val lookBackInterval: Duration = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String = spec.catalogSettings.namespace
  override val warehouse: String = spec.catalogSettings.warehouse
  override val catalogUri: String = spec.catalogSettings.catalogUri

  override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  override val stagingLocation: Option[String] = spec.stagingLocation

  @jsonExclude
  val connectionString: String = sys.env("ARCANE_CONNECTIONSTRING")

  val database: String = spec.database

  override def toString: String = this.toJsonPretty


given Conversion[SqlServerChangeTrackingStreamContext, ConnectionOptions] with
  def apply(context: SqlServerChangeTrackingStreamContext): ConnectionOptions =
    ConnectionOptions(context.connectionString,
      context.database,
      context.spec.schema,
      context.spec.table,
      context.spec.partitionExpression)

object SqlServerChangeTrackingStreamContext {
  implicit val icebergSettingsDecoder: JsonDecoder[CatalogSettings] = DeriveJsonDecoder.gen[CatalogSettings]
  implicit val streamSpecDecoder: JsonDecoder[StreamSpec] = DeriveJsonDecoder.gen[StreamSpec]

  implicit val icebergSettingsEncoder: JsonEncoder[CatalogSettings] = DeriveJsonEncoder.gen[CatalogSettings]
  implicit val specEncoder: JsonEncoder[StreamSpec] = DeriveJsonEncoder.gen[StreamSpec]
  implicit val contextEncoder: JsonEncoder[SqlServerChangeTrackingStreamContext] = DeriveJsonEncoder.gen[SqlServerChangeTrackingStreamContext]

  type Environment = StreamContext
    & ConnectionOptions
    & GroupingSettings
    & VersionedDataGraphBuilderSettings
    & IcebergCatalogSettings

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[Any, Throwable, Environment] =
    sys.env.get("STREAMCONTEXT__SPEC") map { raw =>
      val spec = raw.fromJson[StreamSpec] match {
        case Left(error) => throw new Exception(s"Failed to decode the stream context: $error")
        case Right(value) => value
      }
      val context = SqlServerChangeTrackingStreamContext(spec)
      ZLayer.succeed(context) ++ ZLayer.succeed[ConnectionOptions](context)
    } getOrElse {
      ZLayer.fail(new Exception("The stream context is not specified."))
    }
}
