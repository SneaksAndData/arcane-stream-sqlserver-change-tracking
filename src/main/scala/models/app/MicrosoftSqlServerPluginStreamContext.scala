package com.sneaksanddata.arcane.sql_server_change_tracking
package models.app

import com.sneaksanddata.arcane.framework.models.app.{DefaultPluginStreamContext, PluginStreamContext}
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.DefaultSinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.DefaultStagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultStreamModeSettings,
  DefaultThroughputSettings
}
import upickle.ReadWriter
import upickle.implicits.key
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** The specification for the stream.
  */
case class MicrosoftSqlServerPluginStreamContext(
    @key("observability") private val observabilityIn: DefaultObservabilitySettings,
    @key("staging") private val stagingIn: DefaultStagingSettings,
    @key("streamMode") private val streamModeIn: DefaultStreamModeSettings,
    @key("sink") private val sinkIn: DefaultSinkSettings,
    @key("throughput") private val throughputIn: DefaultThroughputSettings,
    override val source: MicrosoftSqlServerSourceSettings
) extends DefaultPluginStreamContext(observabilityIn, stagingIn, streamModeIn, sinkIn, throughputIn) derives ReadWriter:
  // TODO: should be implemented when Operator supports overrides
  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = this

object MicrosoftSqlServerPluginStreamContext:
  def apply(value: String): MicrosoftSqlServerPluginStreamContext =
    PluginStreamContext[MicrosoftSqlServerPluginStreamContext](value)

  lazy val layer
      : ZLayer[Any, Throwable, PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig] =
    PluginStreamContext.getLayer[MicrosoftSqlServerPluginStreamContext]

//
//  val sourceConnectionString: String = sys.env("ARCANE__CONNECTIONSTRING")
//
//  override val isServerSide: Boolean = true
//
//  override val essentialFields: Set[String] =
//    Set("sys_change_version", "sys_change_operation", "changetrackingversion", "arcane_merge_key")
//
//  override val backfillBehavior: BackfillBehavior = BackfillBehavior.Overwrite
//
//  /** SQL Server stream always emits the same schema. Schema change normally causes CDC to break. There are, however,
//    * cases when this doesn't seem to happen - to be investigated.
//    */
//  val isUnifiedSchema: Boolean = true
//
