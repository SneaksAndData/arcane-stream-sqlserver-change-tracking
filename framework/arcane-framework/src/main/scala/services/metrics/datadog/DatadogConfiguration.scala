package com.sneaksanddata.arcane.framework
package services.metrics.datadog

import services.metrics.datadog.base.DataDogConfiguration

import com.timgroup.statsd.{NoOpStatsDClient, NonBlockingStatsDClientBuilder, StatsDClient}

/**
 * A trait that represents a DataDog configuration that does nothing.
 */
case class NoOpConfiguration() extends DataDogConfiguration:
  def build: StatsDClient = new NoOpStatsDClient()

/**
 * A trait that represents a DataDog configuration that uses a unix domain socket.
 */
case class UnixDomainSocketConfiguration(statsdServerName: String, environment: String, serviceVersion: String, prefix: String) extends DataDogConfiguration:
  def build: StatsDClient = new NonBlockingStatsDClientBuilder()
    .hostname("/var/run/datadog/dsd.socket")
    .port(0) // Necessary for unix socket
    .prefix(prefix)
    .build();

/**
 * The companion object for the DataDogConfiguration class.
 */
object DataDogConfiguration:

  /**
   * Selects the DataDog configuration based on the environment.
   *
   * @param metricNamespace The metric namespace.
   * @return The DataDog configuration.
   */
  def selectFromEnvironment(metricNamespace: String): DataDogConfiguration = sys.env.get("ARCANE_FRAMEWORK_DATADOG_ENABLED") match
    case Some(_) => unixDomainSocket(metricNamespace)
    case None => noOp(metricNamespace)

  private def noOp(metricNamespace: String): DataDogConfiguration =
    NoOpConfiguration()

  private def unixDomainSocket(metricNamespace: String): DataDogConfiguration =
    val path = sys.env.getOrElse("ARCANE_FRAMEWORK_DD_UNIX_DOMAIN_SOCKET_PATH", "unix:///var/run/datadog/dsd.socket")
    val deployEnv = sys.env.getOrElse("ARCANE_FRAMEWORK_DD_DEPLOY_ENV", "development")
    val version = sys.env.getOrElse("APPLICATION_VERSION", "0.0.0")
    UnixDomainSocketConfiguration(path, deployEnv, version, metricNamespace.toLowerCase)
