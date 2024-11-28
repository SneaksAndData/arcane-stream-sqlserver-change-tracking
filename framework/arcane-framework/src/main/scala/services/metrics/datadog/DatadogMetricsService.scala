package com.sneaksanddata.arcane.framework
package services.metrics.datadog

import services.metrics.base.MetricsService
import services.metrics.datadog.base.DataDogConfiguration

import zio.{ZIO, ZLayer}

import scala.collection.immutable.SortedMap


/**
 * A metrics service that sends metrics to DataDog.
 *
 * @param dataDogConfiguration The DataDog configuration.
 */
class DatadogMetricsService(dataDogConfiguration: DataDogConfiguration) extends MetricsService with AutoCloseable:
  
  private val dataDog = dataDogConfiguration.build

  /**
   * Increments the counter.
   *
   * @param name The name of the counter.
   * @param value The value to increment the counter by.
   */
  def incrementCounter(name: String, dimensions: SortedMap[String, String], value: Long): Unit =
    dataDog.increment(name, value, dimensions.toTags*)


  /**
   * Closes the metrics service.
   */
  override def close(): Unit = dataDog.close()

  extension (a: SortedMap[String, String]) private def toTags: List[String] = a.map { case (k, v) => s"$k:$v" }.toList

/**
 * The companion object for the DatadogMetricsService class.
 */
object DatadogMetricsService:

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[DataDogConfiguration, Nothing, MetricsService] =
    ZLayer {
      for
        configuration <- ZIO.service[DataDogConfiguration]
      yield DatadogMetricsService(configuration)
    }

  /**
   * Creates a new instance of the DatadogMetricsService.
   *
   * @param dataDogConfiguration The DataDog client.
   * @return The DatadogMetricsService instance.
   */
  def apply(dataDogConfiguration: DataDogConfiguration): DatadogMetricsService = new DatadogMetricsService(dataDogConfiguration)