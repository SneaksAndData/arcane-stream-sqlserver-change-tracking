package com.sneaksanddata.arcane.framework
package services.metrics.datadog.base

import com.timgroup.statsd.StatsDClient


/**
 * A trait that represents a DataDog configuration.
 */
trait DataDogConfiguration:
  
  /**
   * Builds a DataDog client.
   *
   * @return The DataDog client.
   */
  def build: StatsDClient

