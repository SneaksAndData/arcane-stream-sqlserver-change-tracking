package com.sneaksanddata.arcane.framework
package services.base

import scala.concurrent.Future

/**
 * Represents a provider of a schema for a data produced by Arcane.
 *
 * @tparam Schema The type of the schema.
 */
trait SchemaProvider[Schema] {

  /**
   * Gets the schema for the data produced by Arcane.
   *
   * @return A future containing the schema for the data produced by Arcane.
   */
  def getSchema: Future[Schema]
}
