package com.sneaksanddata.arcane.framework
package services.base

import scala.concurrent.Future

trait SchemaProvider[Schema] {

  def getSchema: Future[Schema]
}
