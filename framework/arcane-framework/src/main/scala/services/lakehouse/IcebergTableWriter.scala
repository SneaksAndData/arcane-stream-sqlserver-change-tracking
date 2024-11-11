package com.sneaksanddata.arcane.framework
package services.lakehouse

import org.apache.iceberg.Table
import org.apache.iceberg.rest.RESTCatalog

import scala.concurrent.Future

class IcebergTableWriter extends TableWriter[Table, RESTCatalog]:
  override def write: Future[Table] = ???
  