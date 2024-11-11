package com.sneaksanddata.arcane.framework
package services.lakehouse

trait LakehouseCatalog:
  def initialize(name: String, properties: Map[String, String]): Unit
