package com.sneaksanddata.arcane.framework
package services.lakehouse

import exceptions.ConfigurationException

import zio.{IO, System, ZIO, ZLayer}

case class IcebergSettings(namespace: String,
                            warehouse: String,
                            catalogUri: String,
                            additionalProperties: Map[String, String],
                            s3CatalogFileIO: S3CatalogFileIO,
                            locationOverride: Option[String])



object IcebergSettings:
  
  private def requiredEnvironmentVariable(name: String): IO[ConfigurationException, String] =
    for 
      opt <- System.env(name).mapError(ConfigurationException(s"Environment variable $name is not set", _))
      value = opt.getOrElse(throw new ConfigurationException(s"Environment variable $name is not set"))
    yield value
    
  private def optionalEnvironmentVariable(name: String): IO[ConfigurationException, Option[String]] =
    System.env(name).mapError(ConfigurationException(s"Environment variable $name is not set", _))

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[S3CatalogFileIO, ConfigurationException, IcebergSettings] =
    ZLayer {
      for
        namespace <- requiredEnvironmentVariable("ARCANE_FRAMEWORK_ICEBERG_NAMESPACE")
        warehouse <- requiredEnvironmentVariable("ARCANE_FRAMEWORK_ICEBERG_WAREHOUSE")
        catalogUri <- requiredEnvironmentVariable("ARCANE_FRAMEWORK_ICEBERG_CATALOG_URI")
        additionalProperties = IcebergCatalogCredential.oAuth2Properties
        s3CatalogFileIO <- ZIO.service[S3CatalogFileIO]
        locationOverride <- optionalEnvironmentVariable("ARCANE_FRAMEWORK_ICEBERG_LOCATION_OVERRIDE")
      yield IcebergSettings(namespace, warehouse, catalogUri, additionalProperties, s3CatalogFileIO, locationOverride)
    }
