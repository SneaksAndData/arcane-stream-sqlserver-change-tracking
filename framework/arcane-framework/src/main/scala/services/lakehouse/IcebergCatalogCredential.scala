package com.sneaksanddata.arcane.framework
package services.lakehouse

import org.apache.iceberg.rest.auth.OAuth2Properties

private trait IcebergCatalogCredential:
  val credential: String
  val oauth2Uri: String

  final val oAuth2Properties: Map[String, String] = Map(
    OAuth2Properties.CREDENTIAL -> credential,
    OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri
  )

object IcebergCatalogCredential extends IcebergCatalogCredential:
  override val credential: String = scala.util.Properties.envOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID", "") + ":" + scala.util.Properties.envOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET", "")
  override val oauth2Uri: String = scala.util.Properties.envOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI", "")


