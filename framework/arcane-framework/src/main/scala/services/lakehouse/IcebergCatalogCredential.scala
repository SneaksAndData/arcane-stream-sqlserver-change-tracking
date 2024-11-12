package com.sneaksanddata.arcane.framework
package services.lakehouse

import org.apache.iceberg.rest.auth.OAuth2Properties

private trait IcebergCatalogCredential:
  val credential: String
  val oauth2Uri: String
  val oauth2Scope: String

object IcebergCatalogCredential extends IcebergCatalogCredential:
  override val credential: String = sys.env.getOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_ID", "") + ":" + sys.env.getOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_SECRET", "")
  override val oauth2Uri: String = sys.env.getOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_CLIENT_URI", "")
  override val oauth2Scope: String = sys.env.getOrElse("ARCANE.FRAMEWORK__S3_CATALOG_AUTH_SCOPE", "")

  final val oAuth2Properties: Map[String, String] = Map(
    OAuth2Properties.CREDENTIAL -> credential,
    OAuth2Properties.OAUTH2_SERVER_URI -> oauth2Uri,
    OAuth2Properties.SCOPE -> oauth2Scope
  )
