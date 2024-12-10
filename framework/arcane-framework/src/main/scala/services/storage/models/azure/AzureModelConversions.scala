package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import com.azure.storage.blob.models.BlobItem
import services.storage.models.base.StoredBlob

import scala.jdk.CollectionConverters.*

object AzureModelConversions:
  given Conversion[BlobItem, StoredBlob] with
    override def apply(blobItem: BlobItem): StoredBlob = StoredBlob(
      name = blobItem.getName,
      createdOn = blobItem.getProperties.getCreationTime.toEpochSecond,
      metadata = blobItem.getMetadata.asScala.toMap,
      contentHash = Option(blobItem.getProperties.getContentMd5.map(_.toChar).mkString),
      contentEncoding = Option(blobItem.getProperties.getContentEncoding),
      contentType = Option(blobItem.getProperties.getContentType),
      contentLength = Option(blobItem.getProperties.getContentLength),
      lastModified = Option(blobItem.getProperties.getLastModified.toEpochSecond)
    )
