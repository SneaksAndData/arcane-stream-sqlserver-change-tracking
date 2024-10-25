package com.sneaksanddata.arcane.framework
package services.storage.base

import services.storage.exceptions.BlobStorageException
import services.storage.models.UploadResult
import services.storage.models.base.BlobPath

import zio.ZIO

import java.net.URL

/**
 * A trait that defines the interface for writing to a blob storage.
 *
 * @tparam PathType The type of the path to the blob.
 */
trait BlobStorageWriter[PathType <: BlobPath] {
  /**
   * Saves the given bytes as a blob.
   *
   * @param blobPath The path to the blob.
   * @param data The bytes to save.
   * @return The result of the upload.
   */
  def saveBytesAsBlob(blobPath: PathType, data: Array[Byte]): ZIO[Any, BlobStorageException, UploadResult]

  /**
   * Saves the given text as a blob.
   *
   * @param blobPath The path to the blob.
   * @param data The text to save.
   * @return The result of the upload.
   */
  def saveTextAsBlob(blobPath: PathType, data: String): ZIO[Any, BlobStorageException, UploadResult]

  /**
   * Removes the blob at the given path.
   *
   * @param blobPath The path to the blob.
   * @param data The data to remove.
   */
  def removeBlob(blobPath: PathType, data: String): ZIO[Any, BlobStorageException, Unit]

  /**
   * Gets the URI of the blob at the given path.
   *
   * @param blobPath The path to the blob.
   * @param data The data to get.
   * @return The URI of the blob.
   */
  def getBlobUri(blobPath: PathType, data: String): ZIO[Any, BlobStorageException, URL]
}
