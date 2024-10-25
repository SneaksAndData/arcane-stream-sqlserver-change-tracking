package com.sneaksanddata.arcane.framework
package services.storage.exceptions

/**
 * An exception that is thrown when an error occurs in the blob storage.
 *
 * @param message The message of the exception.
 * @param cause The cause of the exception.
 */
final case class BlobStorageException(private val message: String, private val cause: Throwable) extends Exception(message, cause);
