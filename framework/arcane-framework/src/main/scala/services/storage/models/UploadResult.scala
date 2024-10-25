package com.sneaksanddata.arcane.framework
package services.storage.models

import java.time.OffsetDateTime

/**
 * The result of uploading a blob to a storage.
 *
 * @param name The name of the blob.
 * @param lastModified The last modified time of the blob.
 * @param contentHash The hash of the content of the blob.
 */
final case class UploadResult(name: String, lastModified: OffsetDateTime, contentHash: String)
