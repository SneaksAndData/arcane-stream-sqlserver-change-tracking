package com.sneaksanddata.arcane.framework
package services.storage.models

import java.time.OffsetDateTime

case class UploadResult(name: String, lastModified: OffsetDateTime, contentHash: String)
