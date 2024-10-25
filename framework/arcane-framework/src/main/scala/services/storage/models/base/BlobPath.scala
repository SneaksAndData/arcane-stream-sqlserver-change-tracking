package com.sneaksanddata.arcane.framework
package services.storage.models.base

trait BlobPath {
  /**
   * Converts the path to a HDFS-style path.
   *
   * @return The path as a string.
   */
  def toHdfsPath: String
}
