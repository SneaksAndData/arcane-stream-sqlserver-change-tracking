package com.sneaksanddata.arcane.framework
package sinks.delta

import zio.stream.ZSink

import java.nio.file.Path

class DeltaSink {

  def fileSink(path: Path): ZSink[Any, Throwable, String, Byte, Long] =
    ZSink
      .fromPath(path)
      .contramapChunks[String](_.flatMap(_.getBytes))
}
