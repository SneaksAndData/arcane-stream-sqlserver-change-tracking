package com.sneaksanddata.arcane.sql_server_change_tracking
package metrics

import jnr.unixsocket.{UnixSocketAddress, UnixSocketChannel}
import zio.*
import zio.metrics.connectors.statsd.{StatsdClient, StatsdConfig}

import java.net.{InetSocketAddress, SocketAddress, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{DatagramChannel, SocketChannel, WritableByteChannel}
import scala.util.{Failure, Success, Try}
import zio.metrics.connectors.statsd.StatsdUdsConfig

class StatsdUdsClient(channel: DatagramChannel, address: SocketAddress) extends StatsdClient:

  override def send(chunk: Chunk[Byte]): Long = write(chunk.toArray)

  private def write(ab: Array[Byte]): Long =
    Try(channel.send(ByteBuffer.wrap(ab), address).toLong) match {
      case Success(value) => value
      case Failure(_) => 0L
    }

object StatsdUdsClient:

  def apply(channel: DatagramChannel, address: SocketAddress): StatsdUdsClient = new StatsdUdsClient(channel, address)

  type Environmet = StatsdConfig

  val layer: ZLayer[Environmet, Throwable, StatsdClient] = ZLayer.scoped {
    for
      config  <- ZIO.service[StatsdConfig]
      (channel, address) <- config match {
        case StatsdUdsConfig(path)      => channelUDS(path)
        case _ => throw new IllegalArgumentException("IP channel not supported")
      }
    yield new StatsdUdsClient(channel, address)
  }

  private def channelUDS(path: String): ZIO[Any, Throwable, (DatagramChannel, SocketAddress)] =
    ZIO.attempt {
      val ch = DatagramChannel.open()
      ch.configureBlocking(false)
      val address: SocketAddress = new UnixSocketAddress(path)
      (ch, address)
    }
