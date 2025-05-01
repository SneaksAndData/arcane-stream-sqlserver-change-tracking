package com.sneaksanddata.arcane.sql_server_change_tracking
package metrics

import jnr.unixsocket.{UnixDatagramChannel, UnixSocketAddress, UnixSocketChannel}
import zio.*
import zio.metrics.connectors.statsd.{StatsdClient, StatsdConfig}

import java.net.{InetSocketAddress, ProtocolFamily, SocketAddress, StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{DatagramChannel, SocketChannel, WritableByteChannel}
import scala.util.{Failure, Success, Try}
import zio.metrics.connectors.statsd.StatsdUdsConfig

import java.nio.channels.spi.SelectorProvider

class StatsdUdsClient(channel: UnixDatagramChannel, address: SocketAddress) extends StatsdClient:

  override def send(chunk: Chunk[Byte]): Long = write(chunk.toArray)

  private def write(ab: Array[Byte]): Long =
    Try(channel.send(ByteBuffer.wrap(ab), address).toLong) match {
      case Success(value) =>
//        System.out.println(value)
        value
      case Failure(t) =>
        t.printStackTrace()
        0L
    }

object StatsdUdsClient:

  def apply(channel: UnixDatagramChannel, address: SocketAddress): StatsdUdsClient = new StatsdUdsClient(channel, address)

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

  private def channelUDS(path: String): ZIO[Any, Throwable, (UnixDatagramChannel, SocketAddress)] =
    ZIO.attempt {
      val ch = UnixDatagramChannel.open()
      ch.configureBlocking(false)
//      ch.connect(UnixDomainSocketAddress.of(path))
//      ch.connect(new UnixSocketAddress(path))

      val address: SocketAddress = UnixSocketAddress(path)
      (ch, address)
    }
