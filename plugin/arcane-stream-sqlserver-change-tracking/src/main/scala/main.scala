package com.sneaksanddata.arcane.sql_server_change_tracking

import zio.{Console, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.io.IOException

object main extends ZIOAppDefault {

  @main
  def run: ZIO[ZIOAppArgs & Scope, IOException, Unit] = for {
    _ <- Console.printLine("Hello! What is your name?")
    n <- Console.readLine
    _ <- Console.printLine("Hello, " + n + ", good to meet you!")
  } yield ()
}
