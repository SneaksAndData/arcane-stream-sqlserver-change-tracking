package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.scalatest.*
import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*
import services.connectors.mssql.*

import java.sql.Connection
import java.util.Properties
import scala.concurrent.Future
import scala.language.postfixOps

case class TestConnectionInfo(connectionOptions: ConnectionOptions, connection: Connection)

class MsSqlConnectorsTests extends flatspec.AsyncFlatSpec with Matchers:
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  val connectionUrl = "jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC"

  def createDb(): TestConnectionInfo =
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val query = "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'arcane') BEGIN CREATE DATABASE arcane; alter database Arcane set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON); END;"
    val statement = con.createStatement()
    statement.execute(query)
    createTable(con)
    TestConnectionInfo(ConnectionOptions(connectionUrl, "arcane", "arcane"), con)

  def createTable(con: Connection): Unit =
    val query = "use arcane; drop table if exists dbo.MsSqlConnectorsTests; create table dbo.MsSqlConnectorsTests(x int not null, y int)"
    val statement = con.createStatement()
    statement.execute(query)

    val createPKCmd = "use arcane; alter table dbo.MsSqlConnectorsTests add constraint pk_MsSqlConnectorsTests primary key(x);";
    statement.execute(createPKCmd)

    val enableCtCmd = "use arcane; alter table dbo.MsSqlConnectorsTests enable change_tracking;";
    statement.execute(enableCtCmd)

    for i <- 1 to 10 do
      val insertCmd = s"use arcane; insert into dbo.MsSqlConnectorsTests values($i, ${i+1})"
      statement.execute(insertCmd)


  def removeDb(): Unit =
    val query = "DROP DATABASE arcane"
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val statement = con.createStatement()
    statement.execute(query)


  def withDatabase(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    val conn = createDb()
    test(conn) //andThen { case _ => removeDb() }

  "MsSqlConnector" should "read schema" in withDatabase { dbInfo =>
    val connector = MsSqlConnector(dbInfo.connectionOptions)
    connector.getSchema map { schema =>
      val cols = schema.getColumns()
      cols.size() should be (7)
    }
  }
