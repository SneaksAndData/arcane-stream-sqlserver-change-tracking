package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import models.ArcaneType.{IntType, LongType, StringType}
import models.Field
import services.mssql.{ConnectionOptions, MsSqlConnection, QueryProvider}

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*

import java.sql.Connection
import java.util.Properties
import scala.List
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
    TestConnectionInfo(
      ConnectionOptions(
        connectionUrl,
        "arcane",
        "dbo",
        "MsSqlConnectorsTests",
        Some("format(getdate(), 'yyyyMM')")), con)

  def createTable(con: Connection): Unit =
    val query = "use arcane; drop table if exists dbo.MsSqlConnectorsTests; create table dbo.MsSqlConnectorsTests(x int not null, y int)"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = "use arcane; alter table dbo.MsSqlConnectorsTests add constraint pk_MsSqlConnectorsTests primary key(x);"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = "use arcane; alter table dbo.MsSqlConnectorsTests enable change_tracking;"
    statement.executeUpdate(enableCtCmd)

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
    test(conn)

  "QueryProvider" should "generate columns query" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    val query = QueryProvider.getColumnSummariesQuery(connector.connectionOptions.schemaName,
      connector.connectionOptions.tableName,
      connector.connectionOptions.databaseName)
    query should include ("case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey")
  }

  "QueryProvider" should "generate schema query" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    QueryProvider.getSchemaQuery(connector) map { query =>
      query should (
        include ("ct.SYS_CHANGE_VERSION") and include ("ARCANE_MERGE_KEY") and include("format(getdate(), 'yyyyMM')")
        )
    }
  }

  "MsSqlConnection" should "be able to extract schema column names from the database" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    connection.getSchema map { schema =>
      val fields = for column <- schema if column.isInstanceOf[Field] yield column.asInstanceOf[Field].name
      fields should be (List("x", "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "y", "ChangeTrackingVersion", "ARCANE_MERGE_KEY", "DATE_PARTITION_KEY"))
    }
  }


  "MsSqlConnection" should "be able to extract schema column types from the database" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    connection.getSchema map { schema =>
      val fields = for column <- schema if column.isInstanceOf[Field] yield column.asInstanceOf[Field].fieldType
      fields should be(List(IntType, LongType, StringType, IntType, LongType, StringType, StringType))
    }
  }
