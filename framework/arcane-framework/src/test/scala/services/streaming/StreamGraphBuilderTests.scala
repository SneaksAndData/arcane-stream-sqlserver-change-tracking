package com.sneaksanddata.arcane.framework
package services.streaming

import models.ArcaneType.{IntType, StringType}
import models.DataCell
import services.mssql.query.{LazyQueryResult, QueryRunner, ScalarQueryResult}
import services.mssql.{ConnectionOptions, MsSqlConnection}
import services.streaming.base.StreamLifetimeService

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*
import zio.{Runtime, Unsafe}

import java.sql.Connection
import java.util.Properties
import scala.List
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Using

case class TestConnectionInfo(connectionOptions: ConnectionOptions, connection: Connection)

class StreamGraphBuilderTests extends flatspec.AsyncFlatSpec with Matchers:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private implicit val dataQueryRunner: QueryRunner[LazyQueryResult.OutputType, LazyQueryResult] = QueryRunner()
  private implicit val versionQueryRunner: QueryRunner[Option[Long], ScalarQueryResult[Long]] = QueryRunner()
  private val connectionUrl = "jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC;databaseName=arcane"
  private val runtime = Runtime.default

  def createDb(tableName: String): TestConnectionInfo =
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val query = "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'arcane') BEGIN CREATE DATABASE arcane; alter database Arcane set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON); END;"
    val statement = con.createStatement()
    statement.execute(query)
    createTable(con, tableName)
    TestConnectionInfo(
      ConnectionOptions(
        connectionUrl,
        "arcane",
        "dbo",
        "StreamGraphBuilderTests",
        Some("format(getdate(), 'yyyyMM')")), con)

  def insertData(con: Connection, tableName: String): Unit =
    val sql = s"use arcane; insert into dbo.$tableName values(?, ?)"
    Using(con.prepareStatement(sql)) { insertStatement =>
      for i <- 0 to 9 do
        insertStatement.setInt(1, i)
        insertStatement.setInt(2, i + 1)
        insertStatement.addBatch()
        insertStatement.clearParameters()
      insertStatement.executeBatch()
    }

  def createTable(con: Connection, tableName: String): Unit =
    val query = s"use arcane; drop table if exists dbo.$tableName; create table dbo.StreamGraphBuilderTests(x int not null, y int)"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = s"use arcane; alter table dbo.$tableName add constraint pk_StreamGraphBuilderTests primary key(x);"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = s"use arcane; alter table dbo.$tableName enable change_tracking;"
    statement.executeUpdate(enableCtCmd)
    statement.close()


  def removeDb(): Unit =
    val query = "DROP table if exists arcane.dbo.StreamGraphBuilderTests"
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val statement = con.createStatement()
    statement.execute(query)


  def withFreshTable(tableName: String)(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    removeDb()
    val conn = createDb(tableName)
    insertData(conn.connection, tableName)
    test(conn)

  def withEmptyTable(tableName: String)(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    removeDb()
    val conn = createDb(tableName)
    test(conn)

  "StreamGraph" should "not duplicate data on the first iteration" in withFreshTable("StreamGraphBuilderTests") { dbInfo =>
    val streamGraphBuilder = new StreamGraphBuilder(MsSqlConnection(dbInfo.connectionOptions))

    val lifetime = TestStreamLifetimeService(3)

    val stream = streamGraphBuilder.build(lifetime).map(_.toList)
    val zio = stream.runCollect

    Unsafe.unsafe { implicit unsafe => runtime.unsafe.runToFuture(zio) } flatMap { list =>
      list should have size 3 // 3 batches of changes
      list map (_.size) should contain theSameElementsAs List(10, 0, 0) // only first batch has data
      list.head.size should be (10) // 7 fields in the first batch
    }
  }

  "StreamGraph" should "be able to generate changes stream" in withFreshTable("StreamGraphBuilderTests") { dbInfo =>
    val streamGraphBuilder = new StreamGraphBuilder(MsSqlConnection(dbInfo.connectionOptions))

    val lifetime = TestStreamLifetimeService(3, counter => {
      // Skip first iteration since lifetime service is called before the first iteration
      if counter > 0 then
        val updateStatement = dbInfo.connection.createStatement()
        for i <- 0 to 9 do
          val insertCmd = s"use arcane; insert into dbo.StreamGraphBuilderTests values(${counter * 10 + i}, ${counter * 10 + i + 1})"
          updateStatement.execute(insertCmd)
    })

    val stream = streamGraphBuilder.build(lifetime).map(_.toList)
    val zio = stream.runCollect

    Unsafe.unsafe { implicit unsafe => runtime.unsafe.runToFuture(zio) } flatMap { list  =>
      list must have size 3 // 3 batches of changes
      list map(_.size) must contain only 10 // rows changes in each batch
      list.flatMap(_.map(_.size)) must contain only 7 // 7 fields in each row
    }
  }

  // The unit test validates that the streaming graph for MS SQL Server can read deletions.
  // The test starts an asynchronous stream that reads changes from the database.
  // While the stream is running, the test inserts two rows into the database.
  // When the deletion statement is executed, the insertion event (x = 8888, SYS_CHANGE_OPERATION = "I")
  // is deleted from the database
  // We are expecting that the stream will return only the one deletion event (x = 8888, SYS_CHANGE_OPERATION = "D").
  // and only one insertion event (x = 9999, SYS_CHANGE_OPERATION = "I").
  // No insertion event (x = 8888, SYS_CHANGE_OPERATION = "I") should be returned.
  // This test was ported from .NET to Scala without significant changes.
  "StreamGraph" should "get deletes" in withEmptyTable("StreamGraphBuilderTests") { dbInfo =>

    val streamGraphBuilder = new StreamGraphBuilder(MsSqlConnection(dbInfo.connectionOptions))
    val stream = streamGraphBuilder.build(TestStreamLifetimeService.withMaxQueries(5)).map(_.toList).runCollect()
    val fut = Unsafe.unsafe { implicit unsafe => runtime.unsafe.runToFuture(stream) }

    val updateStatement = dbInfo.connection.createStatement()
    updateStatement.execute(s"use arcane; insert into dbo.StreamGraphBuilderTests values(8888, 9999)")
    updateStatement.execute(s"use arcane; insert into dbo.StreamGraphBuilderTests values(9999, 9999)")

    updateStatement.execute("use arcane; delete from dbo.StreamGraphBuilderTests where x=8888")

    fut map { list =>
      list.head.size should be (2)
      list.head.last should contain allOf(DataCell("x", IntType, 9999), DataCell("SYS_CHANGE_OPERATION", StringType, "I"))
      list.head.head should contain allOf(DataCell("x", IntType, 8888), DataCell("SYS_CHANGE_OPERATION", StringType, "D"))
    }
  }

class TestStreamLifetimeService(maxQueries: Int, callback: Int => Any) extends StreamLifetimeService:
  var counter = 0
  override def cancelled: Boolean =
    callback(counter)
    counter += 1
    counter > maxQueries

object TestStreamLifetimeService:

  def withMaxQueries(maxQueries: Int) = new TestStreamLifetimeService(maxQueries, _ => ())

  def apply(maxQueries: Int) = new TestStreamLifetimeService(maxQueries, _ => ())

  def apply(maxQueries: Int, callback: Int => Any) = new TestStreamLifetimeService(maxQueries, callback)
