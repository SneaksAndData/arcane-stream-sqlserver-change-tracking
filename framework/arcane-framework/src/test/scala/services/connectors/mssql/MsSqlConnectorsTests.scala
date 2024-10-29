package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import services.connectors.mssql.DbServer.{createDb, removeDb}

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.scalatest.*
import org.scalatest.flatspec.FixtureAnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*

import java.sql.Connection
import java.util.Properties
import scala.language.postfixOps

object DbServer:
  private val connectionUrl = "jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC"

  def createDb(): Connection = {
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val query = "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'arcane') BEGIN CREATE DATABASE arcane; alter database Arcane set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON); END;"
    val statement = con.createStatement()
    statement.execute(query)
    con
  }

  def removeDb(): Unit =
    val query = "DROP DATABASE arcane"
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val statement = con.createStatement()
    statement.execute(query)

trait DbFixture:

  this: FixtureTestSuite =>

  type FixtureParam = Connection

  // Allow clients to populate the database after
  // it is created
  def populateDb(db: Connection): Unit = {}

  def withFixture(test: OneArgTest): Outcome =
    val conn = createDb()
    try {
      populateDb(conn)
      withFixture(test.toNoArgTest(conn)) // "loan" the fixture to the test
    }
    finally removeDb() // clean up the fixture



class MsSqlConnectorsTests extends flatspec.FixtureAnyFlatSpec with Matchers with DbFixture:

  override def populateDb(db: Connection): Unit =
    println("ScalaTest is ")

  "Testing" should "be easy" in { db =>
    println("easy!")
    assert(true)
  }
