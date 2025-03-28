package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import org.scalatest.Assertion

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.concurrent.Future

object Fixtures:

  val connectionString = "jdbc:sqlserver://localhost:1433;databaseName=IntegrationTests;user=sa;password=tMIxN11yGZgMC;encrypt=false;trustServerCertificate=true"

  def createTable(tableName: String): (Connection, String) =
    val con = DriverManager.getConnection(connectionString)
    val query = s"use IntegrationTests; drop table if exists dbo.$tableName; create table dbo.$tableName (Id int not null, Name nvarchar(10) not null)"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = s"use IntegrationTests; alter table dbo.$tableName add constraint pk_$tableName primary key(Id);"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = s"use IntegrationTests; alter table dbo.$tableName enable change_tracking;"
    statement.executeUpdate(enableCtCmd)

    (con, tableName)

  def withFreshTable(tableName: String)(test: ((Connection, String)) => Future[Assertion]): Future[Assertion] =
    test(createTable(tableName))
