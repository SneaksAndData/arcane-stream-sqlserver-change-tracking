package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import org.scalatest.Assertion
import zio.ZIO

import java.sql.{Connection, DriverManager}
import scala.concurrent.Future

object Fixtures:

  val connectionString: String = sys.env("ARCANE__CONNECTIONSTRING")
  val trinoConnectionString: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  def getConnection: Connection =
    DriverManager.getConnection(connectionString)

  def createFreshSource(tableName: String): Connection  =
    val con = getConnection
    val query = s"use IntegrationTests; drop table if exists dbo.$tableName; create table dbo.$tableName (Id int not null, Name nvarchar(10) not null)"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = s"use IntegrationTests; alter table dbo.$tableName add constraint pk_$tableName primary key(Id);"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = s"use IntegrationTests; alter table dbo.$tableName enable change_tracking;"
    statement.executeUpdate(enableCtCmd)

    con

  def clearTarget(targetFullName: String): Any =
    val trinoConnection = DriverManager.getConnection(trinoConnectionString)
    val query = s"drop table if exists $targetFullName"
    val statement = trinoConnection.createStatement()
    statement.executeUpdate(query)


  def withFreshTables(sourceTableName: String, targetTableName: String)(test: Connection => Future[Assertion]): Future[Assertion] =
    clearTarget(targetTableName)
    test(createFreshSource(sourceTableName))

  def withFreshTablesZIO(sourceTableName: String, targetTableName: String): ZIO[Any, Nothing, Unit] =
    for _ <- ZIO.succeed(createFreshSource(sourceTableName))
        _   <- ZIO.succeed(clearTarget(targetTableName))
    yield ()
