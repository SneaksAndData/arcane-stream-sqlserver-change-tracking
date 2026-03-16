package com.sneaksanddata.arcane.sql_server_change_tracking
package tests.integration

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.{IntType, StringType}
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, IndexedField, IndexedMergeKeyField}
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.clearTarget
import zio.ZIO

import java.sql.{Connection, DriverManager}

object Fixtures:

  val connectionString: String =
    "jdbc:sqlserver://localhost:1433;user=sa;password=tMIxN11yGZgMC;encrypt=false;trustServerCertificate=true"
  val trinoConnectionString: String = "jdbc:trino://localhost:8080/iceberg/test?user=test"

  val initialSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      IndexedField(name = "Id", fieldType = IntType, fieldId = 1),
      IndexedField(name = "Name", fieldType = StringType, fieldId = 2),
      IndexedMergeKeyField(3)
    )
  )

  def getConnection: Connection =
    DriverManager.getConnection(connectionString)

  def createFreshSource(dbName: String, tableName: String): Connection =
    val con = getConnection
    val query =
      s"use $dbName; drop table if exists dbo.$tableName; create table dbo.$tableName (Id int not null, Name nvarchar(10) not null)"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = s"use $dbName; alter table dbo.$tableName add constraint pk_$tableName primary key(Id);"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = s"use $dbName; alter table dbo.$tableName enable change_tracking;"
    statement.executeUpdate(enableCtCmd)

    con

  def withFreshTablesZIO(
      sourceDbName: String,
      sourceTableName: String,
      targetTableName: String
  ): ZIO[Any, Throwable, Unit] =
    for
      _ <- ZIO.attempt(createFreshSource(sourceDbName, sourceTableName))
      _ <- clearTarget(targetTableName)
    yield ()
