package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import services.connectors.base.SchemaProvider
import services.connectors.mssql.MsSqlConnector.ToParquetSchema

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type, Types}
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types.GroupBuilder

import java.sql.{ResultSet, ResultSetMetaData}
import java.util.Properties
import scala.concurrent.{Future, blocking}
import scala.util.{Try, Using}


class MsSqlConnector extends SchemaProvider[MessageType]:
  private val connectionUrl = "jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC"
  private val table = "arcane"
  private val driver = new SQLServerDriver()
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override def getSchema: Future[MessageType] =
    Future {
      val schma = Using.Manager { use =>
        val connection = blocking(use(driver.connect(connectionUrl, new Properties())))
        val statement = use(connection.createStatement())
        val resultSet = blocking {
          statement.executeQuery(s"use arcane; SELECT * FROM dbo.MsSqlConnectorsTests")
        }
        blocking(resultSet.getMetaData).ToParquetSchema
      }
      schma.get
    }

case class ConnectionOptions(connectionUrl: String, schemaName: String, tableName: String)

object MsSqlConnector:
  def apply(msSqlConnectorOptions: ConnectionOptions): MsSqlConnector = new MsSqlConnector()

  extension (rsm: ResultSetMetaData) def ToParquetSchema: MessageType =
    val gb: GroupBuilder[MessageType] = Types.buildMessage()
    for columnIndex <- 0 until rsm.getColumnCount do
      gb.addField(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "Type"))
    gb.named("root")
