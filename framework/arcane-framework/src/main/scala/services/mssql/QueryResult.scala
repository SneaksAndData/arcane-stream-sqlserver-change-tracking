package com.sneaksanddata.arcane.framework
package services.mssql

import models.{DataColumn, DataRow}

import java.sql.{ResultSet, Statement}
import scala.annotation.tailrec

/**
 * Represents the result of a query to a SQL database.
 */
trait QueryResult[Output] {
  
  type OutputType = Output

  /**
   * Reads the result of the SQL query mapped to an output type.
   *
   * @return The result of the query.
   */
  def read: OutputType

}

/**
 * Lazy-list based implementation of [[QueryResult]].
 *
 * @param statement The statement used to execute the query.
 * @param resultSet The result set of the query.
 */
class LazyQueryResult(statement: Statement, resultSet: ResultSet) extends QueryResult[LazyList[DataRow]] with AutoCloseable {

  /**
   * Reads the result of the query.
   *
   * @return The result of the query.
   */
  override def read: this.OutputType =
    val columns = resultSet.getMetaData.getColumnCount
    LazyList.continually(resultSet)
      .takeWhile(_.next())
      .map(row => toDataRow(row, columns, List.empty))


  /**
   * Closes the statement and the result set owned by this object.
   * When a Statement object is closed, its current ResultSet object, if one exists, is also closed.
   */
  override def close(): Unit = statement.close()

  @tailrec
  private def toDataRow(row: ResultSet, columns: Int, acc: DataRow): DataRow =
    if columns == 0 then acc
    else
      val name = row.getMetaData.getColumnName(columns)
      val value = row.getObject(columns)
      val dataType = row.getMetaData.getColumnType(columns)
      val arcaneType = MsSqlConnection.toArcaneType(dataType)
      toDataRow(row, columns - 1, DataColumn(name, arcaneType, value) :: acc)

}

/**
 * Companion object for [[LazyQueryResult]].
 */
object LazyQueryResult {
  
  /**
   * The output type of the query result.
   */
  type OutputType = LazyList[DataRow]
  
  /**
   * Creates a new [[LazyQueryResult]] object.
   *
   * @param statement The statement used to execute the query.
   * @param resultSet The result set of the query.
   * @return The new [[LazyQueryResult]] object.
   */
  def apply(statement: Statement, resultSet: ResultSet): LazyQueryResult = new LazyQueryResult(statement, resultSet)
}