package com.sneaksanddata.arcane.framework
package services.mssql

import models.{ArcaneSchema, ArcaneType, Field}
import services.base.{CanAdd, SchemaProvider}
import services.mssql.MsSqlConnection.{DATE_PARTITION_KEY, UPSERT_MERGE_KEY, toArcaneType}
import services.mssql.base.QueryResult
import services.mssql.query.{LazyQueryResult, QueryRunner, ScalarQueryResult}

import com.microsoft.sqlserver.jdbc.SQLServerDriver

import java.sql.ResultSet
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import java.util.Properties
import scala.annotation.tailrec
import scala.concurrent.{Future, blocking}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/**
 * Represents a summary of a column in a table.
 * The first element is the name of the column, and the second element is true if the column is a primary key.
 */
type ColumnSummary = (String, Boolean)

/**
 * Represents a query to be executed on a Microsoft SQL Server database.
 */
type MsSqlQuery = String

/**
 * Represents the schema of a table in a Microsoft SQL Server database.
 */
type SqlSchema = Seq[(String, Int)]

/**
 * Represents the connection options for a Microsoft SQL Server database.
 *
 * @param connectionUrl       The connection URL for the database.
 * @param databaseName        The name of the database.
 * @param schemaName          The name of the schema.
 * @param tableName           The name of the table.
 * @param partitionExpression The partition expression for the table.
 */
case class ConnectionOptions(connectionUrl: String,
                             databaseName: String,
                             schemaName: String,
                             tableName: String,
                             partitionExpression: Option[String])

/**
 * Required typeclass implementation
 */
given CanAdd[ArcaneSchema] with
  extension (a: ArcaneSchema) def addField(fieldName: String, fieldType: ArcaneType): ArcaneSchema = a :+ Field(fieldName, fieldType)

/**
 * Represents a connection to a Microsoft SQL Server database.
 *
 * @param connectionOptions The connection options for the database.
 */
class MsSqlConnection(val connectionOptions: ConnectionOptions) extends AutoCloseable with SchemaProvider[ArcaneSchema]:
  private val driver = new SQLServerDriver()
  private val connection = driver.connect(connectionOptions.connectionUrl, new Properties())
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private implicit val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /**
   * Gets the column summaries for the table in the database.
   *
   * @return A future containing the column summaries for the table in the database.
   */
  def getColumnSummaries: Future[List[ColumnSummary]] =
    val query = QueryProvider.getColumnSummariesQuery(connectionOptions.schemaName, connectionOptions.tableName, connectionOptions.databaseName)
    Future {
      val result = Using.Manager { use =>
        val statement = use(connection.createStatement())
        val resultSet = use(statement.executeQuery(query))
        blocking {
          readColumns(resultSet, List.empty)
        }
      }
      result.get
    }

  /**
   * Run a backfill query on the database.
   *
   * @param arcaneSchema The schema for the data produced by Arcane.
   * @return A future containing the result of the backfill.
   */
  def backfill(arcaneSchema: ArcaneSchema)(using queryRunner: QueryRunner): Future[QueryResult[LazyQueryResult.OutputType]] =
    for query <- QueryProvider.getBackfillQuery(this)
        result <- queryRunner.executeQuery(query, connection, LazyQueryResult.apply)
    yield result

  /**
   * Gets the changes in the database since the given version.
   * @param maybeLatestVersion The version to start from.
   * @param lookBackInterval The look back interval for the query.
   * @return A future containing the changes in the database since the given version and the latest observed version.
   */
  def getChanges(maybeLatestVersion: Option[Long], lookBackInterval: Duration)(using queryRunner: QueryRunner): Future[(QueryResult[LazyQueryResult.OutputType], Long)] =
    val query = QueryProvider.getChangeTrackingVersionQuery(connectionOptions.databaseName, maybeLatestVersion, lookBackInterval)

    for versionResult <- queryRunner.executeQuery(query, connection, (st, rs) => ScalarQueryResult.apply(st, rs, readChangeTrackingVersion))
        version = versionResult.read.getOrElse(maybeLatestVersion.getOrElse(0L))
        changesQuery <- QueryProvider.getChangesQuery(this, version - 1)
        result <- queryRunner.executeQuery(changesQuery, connection, LazyQueryResult.apply)
    yield (result, version)

  private def readChangeTrackingVersion(resultSet: ResultSet): Long =
    resultSet.getMetaData.getColumnType(1) match
      case java.sql.Types.BIGINT => resultSet.getLong(1)
      case _ => throw new IllegalArgumentException(s"Invalid column type for change tracking version: ${resultSet.getMetaData.getColumnType(1)}, expected BIGINT")

  /**
   * Closes the connection to the database.
   */
  override def close(): Unit = connection.close()

  /**
   * Gets an empty schema.
   *
   * @return An empty schema.
   */
  override def empty: this.SchemaType = ArcaneSchema.empty()

  /**
   * Gets the schema for the data produced by Arcane.
   *
   * @return A future containing the schema for the data produced by Arcane.
   */
  override def getSchema: Future[this.SchemaType] =
    for query <- QueryProvider.getSchemaQuery(this)
        sqlSchema <- getSqlSchema(query)
    yield toSchema(sqlSchema, empty) match
      case Success(schema) => schema
      case Failure(exception) => throw exception

  private def getSqlSchema(query: String): Future[SqlSchema] = Future {
    val columns = Using.Manager { use =>
      val statement = use(connection.createStatement())
      val resultSet = blocking {
        use(statement.executeQuery(query))
      }
      val metadata = resultSet.getMetaData
      for i <- 1 to metadata.getColumnCount yield (metadata.getColumnName(i), metadata.getColumnType(i))
    }
    columns.get
  }

  @tailrec
  private def toSchema(sqlSchema: SqlSchema, schema: this.SchemaType): Try[this.SchemaType] =
    sqlSchema match
      case Nil => Success(schema)
      case x +: xs =>
        val (name, fieldType) = x
        toArcaneType(fieldType) match
          case Success(arcaneType) => toSchema(xs, schema.addField(name, arcaneType))
          case Failure(exception) => Failure[this.SchemaType](exception)

  @tailrec
  private def readColumns(resultSet: ResultSet, result: List[ColumnSummary]): List[ColumnSummary] =
    val hasNext = resultSet.next()

    if !hasNext then
      return result
    readColumns(resultSet, result ++ List((resultSet.getString(1), resultSet.getInt(2) == 1)))

object MsSqlConnection:
  /**
   * The key used to merge rows in the output table.
   */
  val UPSERT_MERGE_KEY = "ARCANE_MERGE_KEY"

  /**
   * The key used to partition the output table by date.
   */
  val DATE_PARTITION_KEY = "DATE_PARTITION_KEY"

  /**
   * Creates a new Microsoft SQL Server connection.
   *
   * @param connectionOptions The connection options for the database.
   * @return A new Microsoft SQL Server connection.
   */
  def apply(connectionOptions: ConnectionOptions): MsSqlConnection = new MsSqlConnection(connectionOptions)

  /**
   * Converts a SQL type to an Arcane type.
   *
   * @param sqlType The SQL type.
   * @return The Arcane type.
   */
  def toArcaneType(sqlType: Int): Try[ArcaneType] = sqlType match
    case java.sql.Types.BIGINT => Success(ArcaneType.LongType)
    case java.sql.Types.BINARY => Success(ArcaneType.ByteArrayType)
    case java.sql.Types.BIT => Success(ArcaneType.BooleanType)
    case java.sql.Types.CHAR => Success(ArcaneType.StringType)
    case java.sql.Types.DATE => Success(ArcaneType.DateType)
    case java.sql.Types.TIMESTAMP => Success(ArcaneType.TimestampType)
    case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => Success(ArcaneType.DateTimeOffsetType)
    case java.sql.Types.DECIMAL => Success(ArcaneType.BigDecimalType)
    case java.sql.Types.DOUBLE => Success(ArcaneType.DoubleType)
    case java.sql.Types.INTEGER => Success(ArcaneType.IntType)
    case java.sql.Types.FLOAT => Success(ArcaneType.FloatType)
    case java.sql.Types.SMALLINT => Success(ArcaneType.ShortType)
    case java.sql.Types.TIME => Success(ArcaneType.TimeType)
    case java.sql.Types.NCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.NVARCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.VARCHAR => Success(ArcaneType.StringType)
    case _ => Failure(new IllegalArgumentException(s"Unsupported SQL type: $sqlType"))


object QueryProvider:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  /**
   * Gets the schema query for the Microsoft SQL Server database.
   *
   * @param msSqlConnection The connection to the database.
   * @return A future containing the schema query for the Microsoft SQL Server database.
   */
  def getSchemaQuery(msSqlConnection: MsSqlConnection): Future[MsSqlQuery] =
    msSqlConnection.getColumnSummaries
      .map(columnSummaries => {
        val mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        val columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        val matchStatement = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        QueryProvider.getChangesQuery(
          msSqlConnection.connectionOptions,
          mergeExpression,
          columnExpression,
          matchStatement,
          Long.MaxValue)
      })

  /**
   * Gets the changes query for the Microsoft SQL Server database.
   * @param msSqlConnection The connection to the database.
   * @param fromVersion The version to start from.
   * @return A future containing the changes query for the Microsoft SQL Server database.
   */
  def getChangesQuery(msSqlConnection: MsSqlConnection, fromVersion: Long): Future[MsSqlQuery] =
    msSqlConnection.getColumnSummaries
      .map(columnSummaries => {
        val mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        val columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        val matchStatement = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        QueryProvider.getChangesQuery(
          msSqlConnection.connectionOptions,
          mergeExpression,
          columnExpression,
          matchStatement,
          fromVersion)
      })

  /**
   * Gets the column summaries query for the Microsoft SQL Server database.
   *
   * @param schemaName   The name of the schema.
   * @param tableName    The name of the table.
   * @param databaseName The name of the database.
   * @return The column summaries query for the Microsoft SQL Server database.
   */
  def getColumnSummariesQuery(schemaName: String, tableName: String, databaseName: String): MsSqlQuery =
    Source.fromResource("get_column_summaries.sql")
      .getLines
      .mkString("\n")
      .replace("{dbName}", databaseName)
      .replace("{schema}", schemaName)
      .replace("{table}", tableName)

  /**
   * Gets the changes query for the Microsoft SQL Server database.
   * @param msSqlConnection The connection to the database.
   * @return A future containing the changes query for the Microsoft SQL Server database.
   */
  def getBackfillQuery(msSqlConnection: MsSqlConnection): Future[MsSqlQuery] =
    msSqlConnection.getColumnSummaries
      .map(columnSummaries => {
        val mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        val columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "tq")
        QueryProvider.getAllQuery(
          msSqlConnection.connectionOptions,
          mergeExpression,
          columnExpression)
      })

  /**
   * Gets the query that retrieves the change tracking version for the Microsoft SQL Server database.
   *
   * @param databaseName The name of the database.
   * @param maybeVersion The version to start from.
   * @param lookBackRange The look back range for the query.
   * @return The change tracking version query for the Microsoft SQL Server database.
   */
  def getChangeTrackingVersionQuery(databaseName: String, maybeVersion: Option[Long], lookBackRange: Duration)(using formatter: DateTimeFormatter): MsSqlQuery = {
    maybeVersion match
      case None =>
        val lookBackTime = Instant.now().minusSeconds(lookBackRange.getSeconds)
        val formattedTime = formatter.format(LocalDateTime.ofInstant(lookBackTime, ZoneOffset.UTC))
        s"SELECT MIN(commit_ts) FROM $databaseName.sys.dm_tran_commit_table WHERE commit_time > '$formattedTime'"
      case Some(version) => s"SELECT MIN(commit_ts) FROM $databaseName.sys.dm_tran_commit_table WHERE commit_ts > $version"
  }

  private def getMergeExpression(cs: List[ColumnSummary], tableAlias: String): String =
    cs.filter((name, isPrimaryKey) => isPrimaryKey)
      .map((name, _) => s"cast($tableAlias.[$name] as nvarchar(128))")
      .mkString(" + '#' + ")

  private def getMatchStatement(cs: List[ColumnSummary], sourceAlias: String, outputAlias: String, partitionColumns: Option[List[String]]): String =
    val mainMatch = cs.filter((_, isPrimaryKey) => isPrimaryKey)
      .map((name, _) => s"$outputAlias.[$name] = $sourceAlias.[$name]")
      .mkString(" and ")

    partitionColumns match
      case Some(columns) =>
        val partitionMatch = columns
          .map(column => s"$outputAlias.[$column] = $sourceAlias.[$column]")
          .mkString(" and ")
        s"$mainMatch and  ($sourceAlias.SYS_CHANGE_OPERATION == 'D' OR ($partitionMatch))"
      case None => mainMatch


  private def getChangeTrackingColumns(tableColumns: List[ColumnSummary], changesAlias: String, tableAlias: String): String =
    val primaryKeyColumns = tableColumns.filter((_, isPrimaryKey) => isPrimaryKey).map((name, _) => s"$changesAlias.[$name]")
    val additionalColumns = List(s"$changesAlias.SYS_CHANGE_VERSION", s"$changesAlias.SYS_CHANGE_OPERATION")
    val nonPrimaryKeyColumns = tableColumns
      .filter((name, isPrimaryKey) => !isPrimaryKey && !Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION").contains(name))
      .map((name, _) => s"$tableAlias.[$name]")
    (primaryKeyColumns ++ additionalColumns ++ nonPrimaryKeyColumns).mkString(",\n")

  private def getChangeTrackingColumns(tableColumns: List[ColumnSummary], tableAlias: String): String =
    val primaryKeyColumns = tableColumns.filter((_, isPrimaryKey) => isPrimaryKey).map((name, _) => s"$tableAlias.[$name]")
    val additionalColumns = List("0 as SYS_CHANGE_VERSION", "'I' as SYS_CHANGE_OPERATION")
    val nonPrimaryKeyColumns = tableColumns
      .filter((name, isPrimaryKey) => !isPrimaryKey && !Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION").contains(name))
      .map((name, _) => s"$tableAlias.[$name]")

    (primaryKeyColumns ++ additionalColumns ++ nonPrimaryKeyColumns).mkString(",\n")

  private def getChangesQuery(connectionOptions: ConnectionOptions,
                              mergeExpression: String,
                              columnStatement: String,
                              matchStatement: String,
                              changeTrackingId: Long): String =
    val baseQuery = connectionOptions.partitionExpression match {
      case Some(_) => Source.fromResource("get_select_delta_query_date_partitioned.sql").getLines.mkString("\n")
      case None => Source.fromResource("get_select_delta_query.sql").getLines.mkString("\n")
    }

    baseQuery.replace("{dbName}", connectionOptions.databaseName)
      .replace("{schema}", connectionOptions.schemaName)
      .replace("{tableName}", connectionOptions.tableName)
      .replace("{ChangeTrackingColumnsStatement}", columnStatement)
      .replace("{ChangeTrackingMatchStatement}", matchStatement)
      .replace("{MERGE_EXPRESSION}", mergeExpression)
      .replace("{MERGE_KEY}", UPSERT_MERGE_KEY)
      .replace("{DATE_PARTITION_EXPRESSION}", connectionOptions.partitionExpression.getOrElse(""))
      .replace("{DATE_PARTITION_KEY}", DATE_PARTITION_KEY)
      .replace("{lastId}", changeTrackingId.toString)

  private def getAllQuery(connectionOptions: ConnectionOptions,
                  mergeExpression: String,
                  columnExpression: String): String = {

    val baseQuery = connectionOptions.partitionExpression match {
      case Some(_) => Source.fromResource("get_select_all_query_date_partitioned.sql").getLines.mkString("\n")
      case None => Source.fromResource("get_select_all_query.sql").getLines.mkString("\n")
    }

    baseQuery
      .replace("{dbName}", connectionOptions.databaseName)
      .replace("{schema}", connectionOptions.schemaName)
      .replace("{tableName}", connectionOptions.tableName)
      .replace("{ChangeTrackingColumnsStatement}", columnExpression)
      .replace("{MERGE_EXPRESSION}", mergeExpression)
      .replace("{MERGE_KEY}", UPSERT_MERGE_KEY)
      .replace("{DATE_PARTITION_EXPRESSION}", connectionOptions.partitionExpression.getOrElse(""))
      .replace("{DATE_PARTITION_KEY}", DATE_PARTITION_KEY)
  }
