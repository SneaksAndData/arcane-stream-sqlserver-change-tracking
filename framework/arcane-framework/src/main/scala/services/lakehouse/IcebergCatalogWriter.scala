package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, DataRow}
import services.lakehouse.SchemaConversions.*

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.{DataFile, Schema, Table}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList

import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}


// https://www.tabular.io/blog/java-api-part-3/
class IcebergCatalogWriter(
                          namespace: String,
                          warehouse: String,
                          uri: String,
                          additionalProperties: Map[String, String],
                          schema: ArcaneSchema
                        ) extends CatalogWriter[RESTCatalog, Table]:

  private def createTable(name: String, schema: Schema): Future[Table] =
    val tableId = TableIdentifier.of(namespace, name)
    // TODO: add support for partition spec
    Future(catalog.createTable(tableId, schema, PartitionSpec.unpartitioned()))

  private def rowToRecord(row: DataRow)(implicit tbl: Table): GenericRecord =
    val record = GenericRecord.create(tbl.schema())
    val rowMap = row.map { cell => cell.name -> cell.value }.toMap
    record.copy(rowMap.asJava)

  private def appendData(data: Iterable[DataRow])(implicit tbl: Table): Future[Table] = Future {
      val appendTran = tbl.newTransaction()
      // create iceberg records
      val records = data.map(rowToRecord).foldLeft(ImmutableList.builder[GenericRecord]) {
        (builder, record) => builder.add(record)
      }.build()
      val file = tbl.io.newOutputFile(tbl.location + "/" + UUID.randomUUID.toString)
      val dataWriter = Parquet.writeData(file)
          .schema(tbl.schema())
          .createWriterFunc(GenericParquetWriter.buildWriter)
          .overwrite()
          .withSpec(PartitionSpec.unpartitioned())
          .build[GenericRecord]()

      Try(for (record <- records.asScala) { dataWriter.write(record) }) match {
        case Success(writer) =>
          appendTran.newFastAppend().appendFile(dataWriter.toDataFile).commit()
          dataWriter.close()
          appendTran.commitTransaction()
          tbl
        case Failure(ex) =>
          dataWriter.close()
          throw ex
      }
    }

  override def write(data: Iterable[DataRow], name: String): Future[Table] =
    createTable(name, schema).flatMap(appendData(data))

  override implicit val catalog: RESTCatalog = new RESTCatalog()
  override implicit val catalogProperties: Map[String, String] = Map(
    "warehouse" -> warehouse,
    "uri" -> uri
  ) ++ additionalProperties

  override implicit val catalogName: String = java.util.UUID.randomUUID.toString

  def initialize(): Unit =
    //catalog.setConf(org.apache.hadoop.conf.Configuration())
    catalog.initialize(catalogName, catalogProperties.asJava)

  override def delete(tableName: String): Future[Unit] = ???
