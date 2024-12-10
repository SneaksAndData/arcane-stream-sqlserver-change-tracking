package com.sneaksanddata.arcane.framework
package services.cdm

import models.cdm.CSVParser.isComplete
import models.cdm.{SimpleCdmEntity, given}
import models.{ArcaneSchema, DataRow}
import services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReader}

import java.time.{OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

class CdmTable(name: String, storagePath: AdlsStoragePath, entityModel: SimpleCdmEntity, reader: AzureBlobStorageReader):
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val defaultFromYears: Int = 5
  private val schema: ArcaneSchema = implicitly(entityModel)

  private def getListPrefixes(fromYears: Option[Int]): IndexedSeq[String] =
    val currentMoment = OffsetDateTime.now(ZoneOffset.UTC)
    val fromMoment = currentMoment.minusYears(fromYears.getOrElse(defaultFromYears))
    Range.inclusive(
      fromMoment.getYear,
      currentMoment.getYear
    ).flatMap(year => Range.inclusive(
      1,
      12
    ).map{ m =>
      val mon = s"00$m".takeRight(2)
      s"$year-$mon-"
    })

  /**
   * Read a table snapshot, taking optional start time.
   * @param fromYears Folders from Synapse export to include in the snapshot. If not provided, ALL folders will be included
   * @return A stream of rows for this table
   */
  def snapshot(fromYears: Option[Int]): Future[LazyList[DataRow]] =
    // list all matching blobs
    Future.sequence(getListPrefixes(fromYears)
      .flatMap(prefix => reader.listBlobs(storagePath + prefix))
      .map(blob => reader.getBlobContent(storagePath + blob.name, _.map(_.toChar).mkString)))
      .map(_.flatMap(content => content.split('\n').foldLeft((Seq.empty[String], "")) { (agg, value) =>
        if isComplete(agg._2) then
          (agg._1 :+ agg._2, "")
        else
          (agg._1, agg._2 + value)
      }._1.map(implicitly[DataRow](_, schema))))
      .map(LazyList.from)

object CdmTable:
  def apply(settings: CdmTableSettings, entityModel: SimpleCdmEntity, reader: AzureBlobStorageReader): CdmTable = new CdmTable(
    name = settings.name,
    storagePath = AdlsStoragePath(settings.rootPath).get,
    entityModel = entityModel,
    reader = reader
  )
