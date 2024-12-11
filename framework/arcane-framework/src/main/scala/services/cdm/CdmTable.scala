package com.sneaksanddata.arcane.framework
package services.cdm

import models.cdm.CSVParser.isComplete
import models.cdm.{SimpleCdmEntity, given}
import models.{ArcaneSchema, DataRow}
import services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReader}

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

class CdmTable(name: String, storagePath: AdlsStoragePath, entityModel: SimpleCdmEntity, reader: AzureBlobStorageReader):
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val defaultFromYears: Int = 5
  private val schema: ArcaneSchema = implicitly(entityModel)

  private def getListPrefixes(startDate: Option[OffsetDateTime]): IndexedSeq[String] =
    val currentMoment = OffsetDateTime.now(ZoneOffset.UTC)
    val startMoment = startDate.getOrElse(currentMoment.minusYears(defaultFromYears))
    Range.inclusive(
      startMoment.getYear,
      currentMoment.getYear
    ).flatMap(year => Range.inclusive(
      1,
      12
    ).map { m =>
      val mon = s"00$m".takeRight(2)
      (s"$year-$mon-", year, m)
    }).collect {
      // include all prefixes from previous years
      // in case year for both dates is the same, we will never hit this case
      case (prefix, year, _) if year < currentMoment.getYear => prefix
      // only include prefixes for current year that are less than current month
      // this only applies to the case when startMoment year is less than current moment - then we take months from 1 to current month
      case (prefix, year, mon) if (year == currentMoment.getYear) && (currentMoment.getYear > startMoment.getYear) && (mon <= currentMoment.getMonth.getValue) => prefix
      // in case both dates are in the same year, we limit month selection to start from startMoment month
      case (prefix, year, mon) if (year == currentMoment.getYear) && (currentMoment.getYear == startMoment.getYear) && (mon >= startMoment.getMonth.getValue) && (mon <= currentMoment.getMonth.getValue) => prefix
    }

  /**
   * Read a table snapshot, taking optional start time.
   * @param startDate Folders from Synapse export to include in the snapshot, based on the start date provided. If not provided, ALL folders from now - defaultFromYears will be included
   * @return A stream of rows for this table
   */
  def snapshot(startDate: Option[OffsetDateTime] = None): Future[LazyList[DataRow]] =
    // list all matching blobs
    Future.sequence(getListPrefixes(startDate)
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
