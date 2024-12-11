package com.sneaksanddata.arcane.framework
package models

import models.cdm.SimpleCdmModel
import services.storage.models.azure.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import services.cdm.{CdmTable, CdmTableSettings}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should

import java.time.{OffsetDateTime, ZoneOffset}


class CdmTableTests extends AsyncFlatSpec with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val integrationTestContainer = sys.env.get("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  private val integrationTestAccount = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  private val integrationTestAccessKey = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")

  it should "read model schemas from Synapse Link blob storage" in {
    (integrationTestContainer, integrationTestAccount, integrationTestAccessKey) match
      case (Some(container), Some(account), Some(key)) => SimpleCdmModel(s"abfss://$container@$account.dfs.core.windows.net/", AzureBlobStorageReader(account, StorageSharedKeyCredential(account, key))).map { model =>
        model.entities.size should be > 0
      }
      case _ => cancel("Skipping test since it is not configured to run")
  }

  it should "read a CDM table from Synapse Link blob storage" in {
    (integrationTestContainer, integrationTestAccount, integrationTestAccessKey) match
      case (Some(container), Some(account), Some(key)) => SimpleCdmModel(s"abfss://$container@$account.dfs.core.windows.net/", AzureBlobStorageReader(account, StorageSharedKeyCredential(account, key))).flatMap { model =>
        val entityToRead = model.entities.find(v => v.name == "salesline").get
        CdmTable(CdmTableSettings(name = entityToRead.name, rootPath = s"abfss://$container@$account.dfs.core.windows.net/"), entityToRead, AzureBlobStorageReader(account, StorageSharedKeyCredential(account, key)))
          .snapshot(Some(OffsetDateTime.now(ZoneOffset.UTC).minusHours(6)))
          .map { rows =>
            rows.foldLeft(0L){ (agg, _) => agg + 1 } should be > 0L
          }
      }
      case _ => cancel("Skipping test since it is not configured to run")
  }
}
