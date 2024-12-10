package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import services.storage.base.BlobStorageReader

import com.azure.identity.{DefaultAzureCredential, DefaultAzureCredentialBuilder}
import com.azure.storage.blob.{BlobAsyncClient, BlobClient, BlobContainerAsyncClient, BlobContainerClient, BlobServiceClientBuilder}
import services.storage.models.base.StoredBlob
import services.storage.models.azure.AzureModelConversions.given

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}

import java.time.Duration
import scala.annotation.tailrec
import scala.concurrent.Future

final class AzureBlobStorageReader extends BlobStorageReader[AdlsStoragePath]:
  private val httpMaxRetries = 3
  private val httpRetryTimeout = Duration.ofSeconds(60)
  private val httpMinRetryDelay = Duration.ofMillis(500)
  private val httpMaxRetryDelay = Duration.ofSeconds(3)
  
  private lazy val defaultCredential = new DefaultAzureCredentialBuilder().build()
  private lazy val serviceClient = new BlobServiceClientBuilder()
    .credential(defaultCredential)
    .retryOptions(RequestRetryOptions(RetryPolicyType.EXPONENTIAL, httpMaxRetries, httpRetryTimeout.toSeconds.toInt, httpMinRetryDelay.toMillis, httpMaxRetryDelay.toMillis, null))
    .buildClient()
  private val defaultTimeout = Duration.ofSeconds(30)
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private def getBlobClient(blobPath: AdlsStoragePath): BlobClient =
    getBlobContainerClient(blobPath).getBlobClient(blobPath.blobPrefix)

  private def getBlobContainerClient(blobPath: AdlsStoragePath): BlobContainerClient =
    serviceClient.getBlobContainerClient(blobPath.container)

  def getBlobContent[Result](blobPath: AdlsStoragePath, deserializer: Array[Byte] => Result): Future[Result] =
    val client = getBlobClient(blobPath)
    Future(deserializer(client.downloadContent().toBytes))

  def listBlobs(blobPath: AdlsStoragePath): LazyList[StoredBlob] =
    val client = getBlobContainerClient(blobPath)
    val listOptions = new ListBlobsOptions().setPrefix(blobPath.blobPrefix)

    @tailrec
    def getPage(pageToken: Option[String], result: Iterable[StoredBlob]): Iterable[StoredBlob] =
      val page = client.listBlobs(listOptions, pageToken.orNull, defaultTimeout)
        .iterableByPage()
        .iterator()
        .next()

      val pageData = page.getValue.asScala.map(implicitly)

      if page.getContinuationToken.isEmpty then
        result ++ pageData
      else
        getPage(Some(page.getContinuationToken), result ++ pageData)

    LazyList.from(getPage(None, List()))


