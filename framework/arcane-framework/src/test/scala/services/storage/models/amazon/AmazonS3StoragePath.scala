import com.sneaksanddata.arcane.framework.services.storage.models.amazon.AmazonS3StoragePath
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

class AmazonS3StoragePath extends AnyFlatSpec {

  "AmazonS3StoragePath" should "be able to parse correct path" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath.fromHdfsPath(path)
    assert(parsed.isSuccess)
    assert(parsed.get.bucket == "bucket")
    assert(parsed.get.objectKey == "key")
  }

  it should "have stable serialization and deserialization" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath.fromHdfsPath(path)
    val serialized = AmazonS3StoragePath.fromHdfsPath(parsed.get.toHdfsPath)

    assert(parsed.isSuccess)
    assert(serialized.isSuccess)
    assert(parsed.get.bucket == serialized.get.bucket)
    assert(parsed.get.objectKey == serialized.get.objectKey)
    assert(parsed.get.toHdfsPath == path)
  }

  it should "be able to jon paths" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath.fromHdfsPath(path)
    assert(parsed.isSuccess)

    val parsedJoined = parsed.get.join("key2")
    assert(parsed.get.bucket == "bucket")
    assert(parsedJoined.objectKey == "key/key2")
  }


  val cases = Table(
    ("original", "joined", "expected"),  // First tuple defines column names
    ("s3a://bucket-name/", "/folder1///folder2/file.txt", "s3a://bucket-name//folder1///folder2/file.txt"), // Subsequent tuples define the data
    ("s3a://bucket-name/", "folder1///folder2/file.txt", "s3a://bucket-name/folder1///folder2/file.txt"),
    ("s3a://bucket-name", "folder1///folder2/file.txt", "s3a://bucket-name/folder1///folder2/file.txt"),
    ("s3a://bucket-name", "/folder1///folder2/file.txt", "s3a://bucket-name//folder1///folder2/file.txt")
  )

  forAll (cases) { (orig: String, rest: String, expected: String ) =>
    it should f"be able to remove extra slashes with values ($orig, $rest, $expected)" in {
      val parsed = AmazonS3StoragePath.fromHdfsPath(orig)
      assert(parsed.isSuccess)
      val result = parsed.get.join(rest)

      assert(result.toHdfsPath == expected)
    }
  }
}