package uk.gov.justice.digital.hmpps.landingzoneprocessing.model

import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8

/**
 * Represents a file in S3.
 */
data class S3File(
    val bucketName: String,
    val key: String
) {
    companion object {

        fun inputFileFromLambdaPayload(payload: MutableMap<String, Any>): S3File {
            val record = extractRecord(payload)
            val inputS3BucketName = extractBucketName(record)
            val s3ObjectKey = extractObjectKey(record)
            return S3File(bucketName = inputS3BucketName, key = s3ObjectKey)
        }

        fun avroSchemaFileFromInputFileKey(schemaRegistryBucketName: String, inputFileKey: String): S3File {
            val schemaObjectKey = "${s3FilePrefix(inputFileKey)}.avsc"
            return S3File(bucketName = schemaRegistryBucketName, key = schemaObjectKey)
        }

        fun outputFileFromInputFileKey(outputBucketName: String, inputFileKey: String): S3File {
            val prefixInBucket = ensureSlash(s3FilePrefix(inputFileKey))
            // This filename prefix is consistent with what the DMS outputs and is
            // used by the batch job to identify full load files by name.
            val fileNamePrefix = "LOAD"
            val outputObjectKey = "${prefixInBucket}$fileNamePrefix-${s3FileNameWithoutPrefix(inputFileKey)}.parquet"
            return S3File(bucketName = outputBucketName, key = outputObjectKey)
        }

        fun violationsFileFromInputFileKey(violationsBucketName: String, violationsPath: String, inputFileKey: String): S3File {
            val violationsObjectKey = "${ensureSlash(violationsPath)}$inputFileKey"
            return S3File(bucketName = violationsBucketName, key = violationsObjectKey)
        }

        private fun ensureSlash(path: String): String {
            return if (path.endsWith("/")) {
                path
            } else {
                "$path/"
            }
        }

        private fun extractRecord(payload: MutableMap<String, Any>): MutableMap<String, Any>? {
            return (payload["Records"] as List<MutableMap<String, Any>>?)?.get(0)
        }

        private fun extractBucketName(record: MutableMap<String, Any>?): String {
            val s3 = record?.get("s3") as Map<String, Any>?
            val bucket = s3?.get("bucket") as Map<String, Any>?
            val name = bucket?.get("name") as String?
            return requireNotNull(name) { "Input bucket name must not be null" }
        }

        private fun urlDecode(encodedUrl: String): String = URLDecoder.decode(encodedUrl, UTF_8.name())

        private fun extractObjectKey(record: MutableMap<String, Any>?): String {
            val s3 = record?.get("s3") as Map<String, Any>?
            val obj = s3?.get("object") as Map<String, Any>?
            val key = obj?.get("key") as String?
            return urlDecode(requireNotNull(key) { "input object key must not be null" })
        }

        private fun s3FilePrefix(s3ObjectKey: String): String = s3ObjectKey.substringBeforeLast('/')
        private fun s3FileNameWithoutPrefix(s3ObjectKey: String): String = s3ObjectKey.substringAfterLast('/')
    }
}
