package uk.gov.justice.digital.hmpps.landingzoneprocessing.client

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.io.InputStream

class AwsS3Client(private val s3: S3Client = S3Client.create()): AutoCloseable {

    fun putBytes(bytes: ByteArray, bucketName: String, fileKey: String) {
        s3.putObject(
            PutObjectRequest
                .builder()
                .bucket(bucketName)
                .key(fileKey)
                .build(),
            RequestBody.fromBytes(bytes)
        )
    }

    fun deleteFile(bucketName: String, fileKey: String) {
        val deleteObjectRequest = DeleteObjectRequest
            .builder()
            .bucket(bucketName)
            .key(fileKey)
            .build()

        s3.deleteObject(deleteObjectRequest)
    }

    fun toInputStream(bucketName: String, fileKey: String): InputStream {
        val getObjectRequest = GetObjectRequest
            .builder()
            .bucket(bucketName)
            .key(fileKey)
            .build()

        return s3.getObject(getObjectRequest)
    }

    fun listFiles(bucketName: String, prefix: String): List<S3Object> {
        val files = mutableListOf<S3Object>()
        var continuationToken: String? = null
        // Paginate through all files
        do {
            val request = ListObjectsV2Request
                .builder()
                .bucket(bucketName)
                .prefix(prefix)
                .continuationToken(continuationToken)
                .build()

            val response = s3.listObjectsV2(request)
            files.addAll(response.contents())
            continuationToken = response.continuationToken()
        } while (response.isTruncated)
        return files
    }

    fun copyFile(
        sourceBucketName: String,
        sourceFileKey: String,
        destinationBucketName: String,
        destinationFileKey: String
    ) {
        val request = CopyObjectRequest
            .builder()
            .sourceBucket(sourceBucketName)
            .sourceKey(sourceFileKey)
            .destinationBucket(destinationBucketName)
            .destinationKey(destinationFileKey)
            .build()
        s3.copyObject(request)
    }

    override fun close() = s3.close()
}