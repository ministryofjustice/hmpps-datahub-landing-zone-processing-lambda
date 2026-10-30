package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel.WARN
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import org.apache.avro.Schema
import software.amazon.awssdk.services.s3.model.S3Object
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsS3Client
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import java.nio.charset.Charset

/**
 * Interaction with S3
 */
class S3FileService(
    private val s3Client: AwsS3Client,
    private val logger: LambdaLogger
) {
    fun retrieveCsvRows(
        file: S3File,
        numberOfHeaderRowsToSkips: Int,
        charSet: Charset
    ): List<List<String>> {
        val inputStream = s3Client.toInputStream(file.bucketName, file.key)
        val rows = inputStream.use { ins ->
            csvReader {
                charset = charSet.name()
            }.readAll(ins)
        }
        logger.log("Read ${rows.size} rows, including header rows, from ${file.uri}")
        if (numberOfHeaderRowsToSkips  > 0) {
            require(rows.size >= numberOfHeaderRowsToSkips) { "Configured to skip $numberOfHeaderRowsToSkips header rows but ${file.uri} has only ${rows.size} rows" }
            return rows.drop(numberOfHeaderRowsToSkips)
        } else {
            return rows
        }
    }

    fun retrieveSchema(file: S3File): Schema {
        s3Client.toInputStream(file.bucketName, file.key).use { inputStream ->
            return Schema.Parser().parse(inputStream)
        }
    }

    /**
     * Lists the files under the given prefix, leaving out folder markers and empty objects, which have no CSV
     * data to convert.
     */
    fun listFiles(file: S3File): List<S3File> {
        val s3Objects = s3Client.listFiles(file.bucketName, file.key)
        logger.log("Found ${s3Objects.size} objects under ${file.uri}")
        return s3Objects.mapNotNull { o ->
            val listedFile = S3File(file.bucketName, o.key())
            val description = "${listedFile.uri} (${o.size()} bytes, last modified ${o.lastModified()})"
            when (val reason = reasonToSkip(o)) {
                null -> {
                    logger.log("Will process $description")
                    listedFile
                }
                else -> {
                    logger.log("Skipping $description because $reason", WARN)
                    null
                }
            }
        }
    }

    fun writeBytes(bytes: ByteArray, destinationFile: S3File) = s3Client.putBytes(bytes, destinationFile.bucketName, destinationFile.key)

    fun deleteFile(file: S3File) = s3Client.deleteFile(file.bucketName, file.key)

    fun moveFile(sourceFile: S3File, destinationFile: S3File) {
        s3Client.copyFile(sourceFile.bucketName, sourceFile.key, destinationFile.bucketName, destinationFile.key)
        s3Client.deleteFile(sourceFile.bucketName, sourceFile.key)
    }

    private fun reasonToSkip(o: S3Object): String? = when {
        o.key().endsWith("/") -> "it is a folder marker"
        o.size() == 0L -> "it is empty"
        else -> null
    }
}
