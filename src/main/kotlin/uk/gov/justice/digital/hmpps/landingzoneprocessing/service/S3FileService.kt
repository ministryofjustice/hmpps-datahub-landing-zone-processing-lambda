package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import org.apache.avro.Schema
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsS3Client
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import java.nio.charset.Charset

/**
 * Interaction with S3
 */
class S3FileService(
    private val s3Client: AwsS3Client
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
        if (numberOfHeaderRowsToSkips  > 0) {
            require(rows.size >= numberOfHeaderRowsToSkips) { "Configured to skip $numberOfHeaderRowsToSkips rows but there are ony ${rows.size} rows" }
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

    fun listFiles(file: S3File): List<S3File> {
        val s3Objects = s3Client.listFiles(file.bucketName, file.key)
        return s3Objects.map { o -> S3File(file.bucketName, o.key()) }
    }

    fun writeBytes(bytes: ByteArray, destinationFile: S3File) = s3Client.putBytes(bytes, destinationFile.bucketName, destinationFile.key)

    fun deleteFile(file: S3File) = s3Client.deleteFile(file.bucketName, file.key)

    fun moveFile(sourceFile: S3File, destinationFile: S3File) {
        s3Client.copyFile(sourceFile.bucketName, sourceFile.key, destinationFile.bucketName, destinationFile.key)
        s3Client.deleteFile(sourceFile.bucketName, sourceFile.key)
    }
}