package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.CHARSET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.LOG_CSV_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.OUTPUT_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.SCHEMA_REGISTRY_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.VIOLATIONS_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.VIOLATIONS_PATH_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.FailedCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.SuccessfulCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.avroSchemaFileFromInputFileKey
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.outputFileFromInputFileKey
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.violationsFileFromInputFileKey
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

/**
 * Encapsulates processing of a single file.
 */
class SingleFileProcessor(
    private val s3FileService: S3FileService,
    private val csvToParquet: CsvToParquetBytesConverter,
    private val logger: LambdaLogger,
    env: Env
) {

    private val charset = Charset.forName(env.get(CHARSET_ENV_KEY))
    private val numberOfHeaderRowsToSkip = env.get(NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY).toInt()
    private val outputBucketName = env.get(OUTPUT_BUCKET_ENV_KEY)
    private val schemaRegistryBucketName = env.get(SCHEMA_REGISTRY_BUCKET_ENV_KEY)
    private val violationsBucketName = env.get(VIOLATIONS_BUCKET_ENV_KEY)
    private val violationsBucketPath = env.get(VIOLATIONS_PATH_ENV_KEY)
    private val logCsv = env.get(LOG_CSV_ENV_KEY).toBooleanStrict()

    fun processFile(sourceFile: S3File) {
        logger.log("Retrieving CSV data")
        val csvRows = s3FileService.retrieveCsvRows(sourceFile, numberOfHeaderRowsToSkip, charset)

        if (logCsv) {
            logCsvRows(csvRows)
        }

        val schemaFile = avroSchemaFileFromInputFileKey(schemaRegistryBucketName, sourceFile.key)
        val outputFile = outputFileFromInputFileKey(outputBucketName, sourceFile.key)

        logger.log("Input file is ${sourceFile.bucketName}/${sourceFile.key}, charset is $charset and will skip $numberOfHeaderRowsToSkip rows")
        logger.log("Avro schema is ${schemaFile.bucketName}/${schemaFile.key}")
        logger.log("Output file will be ${outputFile.bucketName}/${outputFile.key}")

        logger.log("Retrieving avro schema")
        val avroSchema = s3FileService.retrieveSchema(schemaFile)
        logger.log("Converting CSV data to Parquet")
        val maybeParquetBytes = csvToParquet.toParquetByteArray(avroSchema, csvRows)

        when (maybeParquetBytes) {
            is SuccessfulCsvToParquetBytesConversion -> {
                logger.log("Writing Parquet data")
                s3FileService.writeBytes(maybeParquetBytes.bytes, outputFile)
                logger.log("Deleting source CSV file")
                s3FileService.deleteFile(sourceFile)
            }

            is FailedCsvToParquetBytesConversion -> {
                val violationsFile =
                    violationsFileFromInputFileKey(violationsBucketName, violationsBucketPath, sourceFile.key)
                logger.log("Violation: Moving source file $sourceFile to $violationsFile")
                s3FileService.moveFile(sourceFile, violationsFile)
            }
        }

    }

    private fun logCsvRows(csvRows: List<List<String>>) {
        val os = ByteArrayOutputStream()
        csvWriter {
            lineTerminator = "\n" // It is "\r\n" by default
        }.writeAll(csvRows, os)
        val csvContents = os.toString("UTF-8")
        logger.log("CSV file:\n\n$csvContents")
    }
}
