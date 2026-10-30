package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.mock
import org.mockito.kotlin.whenever
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsS3Client
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvRowToAvroRecordConverter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.SuccessfulCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.parquetBytesToAvro
import java.io.File

/**
 * Runs a CSV file from local disk through the lambda's reading and conversion, for checking a file that failed.
 * Set LOCAL_CSV_PATH to the file to run it, otherwise it's skipped. Set LOCAL_SCHEMA_PATH to a contract's Avro
 * schema to convert it with that contract too.
 */
class LocalCsvFileTest {

    private val localCsvPath: String? = System.getenv("LOCAL_CSV_PATH")
    private val localSchemaPath: String? = System.getenv("LOCAL_SCHEMA_PATH")
    private val numberOfHeaderRowsToSkip = 1

    private val mockS3 = mock<AwsS3Client>()
    private val logger = object : LambdaLogger {
        override fun log(message: String) = println(message)
        override fun log(message: ByteArray) = println(String(message))
    }
    private val s3FileService = S3FileService(mockS3, logger)
    private val s3File = S3File("bucket", "domain/table/file.csv")

    @Test
    fun `should read the local CSV file`() {
        val file = localCsvFile()
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(file.inputStream())

        val rows = s3FileService.retrieveCsvRows(s3File, numberOfHeaderRowsToSkip, Charsets.UTF_8)

        println("Read ${rows.size} data rows with ${rows.map { it.size }.distinct()} columns from ${file.name}")
        assertTrue(rows.isNotEmpty())
        assertEquals(1, rows.map { it.size }.distinct().size)
    }

    @Test
    fun `should convert the local CSV file to Parquet with an all-string schema`() {
        val file = localCsvFile()
        val header = file.inputStream().use { s3FileServiceReading(it) }.first()
        val schema = allStringSchema(header)
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(file.inputStream())
        val rows = s3FileService.retrieveCsvRows(s3File, numberOfHeaderRowsToSkip, Charsets.UTF_8)

        val result = CsvToParquetBytesConverter(CsvRowToAvroRecordConverter(logger)).toParquetByteArray(schema, rows)

        assertTrue(result is SuccessfulCsvToParquetBytesConversion, "Conversion failed: $result")
        val records = parquetBytesToAvro((result as SuccessfulCsvToParquetBytesConversion).bytes)
        println("Converted ${records.size} records. First record: ${records.firstOrNull()}")
        assertEquals(rows.size, records.size)
    }

    @Test
    fun `should convert the local CSV file to Parquet with a local contract`() {
        val file = localCsvFile()
        assumeTrue(localSchemaPath != null, "LOCAL_SCHEMA_PATH is not set")
        val schema = File(localSchemaPath!!).inputStream().use { Schema.Parser().parse(it) }
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(file.inputStream())
        val rows = s3FileService.retrieveCsvRows(s3File, numberOfHeaderRowsToSkip, Charsets.UTF_8)

        val result = CsvToParquetBytesConverter(CsvRowToAvroRecordConverter(logger)).toParquetByteArray(schema, rows)

        assertTrue(result is SuccessfulCsvToParquetBytesConversion, "Conversion failed: $result")
    }

    private fun localCsvFile(): File {
        assumeTrue(localCsvPath != null, "LOCAL_CSV_PATH is not set")
        return File(localCsvPath!!)
    }

    private fun s3FileServiceReading(input: java.io.InputStream): List<List<String>> {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(input)
        return s3FileService.retrieveCsvRows(s3File, 0, Charsets.UTF_8)
    }

    private fun allStringSchema(header: List<String>): Schema {
        val fields = SchemaBuilder.record("local_csv").fields()
        header.forEach { column ->
            val name = column.lowercase().replace(Regex("[^a-z0-9]+"), "_").trim('_')
            fields.name(name).type().nullable().stringType().noDefault()
        }
        return fields.endRecord()
    }
}
