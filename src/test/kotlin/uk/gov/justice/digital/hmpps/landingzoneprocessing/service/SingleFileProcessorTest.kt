package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.CHARSET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.LOG_CSV_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.OUTPUT_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.SCHEMA_REGISTRY_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.VIOLATIONS_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.VIOLATIONS_PATH_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvRowToAvroRecordConverter.CsvRowToAvroConversionResult.UnsupportedTypeFailure
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.FailedCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.SuccessfulCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.avroSchemaFromResources

class SingleFileProcessorTest {

    private val mockS3FileService = mock<S3FileService>()
    private val mockCsvToParquet = mock<CsvToParquetBytesConverter>()
    private val mockLogger = mock<LambdaLogger>()
    private val mockEnv = mock<Env>()

    private val byteArray = ByteArray(1)
    private val csvRows = listOf(
        listOf("1", "2", "3"),
        listOf("4", "5", "6"),
    )
    private val schema = avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/basicAvroSchema.avsc")
    private val sourceFile = S3File("bucket", "some/key")

    @Test
    fun `should retrieve CSV rows`() {
        setupEnvStubs()
        setupHappyPathStubs()

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        verify(mockS3FileService, times(1)).retrieveCsvRows(sourceFile, 0, Charsets.UTF_8)
    }

    @Test
    fun `should retrieve Avro schema`() {
        setupEnvStubs()
        setupHappyPathStubs()

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        val expectedSchemaFile = S3File("schema-reg-bucket", "some.avsc")
        verify(mockS3FileService, times(1)).retrieveSchema(expectedSchemaFile)
    }

    @Test
    fun `should convert the CSV to parquet`() {
        setupEnvStubs()
        setupHappyPathStubs()

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        verify(mockCsvToParquet, times(1)).toParquetByteArray(schema, csvRows)
    }

    @Test
    fun `should write bytes to the sink`() {
        setupEnvStubs()
        setupHappyPathStubs()

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        val expectedOutputFile = S3File("output-bucket", "some/LOAD-key.parquet")
        verify(mockS3FileService, times(1)).writeBytes(byteArray, expectedOutputFile)
    }

    @Test
    fun `should delete the original file`() {
        setupEnvStubs()
        setupHappyPathStubs()

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        verify(mockS3FileService, times(1)).deleteFile(sourceFile)
    }

    @Test
    fun `should log CSV rows`() {
        whenever(mockEnv.get(CHARSET_ENV_KEY)).thenReturn("UTF-8")
        whenever(mockEnv.get(NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY)).thenReturn("0")
        whenever(mockEnv.get(SCHEMA_REGISTRY_BUCKET_ENV_KEY)).thenReturn("schema-reg-bucket")
        whenever(mockEnv.get(OUTPUT_BUCKET_ENV_KEY)).thenReturn("output-bucket")
        whenever(mockEnv.get(LOG_CSV_ENV_KEY)).thenReturn("true")

        setupHappyPathStubs()

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        verify(mockLogger, times(1)).log("CSV file:\n\n1,2,3\n4,5,6\n")
    }

    @Test
    fun `should propagate exception when retrieving CSV rows`() {
        setupEnvStubs()

        whenever(mockS3FileService.retrieveCsvRows(any(), any(), any())).thenThrow(RuntimeException::class.java)

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        assertThrows(RuntimeException::class.java) {
            underTest.processFile(sourceFile)
        }
    }

    @Test
    fun `should propagate exception when retrieving Avro Schema`() {
        setupEnvStubs()

        whenever(mockS3FileService.retrieveCsvRows(any(), any(), any())).thenReturn(csvRows)
        whenever(mockS3FileService.retrieveSchema(any())).thenThrow(RuntimeException::class.java)

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        assertThrows(RuntimeException::class.java) {
            underTest.processFile(sourceFile)
        }
    }

    @Test
    fun `should propagate exception when converting to Parquet`() {
        setupEnvStubs()

        whenever(mockS3FileService.retrieveCsvRows(any(), any(), any())).thenReturn(csvRows)
        whenever(mockS3FileService.retrieveSchema(any())).thenReturn(schema)
        whenever(mockCsvToParquet.toParquetByteArray(any(), any())).thenThrow(RuntimeException::class.java)

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        assertThrows(RuntimeException::class.java) {
            underTest.processFile(sourceFile)
        }
    }

    @Test
    fun `should propagate exception when writing to sink`() {
        setupEnvStubs()
        setupHappyPathStubs()

        whenever(mockS3FileService.writeBytes(any(), any())).thenThrow(RuntimeException::class.java)

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        assertThrows(RuntimeException::class.java) {
            underTest.processFile(sourceFile)
        }
    }

    @Test
    fun `should propagate exception when deleting original file`() {
        setupEnvStubs()
        setupHappyPathStubs()

        whenever(mockS3FileService.deleteFile(any())).thenThrow(RuntimeException::class.java)

        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        assertThrows(RuntimeException::class.java) {
            underTest.processFile(sourceFile)
        }
    }

    @Test
    fun `should move input file to violations for failed conversion`() {
        setupEnvStubs()

        whenever(mockS3FileService.retrieveCsvRows(any(), any(), any())).thenReturn(csvRows)
        whenever(mockS3FileService.retrieveSchema(any())).thenReturn(schema)
        whenever(mockCsvToParquet.toParquetByteArray(any(), any())).thenReturn(
            FailedCsvToParquetBytesConversion(UnsupportedTypeFailure("some message"))
        )
        val underTest = SingleFileProcessor(mockS3FileService, mockCsvToParquet, mockLogger, mockEnv)
        underTest.processFile(sourceFile)

        val expectedViolationsFile = S3File("violations-bucket", "landing/some/key")
        verify(mockS3FileService, times(1)).moveFile(sourceFile, expectedViolationsFile)
    }

    private fun setupHappyPathStubs() {
        whenever(mockS3FileService.retrieveCsvRows(any(), any(), any())).thenReturn(csvRows)
        whenever(mockS3FileService.retrieveSchema(any())).thenReturn(schema)
        whenever(mockCsvToParquet.toParquetByteArray(any(), any())).thenReturn(
            SuccessfulCsvToParquetBytesConversion(
                byteArray
            )
        )
    }

    private fun setupEnvStubs() {
        whenever(mockEnv.get(CHARSET_ENV_KEY)).thenReturn("UTF-8")
        whenever(mockEnv.get(NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY)).thenReturn("0")
        whenever(mockEnv.get(SCHEMA_REGISTRY_BUCKET_ENV_KEY)).thenReturn("schema-reg-bucket")
        whenever(mockEnv.get(OUTPUT_BUCKET_ENV_KEY)).thenReturn("output-bucket")
        whenever(mockEnv.get(VIOLATIONS_BUCKET_ENV_KEY)).thenReturn("violations-bucket")
        whenever(mockEnv.get(VIOLATIONS_PATH_ENV_KEY)).thenReturn("landing")
        whenever(mockEnv.get(LOG_CSV_ENV_KEY)).thenReturn("false")
    }
}
