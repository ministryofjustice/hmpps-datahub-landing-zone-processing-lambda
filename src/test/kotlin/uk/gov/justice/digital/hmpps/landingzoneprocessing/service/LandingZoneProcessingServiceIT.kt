package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.*
import software.amazon.awssdk.services.s3.model.S3Object
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.CHARSET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.LOG_CSV_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.OUTPUT_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.SCHEMA_REGISTRY_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.VIOLATIONS_BUCKET_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.Env.VIOLATIONS_PATH_ENV_KEY
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsS3Client
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.jsonLambdaPayloadFromResources
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.parquetBytesToAvro

class LandingZoneProcessingServiceIT {

    companion object {
        private const val PRISONS_CSV = "/csv/prisons/Prison Estate(Prisons).csv"
        private const val PRISONS_BUCKET_KEY = "prisonestate/prisons/Prison Estate(Prisons).csv"
        private const val PRISONS_SCHEMA = "/avro-schemas/prisonestate/prisons.avsc"
        private const val PRISONS_SCHEMA_KEY = "prisonestate/prisons.avsc"
        private const val INPUT_BUCKET = "landing-processing-bucket-name"
        private const val SCHEMA_REG_BUCKET = "schema-reg-bucket"
        private const val OUTPUT_BUCKET = "output-bucket"
        private const val OUTPUT_BUCKET_EXPECTED_KEY = "prisonestate/prisons/LOAD-Prison Estate(Prisons).csv.parquet"
    }

    private val mockS3Client = mock<AwsS3Client>()
    private val mockStepFunctionService = mock<StepFunctionService>()
    private val mockLogger = mock<LambdaLogger>()
    private val mockEnv = mock<Env>()

    @Test
    fun `should process Prisons data from CSV`() {
        // Stub retrieving environment variables
        whenever(mockEnv.get(CHARSET_ENV_KEY)).thenReturn("UTF-8")
        whenever(mockEnv.get(NUMBER_OF_HEADER_ROWS_TO_SKIP_ENV_KEY)).thenReturn("1")
        whenever(mockEnv.get(SCHEMA_REGISTRY_BUCKET_ENV_KEY)).thenReturn(SCHEMA_REG_BUCKET)
        whenever(mockEnv.get(OUTPUT_BUCKET_ENV_KEY)).thenReturn(OUTPUT_BUCKET)
        whenever(mockEnv.get(VIOLATIONS_BUCKET_ENV_KEY)).thenReturn("violations-bucket")
        whenever(mockEnv.get(VIOLATIONS_PATH_ENV_KEY)).thenReturn("landing")
        whenever(mockEnv.get(LOG_CSV_ENV_KEY)).thenReturn("false")

        // Stub listing the files
        whenever(mockS3Client.listFiles(INPUT_BUCKET, PRISONS_BUCKET_KEY)).thenReturn(listOf(S3Object.builder().key(PRISONS_BUCKET_KEY).build()))
        // Stub retrieving the CSV rows
        whenever(mockS3Client.toInputStream(INPUT_BUCKET, PRISONS_BUCKET_KEY)).thenReturn(javaClass.getResourceAsStream(PRISONS_CSV))
        // Stub retrieving the Avro schema
        whenever(mockS3Client.toInputStream(SCHEMA_REG_BUCKET, PRISONS_SCHEMA_KEY)).thenReturn(javaClass.getResourceAsStream(PRISONS_SCHEMA))

        val s3FileService = S3FileService(mockS3Client)
        val underTest = LandingZoneProcessingService(
            s3FileService,
            mockStepFunctionService,
            SingleFileProcessor(
                s3FileService,
                CsvToParquetBytesConverter(),
                mockLogger,
                mockEnv
            )
        )

        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/integration-test.json")
        underTest.process(payload)

        val captor = argumentCaptor<ByteArray>()
        verify(mockS3Client, times(1)).putBytes(captor.capture(), eq(OUTPUT_BUCKET), eq(OUTPUT_BUCKET_EXPECTED_KEY))

        val avroRecords = parquetBytesToAvro(captor.firstValue)
        assertEquals(3, avroRecords.size)

        val prison1 = avroRecords.find { r -> r.get("prison_id") as Int == 1 }
            ?: throw AssertionError("Could not find prison_id 1")

        assertEquals("ABC", prison1.get("prison_code").toString())
        assertEquals("Prison1", prison1.get("prison").toString())
        assertEquals(123, prison1.get("operational_capacity") as Int)
        assertNull(prison1.get("staff_headcount"))

        val prison2 = avroRecords.find { r -> r.get("prison_id") as Int == 2 }
            ?: throw AssertionError("Could not find prison_id 1")

        assertEquals("DEF", prison2.get("prison_code").toString())
        assertEquals("Prison2", prison2.get("prison").toString())
        assertEquals(456, prison2.get("operational_capacity") as Int)
        assertNull(prison2.get("staff_headcount"))

        val prison3 = avroRecords.find { r -> r.get("prison_id") as Int == 3 }
            ?: throw AssertionError("Could not find prison_id 1")

        assertEquals("GHI", prison3.get("prison_code").toString())
        assertEquals("Prison3", prison3.get("prison").toString())
        assertEquals(789, prison3.get("operational_capacity") as Int)
        assertEquals(76, prison3.get("staff_headcount"))
    }
}
