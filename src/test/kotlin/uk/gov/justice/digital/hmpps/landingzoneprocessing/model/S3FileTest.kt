package uk.gov.justice.digital.hmpps.landingzoneprocessing.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.avroSchemaFileFromInputFileKey
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.inputFileFromLambdaPayload
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.outputFileFromInputFileKey
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.violationsFileFromInputFileKey
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.jsonLambdaPayloadFromResources

class S3FileTest {

    @Test
    fun `should create input S3File from Lambda payload`() {
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/with-step-function-token.json")
        val actual = inputFileFromLambdaPayload(payload)
        val expected = S3File("landing-processing-bucket-name", "any/number/of/prefixes")
        assertEquals(expected, actual)
    }

    @Test
    fun `should throw for a missing bucket name in Lambda payload`() {
        val badPayload = jsonLambdaPayloadFromResources("json-lambda-payloads/missing-bucket-name.json")
        assertThrows(RuntimeException::class.java) { badPayload
            inputFileFromLambdaPayload(badPayload)
        }
    }

    @Test
    fun `should throw for a missing object key in Lambda payload`() {
        val badPayload = jsonLambdaPayloadFromResources("json-lambda-payloads/missing-object-key.json")
        assertThrows(RuntimeException::class.java) { badPayload
            inputFileFromLambdaPayload(badPayload)
        }
    }

    @Test
    fun `should create schema S3File from input file key`() {
        val actual = avroSchemaFileFromInputFileKey("schema-reg-bucket", "some/domain/table/file.csv")
        val expected = S3File("schema-reg-bucket", "some/domain/table.avsc")
        assertEquals(expected, actual)
    }

    @Test
    fun `should create output S3File from input file key`() {
        val actual = outputFileFromInputFileKey("output-bucket", "some/domain/table/file.csv")
        val expected = S3File("output-bucket", "some/domain/table/LOAD-file.csv.parquet")
        assertEquals(expected, actual)
    }

    @Test
    fun `should create violations S3File`() {
        val actual = violationsFileFromInputFileKey("violations-bucket", "some/path", "some/domain/table/file.csv")
        val expected = S3File("violations-bucket", "some/path/some/domain/table/file.csv")
        assertEquals(expected, actual)
    }

    @Test
    fun `should create violations S3File with slash on end of violations path`() {
        val actual = violationsFileFromInputFileKey("violations-bucket", "some/path/", "some/domain/table/file.csv")
        val expected = S3File("violations-bucket", "some/path/some/domain/table/file.csv")
        assertEquals(expected, actual)
    }
}
