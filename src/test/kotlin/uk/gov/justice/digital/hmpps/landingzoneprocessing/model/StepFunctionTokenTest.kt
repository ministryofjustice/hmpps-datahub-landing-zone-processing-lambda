package uk.gov.justice.digital.hmpps.landingzoneprocessing.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken.Companion.maybeStepFunctionTokenFromLambdaPayload
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.jsonLambdaPayloadFromResources

class StepFunctionTokenTest {

    @Test
    fun `should create StepFunctionToken S3File from Lambda payload`() {
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/with-step-function-token.json")
        val actual = maybeStepFunctionTokenFromLambdaPayload(payload)
        val expected = StepFunctionToken("some-token")
        assertNotNull(actual)
        assertEquals(expected, actual)
    }

    @Test
    fun `should return null when there is no stepFunctionToken in the Lambda payload`() {
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/no-step-function-token.json")
        val actual = maybeStepFunctionTokenFromLambdaPayload(payload)
        assertNull(actual)
    }

}
