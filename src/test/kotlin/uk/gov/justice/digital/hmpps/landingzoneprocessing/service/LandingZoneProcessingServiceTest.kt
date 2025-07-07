package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.jsonLambdaPayloadFromResources

class LandingZoneProcessingServiceTest {

    private val mockS3FileService = mock<S3FileService>()
    private val mockStepFunctionService = mock<StepFunctionService>()
    private val mockSingleFileProcessor = mock<SingleFileProcessor>()

    @Test
    fun `should process a single file`() {
        val underTest = LandingZoneProcessingService(mockS3FileService, mockStepFunctionService, mockSingleFileProcessor)
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/with-step-function-token.json")

        val fileToProcess = S3File("landing-processing-bucket-name", "any/number/of/prefixes")
        whenever(mockS3FileService.listFiles(any())).thenReturn(listOf(fileToProcess))

        underTest.process(payload)

        verify(mockSingleFileProcessor, times(1)).processFile(fileToProcess)
    }

    @Test
    fun `should process multiple files under a prefix`() {
        val underTest = LandingZoneProcessingService(mockS3FileService, mockStepFunctionService, mockSingleFileProcessor)
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/with-step-function-token.json")

        val fileToProcess1 = S3File("landing-processing-bucket-name", "any/number/of/prefixes/file1")
        val fileToProcess2 = S3File("landing-processing-bucket-name", "any/number/of/prefixes/file2")
        whenever(mockS3FileService.listFiles(any())).thenReturn(listOf(fileToProcess1, fileToProcess2))

        underTest.process(payload)

        verify(mockSingleFileProcessor, times(1)).processFile(fileToProcess1)
        verify(mockSingleFileProcessor, times(1)).processFile(fileToProcess2)
    }

    @Test
    fun `should list all files under the prefix configured in the payload`() {
        val underTest = LandingZoneProcessingService(mockS3FileService, mockStepFunctionService, mockSingleFileProcessor)
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/with-step-function-token.json")

        val expectedInputFilePrefix = S3File("landing-processing-bucket-name", "any/number/of/prefixes")
        val fileToProcess1 = S3File("landing-processing-bucket-name", "any/number/of/prefixes/file1")
        val fileToProcess2 = S3File("landing-processing-bucket-name", "any/number/of/prefixes/file2")

        whenever(mockS3FileService.listFiles(any())).thenReturn(listOf(fileToProcess1, fileToProcess2))

        underTest.process(payload)

        verify(mockS3FileService, times(1)).listFiles(expectedInputFilePrefix)
    }

    @Test
    fun `should notify the Step Function API of success if there is a step function token in the payload`() {
        val underTest = LandingZoneProcessingService(mockS3FileService, mockStepFunctionService, mockSingleFileProcessor)
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/with-step-function-token.json")

        val fileToProcess = S3File("landing-processing-bucket-name", "any/number/of/prefixes/file")

        whenever(mockS3FileService.listFiles(any())).thenReturn(listOf(fileToProcess))

        underTest.process(payload)

        verify(mockStepFunctionService, times(1)).notifySuccess(StepFunctionToken("some-token"))
    }

    @Test
    fun `should not notify the Step Function API of success if there is no step function token in the payload`() {
        val underTest = LandingZoneProcessingService(mockS3FileService, mockStepFunctionService, mockSingleFileProcessor)
        val payload = jsonLambdaPayloadFromResources("json-lambda-payloads/no-step-function-token.json")

        val fileToProcess = S3File("landing-processing-bucket-name", "any/number/of/prefixes/file")

        whenever(mockS3FileService.listFiles(any())).thenReturn(listOf(fileToProcess))

        underTest.process(payload)

        verify(mockStepFunctionService, times(0)).notifySuccess(StepFunctionToken("some-token"))
    }

}
