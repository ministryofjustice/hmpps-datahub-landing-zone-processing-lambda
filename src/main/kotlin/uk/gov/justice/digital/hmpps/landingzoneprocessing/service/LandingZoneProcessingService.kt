package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.inputFileFromLambdaPayload
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken.Companion.maybeStepFunctionTokenFromLambdaPayload

class LandingZoneProcessingService(
    private val s3FileService: S3FileService,
    private val stepFunctionService: StepFunctionService,
    private val singleFileProcessor: SingleFileProcessor
) {

    /**
     * Takes the Lambda payload and processes the files indicated by the payload.
     */
    fun process(payload: MutableMap<String, Any>) {
        val stepFunctionToken: StepFunctionToken? = maybeStepFunctionTokenFromLambdaPayload(payload)
        val inputFilePrefix: S3File = inputFileFromLambdaPayload(payload)
        // List all files below the input S3 prefix
        val allFiles = s3FileService.listFiles(inputFilePrefix)
        allFiles.forEach(singleFileProcessor::processFile)

        if (stepFunctionToken != null) {
            stepFunctionService.notifySuccess(stepFunctionToken)
        }
    }
}
