package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel.ERROR
import com.amazonaws.services.lambda.runtime.logging.LogLevel.WARN
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File.Companion.inputFileFromLambdaPayload
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken.Companion.maybeStepFunctionTokenFromLambdaPayload

class LandingZoneProcessingService(
    private val s3FileService: S3FileService,
    private val stepFunctionService: StepFunctionService,
    private val singleFileProcessor: SingleFileProcessor,
    private val logger: LambdaLogger
) {

    /**
     * Takes the Lambda payload and processes the files indicated by the payload.
     */
    fun process(payload: MutableMap<String, Any>) {
        val stepFunctionToken: StepFunctionToken? = maybeStepFunctionTokenFromLambdaPayload(payload)
        val inputFilePrefix: S3File = inputFileFromLambdaPayload(payload)
        logger.log("Processing files under ${inputFilePrefix.uri}. Invoked by a Step Function: ${stepFunctionToken != null}")
        // List all files below the input S3 prefix
        val allFiles = s3FileService.listFiles(inputFilePrefix)
        if (allFiles.isEmpty()) {
            logger.log("There are no files to process under ${inputFilePrefix.uri}", WARN)
        }
        allFiles.forEachIndexed { index, file ->
            logger.log("Processing file ${index + 1} of ${allFiles.size}: ${file.uri}")
            try {
                singleFileProcessor.processFile(file)
            } catch (e: Exception) {
                logger.log("Failed to process ${file.uri}: ${e.message}", ERROR)
                throw e
            }
        }
        logger.log("Finished processing ${allFiles.size} files under ${inputFilePrefix.uri}")

        if (stepFunctionToken != null) {
            stepFunctionService.notifySuccess(stepFunctionToken)
        }
    }
}
