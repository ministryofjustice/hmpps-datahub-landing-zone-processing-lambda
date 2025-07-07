package uk.gov.justice.digital.hmpps.landingzoneprocessing

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsS3Client
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsStepFunctionsClient
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter
import uk.gov.justice.digital.hmpps.landingzoneprocessing.service.LandingZoneProcessingService
import uk.gov.justice.digital.hmpps.landingzoneprocessing.service.S3FileService
import uk.gov.justice.digital.hmpps.landingzoneprocessing.service.SingleFileProcessor
import uk.gov.justice.digital.hmpps.landingzoneprocessing.service.StepFunctionService

/**
 * Lambda, which reads CSV file(s) from S3 and writes the file(s) back to another S3 bucket in Parquet format.
 * The payload has the following format:
 *
 * {
 *    "stepFunctionToken": "some-token",
 *    "Records": [
 *       {
 *          "s3": {
 *             "bucket": {
 *                "name": "landing-processing-bucket-name"
 *             },
 *             "object": {
 *                "key": "any/number/of/prefixes/filename.csv"
 *             }
 *          }
 *       }
 *    ]
 * }
 * This is the format for S3 Bucket object notifications when an object is created, with the addition of a
 * stepFunctionToken field. The stepFunctionToken is optional - if it is provided, then the Step Functions API
 * will be notified of task success when the Lambda succeeds.
 * The s3 object key can be either a file or a prefix. For a file then that file will be processed, for a
 * non-file prefix then all files under that prefix will be processed.
 * This allows calling the lambda from either an AWS Step Function State Machine Task and waiting for the result or
 * from S3 object creation notifications.
 * The s3 object key can be URL encoded or not as required.
 */
class LandingZoneProcessingLambda : RequestHandler<MutableMap<String, Any>, String> {

    override fun handleRequest(
        payload: MutableMap<String, Any>?,
        context: Context?
    ): String? {
        requireNotNull(context) { "Context must not be null" }
        requireNotNull(payload) { "Payload must not be null" }

        AwsS3Client().use { s3Client ->
            AwsStepFunctionsClient().use { stepFunctionsClient ->

                val s3FileService = S3FileService(s3Client = s3Client)

                val landingZoneProcessingService = LandingZoneProcessingService(
                    s3FileService = s3FileService,
                    stepFunctionService = StepFunctionService(stepFunctionClient = stepFunctionsClient),
                    singleFileProcessor = SingleFileProcessor(
                        s3FileService = s3FileService,
                        csvToParquet = CsvToParquetBytesConverter(),
                        logger = context.logger,
                        env = Env
                    )
                )

                landingZoneProcessingService.process(payload)
            }
        }

        return "Completed execution."
    }



}