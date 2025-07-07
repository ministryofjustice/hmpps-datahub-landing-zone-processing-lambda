package uk.gov.justice.digital.hmpps.landingzoneprocessing.client

import software.amazon.awssdk.services.sfn.SfnClient
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest


class AwsStepFunctionsClient(private val sf: SfnClient = SfnClient.create()): AutoCloseable {

    /**
     * Notify Step Functions API of task success.
     *
     * @param token Step Function Token provided when the step function calls a Task
     * @param outputJson The JSON output of the task.
     */
    fun notifySuccess(token: String, outputJson: String) {
        val request = SendTaskSuccessRequest
            .builder()
            .taskToken(token)
            .output(outputJson)
            .build()

        sf.sendTaskSuccess(request)
    }

    override fun close() = sf.close()
}