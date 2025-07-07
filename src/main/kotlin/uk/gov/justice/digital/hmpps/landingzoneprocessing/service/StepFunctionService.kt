package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsStepFunctionsClient
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken

class StepFunctionService(private val stepFunctionClient: AwsStepFunctionsClient) {

    fun notifySuccess(token: StepFunctionToken) {
        stepFunctionClient.notifySuccess(token.token, "{}")
    }
}