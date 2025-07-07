package uk.gov.justice.digital.hmpps.landingzoneprocessing.model

/**
 * Represents a step function token
 */
data class StepFunctionToken(val token: String) {
    companion object {
        /**
         * Returns a StepFunctionToken if there is one in the payload, otherwise null
         */
        fun maybeStepFunctionTokenFromLambdaPayload(payload: MutableMap<String, Any>): StepFunctionToken? =
            (payload["stepFunctionToken"] as String?)?.let(::StepFunctionToken)
    }
}
