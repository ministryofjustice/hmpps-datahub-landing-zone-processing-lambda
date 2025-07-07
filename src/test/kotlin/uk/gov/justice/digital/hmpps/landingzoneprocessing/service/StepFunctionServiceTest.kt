package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsStepFunctionsClient
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.StepFunctionToken

class StepFunctionServiceTest {

    @Test
    fun `notify success`() {
        val mockStepFunctionClient = mock<AwsStepFunctionsClient>()
        val underTest = StepFunctionService(mockStepFunctionClient)

        underTest.notifySuccess(StepFunctionToken("some-token"))

        verify(mockStepFunctionClient, times(1)).notifySuccess("some-token", "{}")
    }

}