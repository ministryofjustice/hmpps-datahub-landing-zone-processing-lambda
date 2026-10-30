package uk.gov.justice.digital.hmpps.landingzoneprocessing.client

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object

class AwsS3ClientTest {

    private val mockS3 = mock<S3Client>()
    private val underTest = AwsS3Client(mockS3)

    @Test
    fun `should list files across every page`() {
        val page1 = ListObjectsV2Response.builder()
            .contents(S3Object.builder().key("prefix/file1").build())
            .isTruncated(true)
            .nextContinuationToken("token-for-page-2")
            .build()
        val page2 = ListObjectsV2Response.builder()
            .contents(S3Object.builder().key("prefix/file2").build())
            .isTruncated(false)
            .build()
        val requestCaptor = argumentCaptor<ListObjectsV2Request>()
        whenever(mockS3.listObjectsV2(requestCaptor.capture())).thenReturn(page1, page2)

        val result = underTest.listFiles("bucket", "prefix")

        assertEquals(listOf("prefix/file1", "prefix/file2"), result.map { it.key() })
        assertEquals(listOf(null, "token-for-page-2"), requestCaptor.allValues.map { it.continuationToken() })
    }
}
