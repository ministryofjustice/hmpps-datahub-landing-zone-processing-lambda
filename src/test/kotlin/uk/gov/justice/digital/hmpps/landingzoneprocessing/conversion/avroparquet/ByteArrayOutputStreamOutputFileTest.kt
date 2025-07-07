package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.avroparquet

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.ByteArrayOutputStream

class ByteArrayOutputStreamOutputFileTest {
    private val outputStream = Mockito.mock<ByteArrayOutputStream>()

    @Test
    fun `should create a ByteArrayPositionOutputStream`() {
        val underTest = ByteArrayOutputStreamOutputFile(outputStream)
        underTest.create(0L).use { pos ->
            assertNotNull(pos)
            assertInstanceOf(ByteArrayPositionOutputStream::class.java, pos)
        }
    }

    @Test
    fun `should not support block size`() {
        val underTest = ByteArrayOutputStreamOutputFile(outputStream)
        assertFalse(underTest.supportsBlockSize())
    }

    @Test
    fun `default block size is 0`() {
        val underTest = ByteArrayOutputStreamOutputFile(outputStream)
        assertEquals(0L, underTest.defaultBlockSize())
    }
}