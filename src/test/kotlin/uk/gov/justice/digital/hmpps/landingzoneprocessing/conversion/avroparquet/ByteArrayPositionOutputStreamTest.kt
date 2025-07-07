package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.avroparquet

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.io.ByteArrayOutputStream

class ByteArrayPositionOutputStreamTest {

    @Test
    fun `should initialise to position 0`() {
        val outputStream = ByteArrayOutputStream()
        val underTest = ByteArrayPositionOutputStream(outputStream)

        assertEquals(0L, underTest.pos)
    }

    @Test
    fun `should write a byte and update position`() {
        val outputStream = spy(ByteArrayOutputStream())
        val underTest = ByteArrayPositionOutputStream(outputStream)
        val byte = 1
        underTest.write(byte)

        assertEquals(1L, underTest.pos)
        verify(outputStream, times(1)).write(byte)
    }

    @Test
    fun `should write a byte array and update position`() {
        val outputStream = spy(ByteArrayOutputStream())
        val underTest = ByteArrayPositionOutputStream(outputStream)
        val bytes = byteArrayOf(1, 2, 3, 4, 5)
        underTest.write(bytes, 0, bytes.size)

        assertEquals(5L, underTest.pos)
        verify(outputStream, times(1)).write(bytes, 0, bytes.size)
    }

}