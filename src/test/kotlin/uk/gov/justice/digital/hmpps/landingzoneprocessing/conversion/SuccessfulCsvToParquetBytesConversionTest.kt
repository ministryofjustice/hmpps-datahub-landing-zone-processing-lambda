package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.SuccessfulCsvToParquetBytesConversion

class SuccessfulCsvToParquetBytesConversionTest {

    @Test
    fun `equals should give equals for same object`() {
        val underTest = SuccessfulCsvToParquetBytesConversion(byteArrayOf(1, 2, 3))
        assertEquals(underTest, underTest)
    }

    @Test
    fun `equals should give equals for different object with same underlying object`() {
        val bytes = byteArrayOf(1, 2, 3)
        val underTest1 = SuccessfulCsvToParquetBytesConversion(bytes)
        val underTest2 = SuccessfulCsvToParquetBytesConversion(bytes)
        assertEquals(underTest1, underTest2)
    }

    @Test
    fun `equals should give equals for different underlying objects with the same elements`() {
        val bytes1 = byteArrayOf(1, 2, 3)
        val bytes2 = byteArrayOf(1, 2, 3)
        val underTest1 = SuccessfulCsvToParquetBytesConversion(bytes1)
        val underTest2 = SuccessfulCsvToParquetBytesConversion(bytes2)
        assertEquals(underTest1, underTest2)
    }

    @Test
    fun `equals should give not equals for underlying objects with different elements`() {
        val bytes1 = byteArrayOf(1, 2, 3)
        val bytes2 = byteArrayOf(1, 2, 3, 4)
        val underTest1 = SuccessfulCsvToParquetBytesConversion(bytes1)
        val underTest2 = SuccessfulCsvToParquetBytesConversion(bytes2)
        assertNotEquals(underTest1, underTest2)
    }

    @Test
    fun `equals should give not equals for null`() {
        val bytes = byteArrayOf(1, 2, 3)
        val underTest = SuccessfulCsvToParquetBytesConversion(bytes)
        assertNotEquals(underTest, null)
    }

    @Test
    fun `hash code should be the same as the underlying bytes content hash code`() {
        val bytes = byteArrayOf(1, 2, 3)
        val underTest = SuccessfulCsvToParquetBytesConversion(bytes)
        assertEquals(bytes.contentHashCode(), underTest.hashCode())
    }
}
