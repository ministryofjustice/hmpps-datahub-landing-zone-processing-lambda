package uk.gov.justice.digital.hmpps.landingzoneprocessing.service

import com.github.doyaaaaaken.kotlincsv.util.CSVFieldNumDifferentException
import org.apache.avro.Schema.Type.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.s3.model.S3Object
import uk.gov.justice.digital.hmpps.landingzoneprocessing.client.AwsS3Client
import uk.gov.justice.digital.hmpps.landingzoneprocessing.model.S3File
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvDifferentNumberOfColumnsInDifferentRows
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvNoHeader
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvWithHeader
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvWithMissingValues
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvWithSpecialCharacters
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvWithTrailingEmptyColumns
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestDataCsv.csvWithTrailingEmptyRows
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.resourceFileToInputStream
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.toInputStream

class S3FileServiceTest {

    private val csvFile = S3File("bucket1", "path/to/csvFile")
    private val schemaFile = S3File("bucket2", "path/to/schema")

    private val mockS3 = mock<AwsS3Client>()
    private val underTest = S3FileService(mockS3)

    @Test
    fun `should read CSV from S3`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvNoHeader))

        underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)

        verify(mockS3, times(1)).toInputStream("bucket1", "path/to/csvFile")
    }

    @Test
    fun `should read a CSV with no header correctly`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvNoHeader))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)
        val expected = listOf(
            listOf("a", "1", "2", "3.0"),
            listOf("b", "2", "3", "4.1"),
            listOf("c", "3", "4", "5.2"),
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should read a CSV with a header correctly`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvWithHeader))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 1, charSet = Charsets.UTF_8)
        val expected = listOf(
            listOf("a", "1", "2", "3.0"),
            listOf("b", "2", "3", "4.1"),
            listOf("c", "3", "4", "5.2"),
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should populate empty string for missing values when reading a CSV file`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvWithMissingValues))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)
        val expected = listOf(
            listOf("a", "1", "", "3.0"),
            listOf("b", "", "3", "4.1"),
            listOf("c", "", "", ""),
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should throw for a CSV with different numbers of columns in different rows`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvDifferentNumberOfColumnsInDifferentRows))

        assertThrows(CSVFieldNumDifferentException::class.java) {
            underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)
        }
    }

    @Test
    fun `should populate accidental trailing rows with empty string when reading a CSV file`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvWithTrailingEmptyRows))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)
        val expected = listOf(
            listOf("a", "1", "2", "3.0"),
            listOf("b", "2", "3", "4.1"),
            listOf("c", "3", "4", "5.2"),
            listOf("", "", "", ""),
            listOf("", "", "", ""),
            listOf("", "", "", "")
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should populate accidental trailing columns with empty string when reading a CSV file`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvWithTrailingEmptyColumns))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)
        val expected = listOf(
            listOf("a", "1", "2", "3.0", "", "", "", "", ""),
            listOf("b", "2", "3", "4.1", "", "", "", "", ""),
            listOf("c", "3", "4", "5.2", "", "", "", "", "")
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should translate special characters when reading a CSV file`() {
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvWithSpecialCharacters))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = Charsets.UTF_8)
        val expected = listOf(
            listOf("a", "©"),
            listOf("b", "ü"),
            listOf("c", "ç")
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should translate non-UTF-8 charsets with special characters when reading a CSV file`() {
        val characterSet = Charsets.ISO_8859_1
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(toInputStream(csvWithSpecialCharacters, charSet = characterSet))

        val actual = underTest.retrieveCsvRows(csvFile, numberOfHeaderRowsToSkips = 0, charSet = characterSet)
        val expected = listOf(
            listOf("a", "©"),
            listOf("b", "ü"),
            listOf("c", "ç")
        )

        assertEquals(expected, actual)
    }

    @Test
    fun `should read schema from S3`() {
        val avroSchemaInputStream = resourceFileToInputStream("avro-schemas/unit-tests/input-schemas-without-dms-columns/basicAvroSchema.avsc")
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(avroSchemaInputStream)

        underTest.retrieveSchema(schemaFile)

        verify(mockS3, times(1)).toInputStream("bucket2", "path/to/schema")
    }

    @Test
    fun `should read the schema from S3 into memory correctly`() {
        val avroSchemaInputStream = resourceFileToInputStream("avro-schemas/unit-tests/input-schemas-without-dms-columns/basicAvroSchema.avsc")
        whenever(mockS3.toInputStream(anyString(), anyString())).thenReturn(avroSchemaInputStream)

        val actualFields = underTest.retrieveSchema(schemaFile).fields

        assertEquals(4, actualFields.size)
        val field1 = actualFields[0]
        val field2 = actualFields[1]
        val field3 = actualFields[2]
        val field4 = actualFields[3]

        assertEquals("a_string_column", field1.name())
        assertEquals(STRING, field1.schema().type)
        assertEquals("an_int_column", field2.name())
        assertEquals(INT, field2.schema().type)
        assertEquals("an_int_column2", field3.name())
        assertEquals(INT, field3.schema().type)
        assertEquals("a_double_column", field4.name())
        assertEquals(DOUBLE, field4.schema().type)
    }

    @Test
    fun `should list files from S3`() {
        val s3Files = listOf(
            S3Object.builder().key("k1").build(),
            S3Object.builder().key("k2").build()
        )
        whenever(mockS3.listFiles(anyString(), anyString())).thenReturn(s3Files)

        underTest.listFiles(csvFile)

        verify(mockS3, times(1)).listFiles("bucket1", "path/to/csvFile")
    }

    @Test
    fun `should map files listed from S3 to S3File`() {
        val s3Files = listOf(
            S3Object.builder().key("k1").build(),
            S3Object.builder().key("k2").build()
        )
        whenever(mockS3.listFiles(anyString(), anyString())).thenReturn(s3Files)

        val result = underTest.listFiles(csvFile)

        val expected = listOf(
            S3File("bucket1", "k1"),
            S3File("bucket1", "k2")
        )
        assertEquals(expected, result)
    }

    @Test
    fun `should write bytes to S3`() {
        val bytes = byteArrayOf(1, 2, 3)

        underTest.writeBytes(bytes, csvFile)

        verify(mockS3, times(1)).putBytes(bytes, "bucket1", "path/to/csvFile")
    }

    @Test
    fun `should delete S3 file`() {
        underTest.deleteFile(csvFile)

        verify(mockS3, times(1)).deleteFile("bucket1", "path/to/csvFile")
    }

    @Test
    fun `should copy and delete S3 file when moving file`() {
        underTest.moveFile(csvFile, schemaFile)

        verify(mockS3, times(1)).copyFile("bucket1", "path/to/csvFile", "bucket2", "path/to/schema")
        verify(mockS3, times(1)).deleteFile("bucket1", "path/to/csvFile")
    }

}
