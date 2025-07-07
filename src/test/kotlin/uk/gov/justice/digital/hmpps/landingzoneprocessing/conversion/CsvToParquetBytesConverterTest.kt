package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.FailedCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.SuccessfulCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.avroSchemaFromResources
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.parquetBytesToAvro
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class CsvToParquetBytesConverterTest {


    @Test
    fun `should convert multiple CSV rows to avro, including null conversions with non-nullable types in schema`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("str", "str2", "1", "2", "3", "4", "true"),
            listOf("another string", "", "", "", "", "", ""),
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNonNullableDataTypes.avsc"), csvRows)
        assertTrue(result is SuccessfulCsvToParquetBytesConversion)
        if (result is SuccessfulCsvToParquetBytesConversion) {
            // Convert back to Avro for assertions
            val resultAvro = parquetBytesToAvro(result.bytes)

            assertEquals(2, resultAvro.size)

            val row1Result = resultAvro[0]

            assertEquals("str", row1Result.get("a_string_column").toString())
            assertEquals("str2", row1Result.get("a_string_column2").toString())
            assertEquals(1, row1Result.get("an_int_column") as Int)
            assertEquals(2L, row1Result.get("a_long_column") as Long)
            assertEquals(3.0f, row1Result.get("a_float_column") as Float)
            assertEquals(4.0, row1Result.get("a_double_column") as Double)
            assertEquals(true, row1Result.get("a_boolean_column") as Boolean)

            val row2Result = resultAvro[1]

            assertEquals("another string", row2Result.get("a_string_column").toString())
            assertNull(row2Result.get("a_string_column2"))
            assertNull(row2Result.get("an_int_column"))
            assertNull(row2Result.get("a_long_column"))
            assertNull(row2Result.get("a_float_column"))
            assertNull(row2Result.get("a_double_column"))
            assertNull(row2Result.get("a_boolean_column"))
        }
    }

    @Test
    fun `should convert multiple CSV rows to avro with nullable union types in schema`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"),
            listOf("another string", "", "", "", "", "", "", "", "", "", "", ""),
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithEverySupportedUnionDataType.avsc"), csvRows)
        assertTrue(result is SuccessfulCsvToParquetBytesConversion)
        if (result is SuccessfulCsvToParquetBytesConversion) {
            // Convert back to Avro for assertions
            val resultAvro = parquetBytesToAvro(result.bytes)

            assertEquals(2, resultAvro.size)

            val row1Result = resultAvro[0]

            assertEquals("str", row1Result.get("a_string_column").toString())
            assertEquals("str2", row1Result.get("a_string_column2").toString())
            assertEquals(1, row1Result.get("an_int_column") as Int)
            assertEquals(2, row1Result.get("an_int_column2") as Int)
            assertEquals(3L, row1Result.get("a_long_column") as Long)
            assertEquals(4L, row1Result.get("a_long_column2") as Long)
            assertEquals(5.0f, row1Result.get("a_float_column") as Float)
            assertEquals(6.0f, row1Result.get("a_float_column2") as Float)
            assertEquals(7.0, row1Result.get("a_double_column") as Double)
            assertEquals(8.0, row1Result.get("a_double_column2") as Double)
            assertEquals(true, row1Result.get("a_boolean_column") as Boolean)
            assertEquals(false, row1Result.get("a_boolean_column2") as Boolean)

            val row2Result = resultAvro[1]

            assertEquals("another string", row2Result.get("a_string_column").toString())
            assertNull(row2Result.get("a_string_column2"))
            assertNull(row2Result.get("an_int_column"))
            assertNull(row2Result.get("an_int_column2"))
            assertNull(row2Result.get("a_long_column"))
            assertNull(row2Result.get("a_long_column2"))
            assertNull(row2Result.get("a_float_column"))
            assertNull(row2Result.get("a_float_column2"))
            assertNull(row2Result.get("a_double_column"))
            assertNull(row2Result.get("a_double_column2"))
            assertNull(row2Result.get("a_boolean_column"))
            assertNull(row2Result.get("a_boolean_column2"))
        }
    }

    @Test
    fun `should include additional DMS Op column hardcoded to Insert`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("str", "str2", "1", "2", "3", "4", "true")
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNonNullableDataTypes.avsc"), csvRows)
        assertTrue(result is SuccessfulCsvToParquetBytesConversion)
        if (result is SuccessfulCsvToParquetBytesConversion) {
            // Convert back to Avro for assertions
            val resultAvro = parquetBytesToAvro(result.bytes)

            assertEquals(1, resultAvro.size)

            val row1Result = resultAvro[0]

            assertEquals("I", row1Result.get("Op").toString())
        }
    }

    @Test
    fun `should include additional DMS _timestamp column with current timestamp`() {
        val stubClock = Clock.fixed(
            Instant.parse("2021-02-01T12:34:56.123456Z"),
            ZoneOffset.UTC
        )
        val underTest = CsvToParquetBytesConverter(clock = stubClock)

        val csvRows = listOf(
            listOf("str", "str2", "1", "2", "3", "4", "true")
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNonNullableDataTypes.avsc"), csvRows)
        assertTrue(result is SuccessfulCsvToParquetBytesConversion)
        if (result is SuccessfulCsvToParquetBytesConversion) {
            // Convert back to Avro for assertions
            val resultAvro = parquetBytesToAvro(result.bytes)

            assertEquals(1, resultAvro.size)

            val row1Result = resultAvro[0]
            assertEquals("2021-02-01 12:34:56.123456", row1Result.get("_timestamp").toString())
        }
    }

    @Test
    fun `should include additional DMS checkpoint_col column hardcoded to null`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("str", "str2", "1", "2", "3", "4", "true")
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNonNullableDataTypes.avsc"), csvRows)
        assertTrue(result is SuccessfulCsvToParquetBytesConversion)
        if (result is SuccessfulCsvToParquetBytesConversion) {
            // Convert back to Avro for assertions
            val resultAvro = parquetBytesToAvro(result.bytes)

            assertEquals(1, resultAvro.size)

            val row1Result = resultAvro[0]

            assertNull(row1Result.get("checkpoint_col"))
        }
    }

    @Test
    fun `should discard accidental empty rows and columns`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("str", "str2", "1", "2", "3", "4", "false", "extra column"),
            listOf("", "", "", "", "", "", ""),
            listOf("", "", "", "", "", "", ""),
            listOf("", "", "", "", "", "", "", ""),
        )

        val result = underTest.toParquetByteArray(
            avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNonNullableDataTypes.avsc"),
            csvRows
        )
        assertTrue(result is SuccessfulCsvToParquetBytesConversion)
        if (result is SuccessfulCsvToParquetBytesConversion) {
            // Convert back to Avro for assertions
            val resultAvro = parquetBytesToAvro(result.bytes)

            assertEquals(1, resultAvro.size)

            val row1Result = resultAvro[0]

            assertEquals("str", row1Result.get("a_string_column").toString())
            assertEquals("str2", row1Result.get("a_string_column2").toString())
            assertEquals(1, row1Result.get("an_int_column") as Int)
            assertEquals(2L, row1Result.get("a_long_column") as Long)
            assertEquals(3.0f, row1Result.get("a_float_column") as Float)
            assertEquals(4.0, row1Result.get("a_double_column") as Double)
            assertEquals(false, row1Result.get("a_boolean_column") as Boolean)
        }
    }

    @Test
    fun `should report failure for type mismatch`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("str", "str2", "shouldbeint", "2", "3", "4", "true")
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNonNullableDataTypes.avsc"), csvRows)
        assertTrue(result is FailedCsvToParquetBytesConversion)
    }

    @Test
    fun `should report failure for unsupported type within avro union`() {
        val underTest = CsvToParquetBytesConverter()

        val csvRows = listOf(
            listOf("1")
        )

        val result = underTest.toParquetByteArray(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithUnsupportedUnionTypeWithUnsupportedBytesType.avsc"), csvRows)
        assertTrue(result is FailedCsvToParquetBytesConversion)
    }
}
