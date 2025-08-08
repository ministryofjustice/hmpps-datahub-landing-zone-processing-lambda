package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvRowToAvroRecordConverter.CsvRowToAvroConversionResult.*
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.avroSchemaFromResources
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

class CsvRowToAvroRecordConverterTest {

    private val underTest = CsvRowToAvroRecordConverter(Clock.fixed(
        Instant.parse("2024-03-02T12:34:56.123456Z"),
        ZoneOffset.UTC
    ))

    private val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithEverySupportedUnionWithBasicDataType.avsc")

    @Test
    fun `should convert every supported basic type in a union`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals("str", actual.get("a_string_column") as String)
            assertEquals("str2", actual.get("a_string_column2") as String)
            assertEquals(1, actual.get("an_int_column") as Int)
            assertEquals(2, actual.get("an_int_column2") as Int)
            assertEquals(3L, actual.get("a_long_column") as Long)
            assertEquals(4L, actual.get("a_long_column2") as Long)
            assertEquals(5.0f, actual.get("a_float_column") as Float)
            assertEquals(6.0f, actual.get("a_float_column2") as Float)
            assertEquals(7.0, actual.get("a_double_column") as Double)
            assertEquals(8.0, actual.get("a_double_column2") as Double)
            assertEquals(true, actual.get("a_boolean_column") as Boolean)
            assertEquals(false, actual.get("a_boolean_column2") as Boolean)
        }
    }

    @Test
    fun `should convert logical timestamp type in a union`() {
        val schemaWithLogicalTimestamp = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithTimestampLogicalType.avsc")
        val row = listOf(
            "1234"
        )

        val result = underTest.toAvro(schemaWithLogicalTimestamp, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals(1234L, actual.get("my_timestamp") as Long)
        }
    }

    @Test
    fun `should map dms Op column to hardcoded 'I' representing insert`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals("I", actual.get("Op") as String)
        }
    }

    @Test
    fun `should map dms _timestamp column to a timestamp representing now in UTC`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals("2024-03-02 12:34:56.123456", actual.get("_timestamp") as String)
        }
    }

    @Test
    fun `should map dms _timestamp column to a timestamp representing now in UTC when in a non-UTC different timezone`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = CsvRowToAvroRecordConverter(Clock.fixed(
            Instant.parse("2024-03-02T12:34:56.123456Z"),
            ZoneId.of("Asia/Kolkata")
        )).toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals("2024-03-02 12:34:56.123456", actual.get("_timestamp") as String)
        }
    }

    @Test
    fun `should map dms checkpoint_col column to hardcoded null`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertNull(actual.get("checkpoint_col"))
        }
    }

    @Test
    fun `should convert when there are extra columns in the CSV that are not in the Avro schema by discarding the extra values`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false", "extra column"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals("str", actual.get("a_string_column") as String)
            assertEquals("str2", actual.get("a_string_column2") as String)
            assertEquals(1, actual.get("an_int_column") as Int)
            assertEquals(2, actual.get("an_int_column2") as Int)
            assertEquals(3L, actual.get("a_long_column") as Long)
            assertEquals(4L, actual.get("a_long_column2") as Long)
            assertEquals(5.0f, actual.get("a_float_column") as Float)
            assertEquals(6.0f, actual.get("a_float_column2") as Float)
            assertEquals(7.0, actual.get("a_double_column") as Double)
            assertEquals(8.0, actual.get("a_double_column2") as Double)
            assertEquals(true, actual.get("a_boolean_column") as Boolean)
            assertEquals(false, actual.get("a_boolean_column2") as Boolean)
        }
    }

    @Test
    fun `should convert float and double without a decimal point`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5", "6", "7", "8", "true", "false"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertEquals("str", actual.get("a_string_column") as String)
            assertEquals("str2", actual.get("a_string_column2") as String)
            assertEquals(1, actual.get("an_int_column") as Int)
            assertEquals(2, actual.get("an_int_column2") as Int)
            assertEquals(3L, actual.get("a_long_column") as Long)
            assertEquals(4L, actual.get("a_long_column2") as Long)
            assertEquals(5.0f, actual.get("a_float_column") as Float)
            assertEquals(6.0f, actual.get("a_float_column2") as Float)
            assertEquals(7.0, actual.get("a_double_column") as Double)
            assertEquals(8.0, actual.get("a_double_column2") as Double)
            assertEquals(true, actual.get("a_boolean_column") as Boolean)
            assertEquals(false, actual.get("a_boolean_column2") as Boolean)
        }
    }

    @Test
    fun `should return TypeConversionFailure for Int type mismatch`() {
        val row = listOf(
            "str", "str2", "not an int", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)
        assertTrue(result is TypeConversionFailure)
    }

    @Test
    fun `should return TypeConversionFailure for Long type mismatch`() {
        val row = listOf(
            "str", "str2", "1", "2", "not a long", "4", "5.0", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)
        assertTrue(result is TypeConversionFailure)
    }

    @Test
    fun `should return TypeConversionFailure for Float type mismatch`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "not a float", "6.0", "7.0", "8.0", "true", "false"
        )

        val result = underTest.toAvro(schema, row)
        assertTrue(result is TypeConversionFailure)
    }

    @Test
    fun `should return TypeConversionFailure for Double type mismatch`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "not a double", "true", "false"
        )

        val result = underTest.toAvro(schema, row)
        assertTrue(result is TypeConversionFailure)
    }

    @Test
    fun `should return TypeConversionFailure for Boolean type mismatch`() {
        val row = listOf(
            "str", "str2", "1", "2", "3", "4", "5.0", "6.0", "7.0", "8.0", "not a boolean", "false"
        )

        val result = underTest.toAvro(schema, row)
        assertTrue(result is TypeConversionFailure)
    }

    @Test
    fun `should convert empty string to null for every supported union type`() {
        val row = listOf(
            "1", "", "", "", "", "", "", "", "", "", "", ""
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is SuccessfulConversion)
        if (result is SuccessfulConversion) {
            val actual = result.avro
            assertNull(actual.get("a_string_column2"))
            assertNull(actual.get("an_int_column"))
            assertNull(actual.get("a_long_column"))
            assertNull(actual.get("a_float_column"))
            assertNull(actual.get("a_double_column"))
            assertNull(actual.get("a_boolean_column"))
        }
    }

    @Test
    fun `should discard a row with no contents`() {
        val row = listOf(
            "", "", "", "", "", "", "", "", "", "", "", ""
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is DiscardedRow)
    }

    @Test
    fun `should give unsupported type failure for unsupported type within avro union`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithUnsupportedUnionBytesType.avsc")
        val row = listOf(
            "1"
        )

        val result = underTest.toAvro(schema, row)

        assertTrue(result is UnsupportedTypeFailure)
    }

    @Test
    fun `should throw for schema with unsupported type outside of a union`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithUnsupportedRecordType.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema with non nullable types`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithNonNullableDataTypes.avsc")
        val row = listOf(
            "str", "str2", "1", "2", "1.0", "2.0", "true"
        )

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema with unsupported union type with more than 2 types`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithUnsupportedUnionTypeWithMoreThan2Types.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema with unsupported union type with less than 2 types`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithUnsupportedUnionTypeWithLessThan2Types.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema with unsupported union type with no null type`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaWithUnsupportedUnionTypeWithNoNullType.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema missing Op field`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaMissingOpColumn.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema missing _timestamp field`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaMissingTimestampColumn.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }

    @Test
    fun `should throw for schema missing checkpoint_col field`() {
        val schema = avroSchemaFromResources("avro-schemas/unit-tests/modified-schemas-with-dms-columns/avroSchemaMissingCheckpointColumn.avsc")

        val row = listOf("str")

        assertThrows(IllegalArgumentException::class.java) {
            underTest.toAvro(schema, row)
        }
    }
}
