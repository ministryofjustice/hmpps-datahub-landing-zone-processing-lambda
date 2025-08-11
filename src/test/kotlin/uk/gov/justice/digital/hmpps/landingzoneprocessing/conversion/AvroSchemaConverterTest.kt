package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.apache.avro.Schema.Type.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers.TestHelpers.avroSchemaFromResources

class AvroSchemaConverterTest {

    private val inputSchema = avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithNullability.avsc")
    private val underTest = AvroSchemaConverter

    @Test
    fun `should add nullable Op as the 1st field in the schema`() {
        val actual = underTest.convert(inputSchema)

        assertEquals(7, actual.fields.size)
        val firstField = actual.fields[0]

        assertEquals("Op", firstField.name())
        assertEquals(UNION, firstField.schema().type)
        assertEquals(2, firstField.schema().types.size)
        assertEquals(NULL, firstField.schema().types[0].type)
        assertEquals(STRING, firstField.schema().types[1].type)
    }

    @Test
    fun `should add nullable _timestamp as the 2nd field in the schema`() {
        val actual = underTest.convert(inputSchema)

        assertEquals(7, actual.fields.size)
        val secondField = actual.fields[1]

        assertEquals("_timestamp", secondField.name())
        assertEquals(UNION, secondField.schema().type)
        assertEquals(2, secondField.schema().types.size)
        assertEquals(NULL, secondField.schema().types[0].type)
        assertEquals(STRING, secondField.schema().types[1].type)
    }

    @Test
    fun `should add nullable checkpoint_col as the last field in the schema`() {
        val actual = underTest.convert(inputSchema)

        assertEquals(7, actual.fields.size)
        val field7 = actual.fields[6]

        assertEquals("checkpoint_col", field7.name())
        assertEquals(UNION, field7.schema().type)
        assertEquals(2, field7.schema().types.size)
        assertEquals(NULL, field7.schema().types[0].type)
        assertEquals(STRING, field7.schema().types[1].type)
    }

    @Test
    fun `should convert fields with basic types to nullable union type`() {
        val actual = underTest.convert(inputSchema)

        assertEquals(7, actual.fields.size)
        // fields[0] and fields[1] are special DMS columns tested in a different test
        val field1 = actual.fields[2]
        val field4 = actual.fields[3]
        val field6 = actual.fields[5]

        assertEquals("string_pk", field1.name())
        assertEquals(UNION, field1.schema().type)
        assertEquals(2, field1.schema().types.size)
        assertEquals(NULL, field1.schema().types[0].type)
        assertEquals(STRING, field1.schema().types[1].type)

        assertEquals("our_nullable_column", field4.name())
        assertEquals(UNION, field4.schema().type)
        assertEquals(2, field4.schema().types.size)
        assertEquals(NULL, field4.schema().types[0].type)
        assertEquals(INT, field4.schema().types[1].type)

        assertEquals("non_nullable_column", field6.name())
        assertEquals(UNION, field6.schema().type)
        assertEquals(2, field6.schema().types.size)
        assertEquals(NULL, field6.schema().types[0].type)
        assertEquals(STRING, field6.schema().types[1].type)
    }

    @Test
    fun `should convert fields with logical types to nullable union type`() {
        val actual = underTest.convert(avroSchemaFromResources("avro-schemas/unit-tests/input-schemas-without-dms-columns/avroSchemaWithTimestampLogicalType.avsc"))

        assertEquals(4, actual.fields.size)
        val field1 = actual.fields[2]

        assertEquals("my_timestamp", field1.name())
        assertEquals(UNION, field1.schema().type)
        assertEquals(2, field1.schema().types.size)
        assertEquals(NULL, field1.schema().types[0].type)
        assertEquals(LONG, field1.schema().types[1].type)
        assertEquals("timestamp-micros", field1.schema().types[1].objectProps.getValue("logicalType"))
        assertFalse(field1.objectProps.getValue("nullable") as Boolean)
    }

    @Test
    fun `should preserve nullable union type for avro optional fields`() {
        val actual = underTest.convert(inputSchema)

        assertEquals(7, actual.fields.size)
        val field5 = actual.fields[4]

        assertEquals("avro_optional_column", field5.name())
        assertEquals(UNION, field5.schema().type)
        assertEquals(2, field5.schema().types.size)
        assertEquals(NULL, field5.schema().types[0].type)
        assertEquals(INT, field5.schema().types[1].type)
    }

    @Test
    fun `should preserve field level properties`() {
        val actual = underTest.convert(inputSchema)

        assertEquals(7, actual.fields.size)
        val field3 = actual.fields[2]
        val field4 = actual.fields[3]
        val field5 = actual.fields[4]
        val field6 = actual.fields[5]

        assertEquals("string_pk", field3.name())
        assertFalse(field3.objectProps.getValue("nullable") as Boolean)
        assertEquals("primary", field3.objectProps.getValue("key"))

        assertEquals("our_nullable_column", field4.name())
        assertTrue(field4.objectProps.getValue("nullable") as Boolean)

        assertEquals("avro_optional_column", field5.name())
        assertTrue(field5.objectProps.getValue("nullable") as Boolean)

        assertEquals("non_nullable_column", field6.name())
        assertFalse(field6.objectProps.getValue("nullable") as Boolean)
    }

    @Test
    fun `should preserve schema level properties`() {
        val actual = underTest.convert(inputSchema)

        assertEquals("somename", actual.name)
        assertEquals("prisonestate", actual.objectProps["service"])
        assertEquals("1.0.0", actual.objectProps["version"])
    }

}
