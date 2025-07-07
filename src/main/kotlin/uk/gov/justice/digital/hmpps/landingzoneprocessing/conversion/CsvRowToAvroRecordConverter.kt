package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.AvroSchemaConverter.CHECKPOINT_COLUMN_NAME
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.AvroSchemaConverter.OP_COLUMN_NAME
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.AvroSchemaConverter.TIMESTAMP_COLUMN_NAME
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvRowToAvroRecordConverter.CsvRowToAvroConversionResult.*
import java.lang.String.format
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

/**
 * Converts a CSV row to an Avro GenericRecord using the provided schema.
 */
class CsvRowToAvroRecordConverter(private val clock: Clock = Clock.systemUTC()) {

    companion object {
        // USed to indicate an insert by the DMS
        private const val INSERT_OP_CODE = "I"
    }


    /**
     * Represents the result of a CsvRowToAvroRecordConverter conversion toAvro
     */
    sealed class CsvRowToAvroConversionResult {
        data class SuccessfulConversion(val avro: GenericRecord) : CsvRowToAvroConversionResult()

        /**
         * When we can't convert a CSV field to the type required by the schema
         */
        data class TypeConversionFailure(val message: String, val cause: IllegalArgumentException) :
            CsvRowToAvroConversionResult()

        /**
         * When the schema contains an unsupported type
         */
        data class UnsupportedTypeFailure(val message: String) : CsvRowToAvroConversionResult()

        /**
         * When we discard a row, e.g. because it is completely empty
         */
        data object DiscardedRow : CsvRowToAvroConversionResult()
    }

    /**
     * Returns a CsvRowToAvroConversionResult, which is the row converted to an Avro GenericRecord in the success case.
     *
     * All types in the provided schema MUST be union types of null and a supported basic type.
     *
     * The schema must contain the additional DMS columns.
     */
    fun toAvro(schema: Schema, row: List<String>): CsvRowToAvroConversionResult {
        val fieldNames = schema.fields.map { f -> f.name() }

        // These columns should have been added by now
        require(fieldNames.contains(OP_COLUMN_NAME)) { "Schema is missing required field $OP_COLUMN_NAME" }
        require(fieldNames.contains(TIMESTAMP_COLUMN_NAME)) { "Schema is missing required field $TIMESTAMP_COLUMN_NAME" }
        require(fieldNames.contains(CHECKPOINT_COLUMN_NAME)) { "Schema is missing required field $CHECKPOINT_COLUMN_NAME" }

        if (row.all { it.isEmpty() }) {
            // If a row was completely empty before the DMS rows were added, then discard it.
            // This allows us to gracefully handle accidental empty rows at the end of a CSV file.
            return DiscardedRow
        }

        val rowWithDmsColumns = addDmsColumnsToRow(row)
        val resultRecord: GenericData.Record = GenericData.Record(schema)

        // Looping through the schema fields rather than the CSV columns means we will ignore and
        // discard any extraneous columns at the end of a CSV file, such as accidental extra empty columns
        for (fieldIndex in schema.fields.indices) {
            val field: Schema.Field = schema.fields[fieldIndex]
            val columnName = field.name()
            val stringVal = rowWithDmsColumns[fieldIndex]
            val type = field.schema().type
            // Our code should have enforced that all types are a union type before then Schema is passed to this method
            require(type == Schema.Type.UNION) { "Nullable union required at $type for field $columnName" }

            if (stringVal.isEmpty()) {
                // For non-string columns, this must be a null.
                // For string columns, we choose to set them to null rather than the empty string
                resultRecord.put(columnName, null)
            } else {
                // Special handling for union types.
                val possibleTypesInTheUnion = schema.fields[fieldIndex].schema().types
                val columnName = field.name()
                val type = field.schema().type
                // It only makes sense for CSV data to have a union where one type is null
                require(possibleTypesInTheUnion.size == 2) { "Too many possible types at $possibleTypesInTheUnion for field $columnName" }
                require(possibleTypesInTheUnion.any { it.type == Schema.Type.NULL }) { "No null type detected at $possibleTypesInTheUnion for field $columnName" }

                val actualType = possibleTypesInTheUnion.first { it.type != Schema.Type.NULL }.type
                try {
                    when (actualType) {
                        Schema.Type.STRING -> resultRecord.put(columnName, stringVal)
                        Schema.Type.INT -> resultRecord.put(columnName, stringVal.toInt())
                        Schema.Type.LONG -> resultRecord.put(columnName, stringVal.toLong())
                        Schema.Type.FLOAT -> resultRecord.put(columnName, stringVal.toFloat())
                        Schema.Type.DOUBLE -> resultRecord.put(columnName, stringVal.toDouble())
                        Schema.Type.BOOLEAN -> resultRecord.put(columnName, stringVal.toBooleanStrict())
                        else -> return UnsupportedTypeFailure("We do not support type $type for field $columnName")
                    }
                } catch (e: IllegalArgumentException) {
                    return TypeConversionFailure(
                        "$columnName could not be converted to the type in the schema. Exception message: ${e.message}",
                        e
                    )
                }
            }
        }
        return SuccessfulConversion(resultRecord)
    }

    private fun addDmsColumnsToRow(row: List<String>): List<String> {
        val timestamp = nowAsDmsFormatStringTimestamp()
        val checkpointColContents = ""
        return listOf(INSERT_OP_CODE, timestamp) + row + checkpointColContents
    }

    private fun nowAsDmsFormatStringTimestamp(): String {
        val now = Instant.now(clock)
        val dateTime = now.atZone(ZoneOffset.UTC).toLocalDateTime()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val formattedTimestampToSecondPrecision = dateTime.format(formatter)
        // DateTimeFormatter doesn't support microseconds properly with .SSSSSS pattern
        val microseconds = dateTime.get(ChronoField.MICRO_OF_SECOND)
        val timestamp = format("%s.%06d", formattedTimestampToSecondPrecision, microseconds)
        return timestamp
    }

}
