package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.apache.avro.Schema

/**
 * We need to convert all fields to nullable types, whether they are allowed to be null or not according to
 * the original schema. We want to allow nulls through at this stage of processing because Validation will handle
 * nulls as Violations later in the pipeline.
 * This means that a column should be converted from:
 * ```
 * {
 *    "name": "a_string_column",
 *    "type": "string"
 *  }
 * ```
 *
 * to:
 *
 * ```
 * {
 *   "name": "a_string_column",
 *   "type": ["null", "string"]
 * }
 * ```
 *
 *  We also need to add the columns the DMS usually adds to the schema:
 *  Op: String column as the 1st column
 *  _timestamp: String column as the 2nd column
 *  checkpoint_col: String column as the last column
 */
object AvroSchemaConverter {

    const val OP_COLUMN_NAME = "Op"
    const val TIMESTAMP_COLUMN_NAME = "_timestamp"
    const val CHECKPOINT_COLUMN_NAME = "checkpoint_col"

    fun convert(originalSchema: Schema): Schema {
        val newFields = addDMSColumns(originalSchema.fields).map(::makeNullable)

        val newSchema = Schema.createRecord(
            originalSchema.name,
            originalSchema.doc,
            originalSchema.namespace,
            originalSchema.isError,
            newFields
        )

        // Add the custom properties back in
        originalSchema.objectProps.forEach { k, v -> newSchema.addProp(k, v) }

        return newSchema

    }

    /**
     * Adds the columns added by the DMS in the correct positions in the schema.
     */
    private fun addDMSColumns(originalFields: List<Schema.Field>): List<Schema.Field> {
        return listOf(
            Schema.Field(OP_COLUMN_NAME, Schema.create(Schema.Type.STRING)),
            Schema.Field(TIMESTAMP_COLUMN_NAME, Schema.create(Schema.Type.STRING)),
        ) + originalFields + Schema.Field(CHECKPOINT_COLUMN_NAME, Schema.create(Schema.Type.STRING))
    }

    private fun makeNullable(field: Schema.Field): Schema.Field {
        val originalFieldSchema = field.schema()
        val newFieldSchema = if (
            originalFieldSchema.type == Schema.Type.UNION &&
            // types is only present if this is a union
            originalFieldSchema.types.any { it.type == Schema.Type.NULL }
        ) {
            // This field is already represented as a nullable union type, so use the field as is
            originalFieldSchema
        } else {
            Schema.createUnion(listOf(Schema.create(Schema.Type.NULL), originalFieldSchema))
        }

        return Schema.Field(field, newFieldSchema)
    }
}
