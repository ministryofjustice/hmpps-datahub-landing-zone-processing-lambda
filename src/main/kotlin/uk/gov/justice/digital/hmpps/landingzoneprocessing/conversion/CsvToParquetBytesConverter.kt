package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvRowToAvroRecordConverter.CsvRowToAvroConversionResult
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvRowToAvroRecordConverter.CsvRowToAvroConversionResult.*
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.FailedCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.CsvToParquetBytesConverter.CsvToParquetBytesConversionResult.SuccessfulCsvToParquetBytesConversion
import uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.avroparquet.ByteArrayOutputStreamOutputFile
import java.io.ByteArrayOutputStream
import java.time.Clock

/**
 * Converts in-memory CSV to Parquet
 */
class CsvToParquetBytesConverter(private val clock: Clock = Clock.systemUTC()) {

    /**
     * Represents the result of a CsvToParquetBytesConverter conversion toParquetByteArray
     */
    sealed class CsvToParquetBytesConversionResult {
        data class FailedCsvToParquetBytesConversion(val cause: CsvRowToAvroConversionResult): CsvToParquetBytesConversionResult()
        data class SuccessfulCsvToParquetBytesConversion(val bytes: ByteArray): CsvToParquetBytesConversionResult() {
            // By default, equals/hashcode do reference equality for arrays rather than contents equality
            override fun equals(other: Any?): Boolean {
                return other is SuccessfulCsvToParquetBytesConversion && bytes.contentEquals(other.bytes)
            }

            override fun hashCode(): Int {
                return bytes.contentHashCode()
            }
        }
    }

    /**
     * Converts the CSV rows to a ByteArray containing data that can be written as a Parquet file
     */
    fun toParquetByteArray(schema: Schema, csvRows: List<List<String>>): CsvToParquetBytesConversionResult {
        // We need to make some changes to the original schema to make it compatible with the processing we
        // are doing and the batch processing job which will pick up processing after this stage.
        val modifiedSchema = AvroSchemaConverter.convert(schema)
        val out = ByteArrayOutputStream() // out is in-memory and does not need closing - its close method is a no op
        val writer = AvroParquetWriter.builder<GenericRecord>(ByteArrayOutputStreamOutputFile(out))
            .withSchema(modifiedSchema)
            // Can set Parquet settings here if needed
            .withConf(PlainParquetConfiguration())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

        writer.use {
            for (row in csvRows) {
                val maybeRecord = CsvRowToAvroRecordConverter(clock).toAvro(modifiedSchema, row)
                when (maybeRecord) {
                    is SuccessfulConversion -> writer.write(maybeRecord.avro)
                    is DiscardedRow -> {} // no op - we don't write anything for a purposely discarded row
                    is TypeConversionFailure -> return FailedCsvToParquetBytesConversion(maybeRecord)
                    is UnsupportedTypeFailure -> return FailedCsvToParquetBytesConversion(maybeRecord)
                }
            }
        }
        return SuccessfulCsvToParquetBytesConversion(out.toByteArray())
    }
}
