package uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.Charset

object TestHelpers {

    /**
     * Reads a JSON file from the test resources directory into the Lambda Payload format
     */
    fun jsonLambdaPayloadFromResources(resourcePath: String): MutableMap<String, Any> {
        val ins = resourceFileToInputStream(resourcePath)
        val json = ins.bufferedReader().use { it.readText() }
        val mapper = jacksonObjectMapper()
        return mapper.readValue(json, object : TypeReference<MutableMap<String, Any>>() {})
    }

    /**
     * Convert a CSV String to an InputStream
     */
    fun toInputStream(csvData: String, charSet: Charset = Charsets.UTF_8): InputStream {
        return ByteArrayInputStream(csvData.toByteArray(charSet))
    }

    /**
     * Converts a file in test resources to an InputStream
     */
    fun resourceFileToInputStream(resourcePath: String): InputStream {
        return this::class.java.classLoader.getResourceAsStream(resourcePath) ?: throw IllegalArgumentException("File $resourcePath not found")
    }

    /**
     * Reads an Avro schema from the test resources directory into an in-memory avro Schema object
     */
    fun avroSchemaFromResources(resourcePath: String): Schema {
        val ins = resourceFileToInputStream(resourcePath)
        ins.use { i ->
            return Schema.Parser().parse(i)
        }
    }

    /**
     * Converts a ByteArray representing a Parquet file stored in-memory to a collection of Avro GenericRecord objects
     */
    fun parquetBytesToAvro(bytes: ByteArray): List<GenericRecord> {
        val reader: ParquetReader<GenericRecord?> = AvroParquetReader.builder<GenericRecord>(object : InputFile {
            override fun getLength(): Long = bytes.size.toLong()

            override fun newStream(): SeekableInputStream = ByteArraySeekableInputStream(bytes)

        }).build()

        val records = mutableListOf<GenericRecord>()
        reader.use { r ->
            while(true) {
                val record = r.read() ?: break
                records.add(record)
            }
        }
        return records
    }
}
