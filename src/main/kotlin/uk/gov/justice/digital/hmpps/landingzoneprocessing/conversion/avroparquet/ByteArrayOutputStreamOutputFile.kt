package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.avroparquet

import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.io.ByteArrayOutputStream

/**
 * Maps between a Parquet OutputFile class and a java ByteArrayOutputStream, so we can write direct to memory
 * rather than to an actual File.
 */
class ByteArrayOutputStreamOutputFile (private val outputStream: ByteArrayOutputStream): OutputFile {
    override fun create(blockSizeHint: Long): PositionOutputStream {
        return ByteArrayPositionOutputStream(outputStream)
    }

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
        return create(blockSizeHint)
    }

    override fun supportsBlockSize(): Boolean = false

    override fun defaultBlockSize(): Long = 0L
}