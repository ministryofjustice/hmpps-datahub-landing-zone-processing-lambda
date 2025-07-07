package uk.gov.justice.digital.hmpps.landingzoneprocessing.conversion.avroparquet

import org.apache.parquet.io.PositionOutputStream
import java.io.ByteArrayOutputStream

/**
 * Used to treat a ByteArrayOutputStream as a PositionOutputStream, which is needed during conversion from avro
 * to in memory parquet.
 */
internal class ByteArrayPositionOutputStream(private val outputStream: ByteArrayOutputStream) : PositionOutputStream() {
    private var position = 0L

    override fun getPos(): Long {
        return position
    }

    override fun write(b: Int) {
        outputStream.write(b)
        position++
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        outputStream.write(b, off, len)
        position += len
    }
}