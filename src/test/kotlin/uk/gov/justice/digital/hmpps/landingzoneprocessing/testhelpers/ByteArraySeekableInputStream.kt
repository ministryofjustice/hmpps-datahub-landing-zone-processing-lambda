package uk.gov.justice.digital.hmpps.landingzoneprocessing.testhelpers

import org.apache.parquet.io.SeekableInputStream
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.nio.ByteBuffer

/**
 * SeekableInputStream wrapping a ByteArray for use in tests when converting Parquet bytes to Avro for test assertions
 */
class ByteArraySeekableInputStream(bytes: ByteArray) : SeekableInputStream() {

    private var pos = 0L
    private val bais = ByteArrayInputStream(bytes)

    override fun getPos(): Long {
        return pos
    }

    override fun seek(newPos: Long) {
        bais.reset()
        bais.skip(newPos)
        pos = newPos
    }

    override fun readFully(bytes: ByteArray) {
        if (bais.available() < bytes.size) {
            throw EOFException("Not enough bytes to fill an array of ${bytes.size} bytes")
        }
        bais.read(bytes)
    }

    override fun readFully(bytes: ByteArray, start: Int, len: Int) {
        if (bais.available() < len) {
            throw EOFException("Not enough bytes to read $len bytes")
        }
        bais.read(bytes, start, len)
    }

    override fun read(buf: ByteBuffer): Int {
        // Fill the buffer from the bytearray, or fill it as far as possible if there aren't enough bytes left
        val remainingBytesInBuffer = buf.remaining()
        val bytesToRead = ByteArray(remainingBytesInBuffer)
        val numBytesRead = bais.read(bytesToRead)
        val reachedEnd = numBytesRead == -1
        if (!reachedEnd) {
            pos += numBytesRead.toLong()
        }
        buf.put(bytesToRead)
        return numBytesRead
    }

    override fun readFully(buf: ByteBuffer) {
        // Fill the buffer from the bytearray, or fill it as far as possible if there aren't enough bytes left
        val remainingBytesInBuffer = buf.remaining()
        val bytesToRead = ByteArray(remainingBytesInBuffer)
        val numBytesRead = bais.read(bytesToRead)
        val reachedEnd = numBytesRead == -1
        if (!reachedEnd) {
            pos += numBytesRead.toLong()
        }
        buf.put(bytesToRead)
        if (numBytesRead < remainingBytesInBuffer) {
            // If it is -1 then the stream was finished
            throw EOFException("$remainingBytesInBuffer remaining bytes in buffer but only $numBytesRead left in stream")
        }
    }

    override fun read(): Int {
        val byte = bais.read()
        val reachedEnd = byte == -1
        if (!reachedEnd) {
            pos++
        }
        return byte
    }
}