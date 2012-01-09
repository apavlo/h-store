package edu.brown.net;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;

public class ByteBufferFifo {
    /** These buffers are available for reading (taking data). */
    private final ArrayDeque<ByteBuffer> readBuffers = new ArrayDeque<ByteBuffer>();

    /** This buffer is where data is written (putting data). */
    private ByteBuffer currentWrite;

    /** Cache of available ByteBuffers, since direct ByteBuffers are "expensive" to allocate. */
    private final ArrayDeque<ByteBuffer> emptyBuffers = new ArrayDeque<ByteBuffer>();

    /** If we alternate reads and writes, this is used to save the read position of the buffer. */
    private int savedReadPosition = 0;

    private boolean bigEndian = false;

    private static final int BUFFER_SIZE = 4096;

    public void clear() {
        readBuffers.clear();
        emptyBuffers.clear();
        currentWrite = null;
    }

    public ByteBuffer getWriteBuffer() {
        if (currentWrite == null) {
            // If we have a single read buffer, there may be space left in it (combine writes!)
            if (readBuffers.size() == 1) {
                ByteBuffer lastRead = readBuffers.peekLast();
                if (!lastRead.hasRemaining()) {
                    // The buffer is empty: remove it
                    removeEmptyReadBuffer();
                } else if (lastRead.limit() < lastRead.capacity()) {
                    // There is space! Remember its read position and make it a write buffer
                    readBuffers.removeLast();
                    assert savedReadPosition == 0;
                    savedReadPosition = lastRead.position();
                    lastRead.position(lastRead.limit());
                    lastRead.limit(lastRead.capacity());
                    currentWrite = lastRead;
                }
           } else {
                // if more than 1 read buffer, the buffer is full
                assert readBuffers.isEmpty() ||
                        readBuffers.peekLast().limit() == readBuffers.peekLast().capacity();
            }

            if (currentWrite == null) {
                // last resort: allocate a new buffer
                currentWrite = allocateBuffer();
            }
        } else if (currentWrite.remaining() == 0) {
            // buffer is full: get a new one
            /* TODO: Support flushing buffers early? Does this help performance?
            if (readBuffers.isEmpty()) {
                // no queued buffers yet: attempt to write it?
                flush();
            }*/

            queueWriteBuffer();
            currentWrite = allocateBuffer();
            assert currentWrite.remaining() == currentWrite.capacity();
        }

        assert currentWrite.remaining() > 0;
        return currentWrite;
    }

    /** Returns the next ByteBuffer for reading or null if the FIFO is empty. */
    public ByteBuffer getReadBuffer() {
        ByteBuffer buffer = readBuffers.peekFirst();

        if (buffer != null && buffer.remaining() == 0) {
            // this buffer has been consumed: discard it and try the next
            removeEmptyReadBuffer();

            buffer = readBuffers.peekFirst();
            assert buffer == null || buffer.remaining() > 0;
        }

        if (buffer == null && currentWrite != null && currentWrite.position() > 0) {
            // Last resort: there is data in our current write buffer: return it
            buffer = currentWrite;
            queueWriteBuffer();
        }

        assert buffer == null || buffer.remaining() > 0;
        return buffer;
    }

    private void removeEmptyReadBuffer() {
        ByteBuffer buffer = readBuffers.removeFirst();
        assert !buffer.hasRemaining();
        buffer.clear();
        emptyBuffers.add(buffer);
    }

    private void queueWriteBuffer() {
        // Flip and queue the write buffer
        currentWrite.flip();
        currentWrite.position(savedReadPosition);
        savedReadPosition = 0;
        assert currentWrite.remaining() > 0;
        readBuffers.add(currentWrite);

        // Delay allocation of write buffers: this lets us typically reuse one buffer. We write to
        // it, flip it and read from it, then recycle it via the emptyBuffers queue.
        currentWrite = null;
    }

    private ByteBuffer allocateBuffer() {
        ByteBuffer buffer = emptyBuffers.pollLast();
        if (buffer == null) {
            buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            if (!bigEndian) {
                buffer.order(ByteOrder.nativeOrder());
            }
        }
        return buffer;
    }

    // TODO: This is a hack. Make a proper setByteOrder method?
    public void setBigEndian() {
        assert !bigEndian;
        bigEndian = true;
        @SuppressWarnings("rawtypes")
        ArrayDeque[] collections = {
                emptyBuffers, readBuffers
        };
        for (ArrayDeque<ByteBuffer> collection : collections) {
            for (ByteBuffer buffer : collection) {
                buffer.order(ByteOrder.BIG_ENDIAN);
            }
        }
        if (currentWrite != null) {
            currentWrite.order(ByteOrder.BIG_ENDIAN);
        }
    }
}
