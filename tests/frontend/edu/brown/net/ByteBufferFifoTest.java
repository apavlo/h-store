package edu.brown.net;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;


public class ByteBufferFifoTest {
    ByteBufferFifo fifo = new ByteBufferFifo();

    @Test
    public void testBigWrites() {
        ByteBuffer out = fifo.getWriteBuffer();
        out.put(new byte[out.remaining()]);

        ByteBuffer second = fifo.getWriteBuffer();
        assertTrue(second != out);
        second.put((byte) 1);

        ByteBuffer read = fifo.getReadBuffer();
        assertTrue(read.remaining() > 0);
        read.position(read.limit());

        assertEquals((byte) 1, fifo.getReadBuffer().get());
        assertNull(fifo.getReadBuffer());
    }

    @Test
    public void testCombineWriteReadWrite() {
        assertNull(fifo.getReadBuffer());

        // Put 2 bytes in the FIFO
        final byte[] DATA = { 0, 1, };
        fifo.getWriteBuffer().put(DATA);

        // Take 1 byte from the FIFO
        assertEquals((byte) 0, fifo.getReadBuffer().get());

        // Put another byte into the FIFO. Previously, this would allocate a new buffer. However,
        // it is probably better if we can combine the writes: results in fewer system calls and
        // fewer network packets.
        fifo.getWriteBuffer().put((byte) 2);

        // Read all the bytes from the FIFO
        ByteBuffer read = fifo.getReadBuffer();
        assertEquals(2, read.remaining());
        assertEquals((byte) 1, read.get());
        assertEquals((byte) 2, read.get());
        assertEquals(null, fifo.getReadBuffer());
    }

    @Test
    public void testCombineWriteNoReadData() {
        // Put 1 byte, read 1 byte, write nothing
        fifo.getWriteBuffer().put((byte) 1);
        assertEquals((byte) 1, fifo.getReadBuffer().get());
        ByteBuffer output = fifo.getWriteBuffer();
        assertEquals(0, output.position());

        // Try to read: nothing!
        assertNull(fifo.getReadBuffer());
    }
}
