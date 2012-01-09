package edu.brown.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class NIOWriteStreamTest {
    private MockByteChannel channel;
    private NIOWriteStream stream;
    private ByteBuffer buffer;

    @Before
    public void setUp() {
        channel = new MockByteChannel();
        stream = new NIOWriteStream(channel);
        buffer = stream.getNext();
    }

    @Test
    public void testSmallWrite() {
        final int NUM_BYTES = 7;
        assert buffer.remaining() > NUM_BYTES;
        for (int i = 0; i < NUM_BYTES; ++i) {
            buffer.put((byte) i);
        }

        // Calling getNext returns the same buffer since there should still be more space
        ByteBuffer buffer2 = stream.getNext();
        assertEquals(buffer, buffer2);

        // Flush the written bytes
        assertFalse(channel.writeCalled);
        stream.flush();

        assertEquals(1, channel.lastWrites.size());
        assertEquals(NUM_BYTES, channel.lastWrites.get(0).length);
        for (int i = 0; i < NUM_BYTES; ++i) {
            assertEquals(i, channel.lastWrites.get(0)[i]);
        }

        // extra flush does nothing
        channel.writeCalled = false;
        stream.flush();
        assertFalse(channel.writeCalled);
    }

    @Test
    public void testLargeWrite() {
        // write a lot of zeros
        final int CAPACITY = buffer.capacity();
        final int WRITE_SIZE = CAPACITY + 100;
        buffer.position(buffer.capacity()-1);

        // Didn't fill the buffer: no writing happens yet
        ByteBuffer buffer2 = stream.getNext();
        assertEquals(buffer2, buffer);
        assertFalse(channel.writeCalled);

        // this call to getNext() causes a write to happen, causing the buffer to be reused
        buffer.position(buffer.capacity());
        buffer2 = stream.getNext();
        assertFalse(channel.writeCalled);
        assertTrue(buffer2 != buffer);
        buffer2.position(WRITE_SIZE - CAPACITY);

        stream.flush();
        assertEquals(2, channel.lastWrites.size());
        assertEquals(CAPACITY, channel.lastWrites.get(0).length);
        assertEquals(WRITE_SIZE - CAPACITY, channel.lastWrites.get(1).length);
    }

    @Test
    public void testPartialLargeWrite() {
        final int CAPACITY = buffer.capacity();

        // fill one buffer; only accept part of it
        channel.numBytesToAccept = 100;
        buffer.position(buffer.capacity());
        ByteBuffer buffer2 = stream.getNext();
        assertFalse(channel.writeCalled);
        assertTrue(buffer2 != buffer);

        // fill the second buffer.
        buffer2.position(buffer2.capacity());
        ByteBuffer buffer3 = stream.getNext();
        assertFalse(channel.writeCalled);
        assertTrue(buffer3 != buffer);
        assertTrue(buffer3 != buffer2);

        buffer3.position(1);

        assertTrue(stream.flush());
        assertEquals(1, channel.lastWrites.size());
        assertEquals(100, channel.lastWrites.get(0).length);
        channel.clear();

        assertTrue(stream.flush());
        assertEquals(1, channel.lastWrites.size());
        assertEquals(0, channel.lastWrites.get(0).length);
        channel.clear();

        channel.numBytesToAccept = -1;
        assertFalse(stream.flush());
        assertEquals(3, channel.lastWrites.size());
        assertEquals(CAPACITY - 100, channel.lastWrites.get(0).length);
        assertEquals(CAPACITY, channel.lastWrites.get(1).length);
        assertEquals(1, channel.lastWrites.get(2).length);

        channel.clear();
        stream.flush();
        assertFalse(channel.writeCalled);
    }
}
