package edu.mit.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Before;
import org.junit.Test;


public class NonBlockingConnectionTest {
    MockByteChannel channel;
    NonBlockingConnection connection;

    @Before
    public void setUp() throws IOException {
        channel = new MockByteChannel();
        connection = new NonBlockingConnection(null, channel);
    }

    @Test
    public void testNoDataRead() {
        assertEquals(0, connection.available());
        assertEquals(0, connection.readAvailable(0));
        assertEquals(0, connection.readAvailable(52));
        channel.end = true;
        assertEquals(0, connection.readAvailable(0));
        assertEquals(-1, connection.readAvailable(52));
        connection.close();
        assertTrue(channel.closed);
    }

    @Test
    public void testReadData() throws IOException {
        channel.setNextRead(new byte[42]);
        assertEquals(0, connection.available());
        assertEquals(0, connection.readAvailable(0));
        assertEquals(0, connection.available());
        assertEquals(42, connection.readAvailable(30));
        assertEquals(42, connection.available());

        // adding more data: doesn't change anything until we need it
        channel.setNextRead(new byte[17]);
        assertEquals(42, connection.readAvailable(37));
        InputStream in = connection.getInputStream();
        byte[] out = new byte[30];
        assertEquals(30, in.read(out));
        try {
            in.read(out);
            fail("expected exception");
        } catch (IllegalStateException e) {}

        assertEquals(12 + 17, connection.readAvailable(13));
        assertEquals(29, in.read(out, 0, 29));
    }

    @Test
    public void testWriteData() throws IOException {
        // Writes are buffered until flush
        OutputStream out = connection.getOutputStream();
        out.write(new byte[42]);
        assertFalse(channel.writeCalled);
        out.write(new byte[42]);
        assertFalse(channel.writeCalled);

        channel.numBytesToAccept = 80;
        boolean blocked = connection.tryFlush();
        assertTrue(blocked);
        assertEquals(80, channel.lastWrites.get(0).length);
        channel.clear();

        // When blocked, tryFlush does nothing.
        blocked = connection.tryFlush();
        assertFalse(blocked);
        assertFalse(channel.writeCalled);

        channel.numBytesToAccept = 1;
        blocked = connection.writeAvailable();
        assertTrue(blocked);
        assertEquals(1, channel.lastWrites.get(0).length);
        channel.clear();

        // Still blocked and tryFlush does nothing.
        blocked = connection.tryFlush();
        assertFalse(blocked);
        assertFalse(channel.writeCalled);

        channel.numBytesToAccept = -1;
        blocked = connection.writeAvailable();
        assertFalse(blocked);
        assertEquals(3, channel.lastWrites.get(0).length);
        channel.clear();

        out.write(new byte[1]);
        blocked = connection.tryFlush();
        assertFalse(blocked);
        assertEquals(1, channel.lastWrites.get(0).length);
    }

    @Test
    public void testWriteLargeData() throws IOException {
        // Writes are buffered until flush
        OutputStream out = connection.getOutputStream();
        byte[] big = new byte[10000];
        out.write(big);

        assertFalse(channel.writeCalled);
        assertFalse(connection.tryFlush());
        assertTrue(channel.writeCalled);

        assertTrue(channel.lastWrites.size() > 1);
        int sum = 0;
        for (byte[] write : channel.lastWrites) {
            sum += write.length;
        }
        assertEquals(big.length, sum);
    }
}
