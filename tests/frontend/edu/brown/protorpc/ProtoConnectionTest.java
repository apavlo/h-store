package edu.brown.protorpc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import ca.evanjones.protorpc.Counter;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import edu.brown.net.MockByteChannel;
import edu.brown.net.NonBlockingConnection;

public class ProtoConnectionTest {
    MockByteChannel channel;
    NonBlockingConnection nonblock;
    ProtoConnection connection;

    @Before
    public void setUp() throws IOException {
        channel = new MockByteChannel();
        connection = new ProtoConnection(new NonBlockingConnection(null, channel));
    }

    @Test
    public void testTryWrite() throws IOException {
        Counter.Value v = Counter.Value.newBuilder().setValue(42).build();
        assertFalse(connection.tryWrite(v));

        CodedInputStream in = CodedInputStream.newInstance(channel.lastWrites.get(0));
        int length = in.readRawLittleEndian32();
        assertEquals(length, channel.lastWrites.get(0).length - 4);
        Counter.Value w = Counter.Value.parseFrom(in);
        assertEquals(v, w);
        assertTrue(in.isAtEnd());
        channel.clear();

        channel.numBytesToAccept = 3;
        assertTrue(connection.tryWrite(v));
        channel.numBytesToAccept = -1;
        assertFalse(connection.writeAvailable());
        assertEquals(2, channel.lastWrites.size());
    }

    
    
    @Test
    public void testReadBufferedMessage() throws IOException {
        Counter.Value.Builder builder = Counter.Value.newBuilder();
        assertTrue(connection.readAllAvailable());
        assertFalse(connection.readBufferedMessage(builder));

        Counter.Value v = Counter.Value.newBuilder().setValue(42).build();
        byte[] all = makeConnectionMessage(v);
        byte[] fragment1 = new byte[3];
        System.arraycopy(all, 0, fragment1, 0, fragment1.length);
        byte[] fragment2 = new byte[all.length - fragment1.length];
        System.arraycopy(all, fragment1.length, fragment2, 0, fragment2.length);
        channel.setNextRead(fragment1);
        assertTrue(connection.readAllAvailable());
        assertTrue(connection.readAllAvailable());
        assertFalse(connection.readBufferedMessage(builder));
        channel.setNextRead(fragment2);
        assertTrue(connection.readAllAvailable());
        assertTrue(connection.readBufferedMessage(builder));
        assertEquals(v, builder.build());

        channel.end = true;
        assertFalse(connection.readBufferedMessage(builder));
        connection.close();
        assertTrue(channel.closed);
    }

    private static byte[] makeConnectionMessage(Counter.Value value)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(out);
        codedOutput.writeRawLittleEndian32(value.getSerializedSize());
        value.writeTo(codedOutput);
        codedOutput.flush();

        byte[] all = out.toByteArray();
        return all;
    }

    @Test
    public void testInputStreamLimitReset() throws IOException {
        // Build a ~40 MB string
        final int MEGABYTE = 1 << 20;
        final int CODED_INPUT_LIMIT = 64;
        char[] megabyte = new char[MEGABYTE];
        for (int i = 0; i < megabyte.length; ++i) {
            megabyte[i] = 'a';
        }
        String megaString = new String(megabyte);

        Counter.Value megaValue = Counter.Value.newBuilder()
                .setName(megaString.toString())
                .setValue(42)
                .build();
        byte[] all = makeConnectionMessage(megaValue);

        Counter.Value.Builder builder = Counter.Value.newBuilder();
        for (int i = 0; i < CODED_INPUT_LIMIT * 2; ++i) {
            channel.setNextRead(all);
            assertTrue(connection.readAllAvailable());
            assertTrue(connection.readBufferedMessage(builder));
            builder.clear();
        }
    }
}
