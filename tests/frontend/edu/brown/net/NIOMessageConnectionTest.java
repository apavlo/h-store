/*
Copyright (c) 2008
Evan Jones
Massachusetts Institute of Technology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package edu.brown.net;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;


public class NIOMessageConnectionTest {
    final ServerSocket localServer;
    Socket serverSide;
    NIOMessageConnection connection;
    byte[] m;

    public NIOMessageConnectionTest() throws IOException {
        localServer = new ServerSocket(0);
    }

    @Before
    public void setUp() throws IOException {
        SocketChannel client = SocketChannel.open();
        client.connect(new InetSocketAddress(InetAddress.getByName(null), localServer.getLocalPort()));
        serverSide = localServer.accept();
        connection = new NIOMessageConnection(client);
    }

    private void writeServer(byte[] data) throws IOException {
        serverSide.getOutputStream().write(data);
        try {
            // This is needed on Mac OS X to ensure that the data gets to the other side.
            // getOutputStream.flush() and Thread.yield() were not sufficient.
            Thread.sleep(1);
        } catch (InterruptedException e) { throw new RuntimeException(e); }
    }

    @Test
    public void testZeroLength() {
        try {
            connection.write(new byte[0]);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testReadMessages() throws IOException {
        assertNull(connection.tryRead());
        writeServer(new byte[]{
                5, 0, 0, 0,
                1, 2, 3, 4, 5,
                1, 0, 0, 0,
                6,
                3, 0, 0});
        m = connection.tryRead();
        assertEquals(5, m.length);
        assertEquals(1, m[0]);
        assertEquals(5, m[4]);
        m = connection.tryRead();
        assertEquals(1, m.length);
        assertEquals(6, m[0]);
        assertNull(connection.tryRead());

        writeServer(new byte[]{
                0,
                7, 8, 9});
        m = connection.tryRead();
        assertEquals(3, m.length);
        assertEquals(7, m[0]);
        assertEquals(9, m[2]);
        assertNull(connection.tryRead());
    }

    @Test
    public void testWriteMessage() throws IOException {
        final byte[] message = new byte[]{1, 2, 3};

        connection.write(message);
        byte[] buffer = new byte[4096];
        int length = serverSide.getInputStream().read(buffer);
        assertEquals(4 + message.length, length);
        assertEquals(message.length, buffer[0]);
        assertEquals(0, buffer[1]);
        assertEquals(0, buffer[2]);
        assertEquals(0, buffer[3]);
        for (int i = 0; i < message.length; ++i) {
            assertEquals(message[i], buffer[4 + i]);
        }
    }

    @Test
    public void testWritesOnce() throws IOException {
        // Counts the number of writes
        class CountWritesSocketChannel extends SocketChannel {
            public CountWritesSocketChannel() {
                super(SelectorProvider.provider());
            }
            
            public int write(ByteBuffer src) {
                writes += 1;
                byte[] discard = new byte[src.remaining()];
                src.get(discard);
                return discard.length;
            }

            public long write(ByteBuffer[] srcs, int offset, int length) {
                writes += 1;
                long sum = 0;
                for (int i = offset; i < length; ++i) {
                    sum += write(srcs[i]);
                }
                return sum;
            }

            public Socket socket() { return new Socket(); }
            protected void implConfigureBlocking(boolean block) {}

            // Unimplemented abstract methods
            public boolean connect(SocketAddress remote) { assert false; return false; }
            public boolean finishConnect() { assert false; return false; }
            public boolean isConnected() { assert false; return false; }
            public boolean isConnectionPending() { assert false; return false; }
            public int read(ByteBuffer src) { assert false; return -1; }
            public long read(ByteBuffer[] srcs, int offset, int length) { assert false; return -1; }

            protected void implCloseSelectableChannel() { assert false; }

            public int writes = 0;
            
            public SocketAddress getRemoteAddress() throws IOException {
                return (null);
            }
            public SocketChannel shutdownInput() throws IOException {
                return (null);
            }
            public SocketChannel shutdownOutput() throws IOException {
                return (null);
            }
        }

        CountWritesSocketChannel channel = new CountWritesSocketChannel();
        connection = new NIOMessageConnection(channel);
        connection.write(new byte[]{1, 2, 3});
        // There should only be one write, so there is only 1 TPC packet.
        assertEquals(1, channel.writes);
    }

    @Test
    public void testWriteLargeMessage() throws IOException {
        // Write 16k of data: this uses writev to avoid an overflow
        byte[] message = new byte[1024*16];
        connection.write(message);
        byte[] buffer = new byte[message.length + 4];
        int length = serverSide.getInputStream().read(buffer);
        assertEquals(buffer.length, length);
    }

    @Test
    public void testWriteUntilBlock() throws IOException, InterruptedException {
        // Write until we block
        int messageCount = 0;
        byte[] message = new byte[1024*16];
        while (true) {
            messageCount += 1;
            boolean writeBlocked = connection.write(message);
            if (writeBlocked) {
                break;
            }
        }

        // Needed on Mac OS X to ensure that the data gets to the other side. See writeServer().
        Thread.sleep(1);

        byte[] buffer = new byte[message.length + 4];
        for (int i = 0; i < messageCount-1; ++i) {
            int length = serverSide.getInputStream().read(buffer);
            assertEquals(buffer.length, length);
        }

        // We should not get a complete message if we do a read now
        int length = serverSide.getInputStream().read(buffer);
        assertTrue(length < buffer.length);

        // finish the write
        assertFalse(connection.tryWrite());
        int length2 = serverSide.getInputStream().read(buffer);
        assertEquals(buffer.length, length + length2);
    }

    @Test
    public void testRegister() throws IOException {
        Selector selector = Selector.open();
        SelectionKey key = connection.register(selector);

        // Nothing to select
        selector.selectNow();
        assertTrue(selector.selectedKeys().isEmpty());

        // Write some data: get the selection key
        writeServer(new byte[]{1, 0, 0, 0, 1});
        selector.selectNow();
        Set<SelectionKey> s = selector.selectedKeys();
        assertEquals(1, s.size());
        assertTrue(s.contains(key));

        // The read makes the signal go away
        assertEquals(1, connection.tryRead().length);
        s.remove(key);
        selector.selectNow();
        assertTrue(selector.selectedKeys().isEmpty());
        selector.close();
    }

    @Test
    public void testOtherEndClosedMidLength() throws IOException {
        writeServer(new byte[]{ 5, 0, 0, });
        serverSide.close();
        assertEquals(0, connection.tryRead().length);
        assertEquals(0, connection.tryRead().length);
    }

    @Test
    public void testOtherEndClosedMidMessage() throws IOException {
        writeServer(new byte[]{ 1, 0, 0, 0, 1, 5, 0, 0, 0, 0, 0, 0 });
        serverSide.close();
        assertEquals(1, connection.tryRead().length);
        assertEquals(0, connection.tryRead().length);
        assertEquals(0, connection.tryRead().length);
    }

    @Test
    public void testClose() throws IOException {
        connection.close();
        assertEquals(-1, serverSide.getInputStream().read(new byte[4096]));
    }

    @Test
    public void testRawWrite() throws IOException {
        final byte[] message = new byte[]{1, 2, 3};

        connection.rawWrite(message);
        byte[] buffer = new byte[4096];
        int length = serverSide.getInputStream().read(buffer);
        assertEquals(message.length, length);
        for (int i = 0; i < message.length; ++i) {
            assertEquals(message[i], buffer[i]);
        }
    }
}
