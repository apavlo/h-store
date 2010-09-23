package ca.evanjones.protorpc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import sun.misc.Signal;

public class NIOEventLoopTest {
    private static final Signal SIGINT = new Signal("INT");
    private static final class SigIntSender implements Runnable {
        public void run() {
            Signal.raise(SIGINT);
        }
    }

    @Test
    public void testSigInt() throws InterruptedException {
        final NIOEventLoop eventLoop = new NIOEventLoop();
        try {
            eventLoop.setExitOnSigInt(false);
            fail("expected exception");
        } catch (IllegalStateException e) {}

        eventLoop.setExitOnSigInt(true);
        try {
            eventLoop.setExitOnSigInt(true);
            fail("expected exception");
        } catch (IllegalStateException e) {}
        eventLoop.setExitOnSigInt(false);
        eventLoop.setExitOnSigInt(true);

        // Deliver SIGINT before looping
        Signal.raise(SIGINT);
        eventLoop.run();

        // Deliver SIGINT while looping
        eventLoop.setExitOnSigInt(true);
        SigIntSender sender = new SigIntSender();
        eventLoop.runInEventThread(sender);
        eventLoop.run();

        class Waker extends Thread {
            public void run() {
                try {
                    sleep(20);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                eventLoop.exitLoop();
            }
        }

        Waker waker = new Waker();
        waker.start();

        // This should block until woken by something else
        eventLoop.run();
        waker.join(1);
        assertFalse(waker.isAlive());
    }

    static final class TestHandler implements EventLoop.Handler {
        SocketChannel client;
        boolean write;

        @Override
        public void acceptCallback(SelectableChannel channel) {
            // accept the connection
            assert client == null;
            try {
                client = ((ServerSocketChannel) channel).accept();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            assert client != null;
        }

        @Override
        public void readCallback(SelectableChannel channel) {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public boolean writeCallback(SelectableChannel channel) {
            assert !write;
            write = true;
            return true;
        }
    }

    private static void writeAll(SocketChannel channel, String message) {
        ByteBuffer b = ByteBuffer.allocate(4096);
        b.position(b.limit());
        while (b.remaining() == 0) {
            b.clear();
            try {
                int bytes = channel.write(b);
                assert bytes >= 0;
                //~ System.out.println(message + " " + bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testWriteUnblocking() throws IOException, InterruptedException {
        final NIOEventLoop eventLoop = new NIOEventLoop();
        final TestHandler serverHandler = new TestHandler();

        final ServerSocketChannel acceptSocket = ServerSocketChannel.open();
        // Mac OS X: Must bind() before calling Selector.register, or you don't get accept() events
        acceptSocket.socket().bind(null);

        eventLoop.registerAccept(acceptSocket, serverHandler);

        // Run a client in a new thread: connect, wait for a latch, read as much as possible
        final int serverPort = acceptSocket.socket().getLocalPort();
        final CountDownLatch startReadingLatch = new CountDownLatch(1);
        Thread client = new Thread() {
            public void run() {
                try {
                    SocketChannel clientSocket = SocketChannel.open();
                    clientSocket.connect(
                            new InetSocketAddress(InetAddress.getLocalHost(), serverPort));

                    startReadingLatch.await();

                    ByteBuffer input = ByteBuffer.allocate(4096);
                    int bytes = clientSocket.read(input);
                    assert bytes > 0;
                    //~ System.out.println("read " + bytes);
                    clientSocket.configureBlocking(false);
                    while (input.position() != 0) {
                        input.clear();
                        bytes = clientSocket.read(input);
                        //~ System.out.println("read " + bytes);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        client.start();

        assertNull(serverHandler.client);
        eventLoop.runOnce();
        serverHandler.client.configureBlocking(false);

        // Write data into the socket until it blocks. We seem to need to do this twice on Linux
        // and Mac OS X to actually fill the buffer.
        for (int i = 0; i < 2; ++i) {
            writeAll(serverHandler.client, "write" + i);
            if (i == 0) {
                // Register a write callback (can only do this once)
                eventLoop.registerWrite(serverHandler.client, serverHandler);
                try {
                    eventLoop.registerWrite(serverHandler.client, serverHandler);
                    fail("expected exception");
                } catch (AssertionError e) {}
            } else {
                assert i == 1;
                // trigger reading
                startReadingLatch.countDown();
            }

            assertFalse(serverHandler.write);
            eventLoop.runOnce();
            assertTrue(serverHandler.write);
            serverHandler.write = false;
        }

        int count = serverHandler.client.write(ByteBuffer.allocate(4096));
        assertTrue(count > 0);
        //~ System.out.println("final write " + count);

        client.join();
    }
}
