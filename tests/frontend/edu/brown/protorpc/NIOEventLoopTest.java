package edu.brown.protorpc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import sun.misc.Signal;

public class NIOEventLoopTest {
    private static final Signal SIGINT = new Signal("INT");
    private static final class SigIntSender implements Runnable {
        public void run() {
            Signal.raise(SIGINT);
        }
    }

    protected NIOEventLoop eventLoop;
    protected TestHandler serverHandler;
    protected ServerSocketChannel acceptSocket;
    protected int serverPort;

    @Before
    public void setUp() throws IOException {
        eventLoop = new NIOEventLoop();
        serverHandler = new TestHandler();
        acceptSocket = ServerSocketChannel.open();
        // Mac OS X: Must bind() before calling Selector.register, or you don't get accept() events
        acceptSocket.socket().bind(null);
        serverPort = acceptSocket.socket().getLocalPort();
    }

    @After
    public void tearDown() throws IOException {
        acceptSocket.close();
    }

    @Test(timeout=5000)
    public void testSigInt() throws InterruptedException {
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

    static final class TestHandler extends AbstractEventHandler {
        SocketChannel client;
        boolean write;
        boolean connected;
        int timerExpiredCount = 0;

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
        public void connectCallback(SocketChannel channel) {
            connected = true;
        }

        @Override
        public boolean writeCallback(SelectableChannel channel) {
            assert !write;
            write = true;
            return true;
        }

        @Override
        public void timerCallback() {
            timerExpiredCount += 1;
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
        eventLoop.registerAccept(acceptSocket, serverHandler);

        // Run a client in a new thread: connect, wait for a latch, read as much as possible
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

    @Test
    public void testConnectCallback() throws IOException {
        eventLoop.registerAccept(acceptSocket, serverHandler);

        SocketChannel clientSocket = SocketChannel.open();
        clientSocket.configureBlocking(false);
        boolean connected = clientSocket.connect(
                new InetSocketAddress(InetAddress.getLocalHost(), serverPort));
        assertFalse(connected);
        assertTrue(clientSocket.isConnectionPending());
        eventLoop.registerConnect(clientSocket, serverHandler);
        // registering it for reading as well is not permitted: causes problems on Linux
        try {
            eventLoop.registerRead(clientSocket, serverHandler);
            fail("expected exception");
        } catch (AssertionError e) {}

        // The event loop will trigger the accept callback
        assertNull(serverHandler.client);
        eventLoop.runOnce();
        assertNotNull(serverHandler.client);
        assertTrue(clientSocket.isConnectionPending());

        // The event loop will also have triggered the connect callback
        assertTrue(serverHandler.connected);
        connected = clientSocket.finishConnect();
        assertTrue(connected);
        assertTrue(clientSocket.isConnected());

        // Registering some other handler in response to the connect event should work
        eventLoop.registerRead(clientSocket, new TestHandler());

        clientSocket.close();
    }

//    @Test(timeout=2000)
//    public void testTimeout() {
//        long start = System.nanoTime();
//        int msDelay = 500;
//        eventLoop.registerTimer(msDelay, serverHandler);
//        int loopCount = 0;
//        while (serverHandler.timerExpiredCount == 0) {
//            eventLoop.runOnce();
//            loopCount += 1;
//        }
//        long end = System.nanoTime();
//        assertTrue(end - start >= msDelay * 1000000);
//        // Linux typically expires timeouts a few ms early, but we shouldn't have to loop more
//        // than twice. But we could, so this might need adjustment.
//        assertTrue(loopCount <= 2);
//    }

    // If this times out, it probably means all the timers didn't get set
    @Test(timeout=500)
    public void testManyTimeouts() {
        final int MS_DELAY = 100;
        final int NUM_TIMERS = 10;

        for (int i = 0; i < NUM_TIMERS; ++i) {
            eventLoop.registerTimer(MS_DELAY, serverHandler);
        }

        while (serverHandler.timerExpiredCount < NUM_TIMERS) {
            eventLoop.runOnce();
        }
    }

    @Test(timeout=2000)
    public void testCancelTimeout() {
        final int REAL_DELAY = 100;
        eventLoop.registerTimer(REAL_DELAY - 50, serverHandler);
        TestHandler handler2 = new TestHandler();
        eventLoop.registerTimer(REAL_DELAY, handler2);
        eventLoop.cancelTimer(serverHandler);

        while (handler2.timerExpiredCount == 0) {
            eventLoop.runOnce();
        }

        assertEquals(0, serverHandler.timerExpiredCount);
    }
}
