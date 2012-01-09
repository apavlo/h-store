package ca.evanjones.protorpc;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.junit.Before;
import org.junit.Test;

import ca.evanjones.protorpc.Counter.CounterService;
import ca.evanjones.protorpc.Protocol.RpcRequest;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

import edu.brown.net.MockByteChannel;
import edu.brown.net.MockSocketChannel;
import edu.brown.net.NonBlockingConnection;

public class ProtoRpcChannelTest {
    private MockEventLoop eventLoop;
    private MockByteChannel channel;
    private ProtoRpcChannel rpcChannel;
    private final StoreResultCallback<Message> callback = new StoreResultCallback<Message>();
    private final StoreResultCallback<Message> secondCallback = new StoreResultCallback<Message>();
    private static final MethodDescriptor ADD_METHOD =
            CounterService.getDescriptor().findMethodByName("Add");

    @Before
    public void setUp() {
        eventLoop = new MockEventLoop();
        channel = new MockByteChannel();

        rpcChannel = new ProtoRpcChannel(eventLoop, new ProtoRpcChannel.ConnectFactory() {
            @Override
            public NonBlockingConnection startNewConnection() {
                return new NonBlockingConnection(null, channel);
            }
        });
    }

    private Counter.Value makeValue(int v) {
        return Counter.Value.newBuilder().setValue(v).build();
    }

    private void callAdd(int value, RpcCallback<Message> callback) {
        callAdd(value, callback, new ProtoRpcController());
    }

    private void callAdd(int value, RpcCallback<Message> callback, ProtoRpcController controller) {
        rpcChannel.callMethod(ADD_METHOD, controller, makeValue(value),
                Counter.Value.getDefaultInstance(), callback);
    }

    private void validateAdd(int sequence, int expectedValue) {
        try {
            CodedInputStream codedInput = CodedInputStream.newInstance(channel.lastWrites.get(0));
            int length = codedInput.readRawLittleEndian32();
            assertEquals(length, channel.lastWrites.get(0).length - 4);
            RpcRequest request = RpcRequest.parseFrom(codedInput);
            assertEquals(sequence, request.getSequenceNumber());
            assertEquals(ADD_METHOD.getFullName(), request.getMethodName());
            Counter.Value value = Counter.Value.parseFrom(request.getRequest());
            assertEquals(expectedValue, value.getValue());
            channel.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void respondAdd(int sequence, StoreResultCallback<Message> callback) {
        RpcResponse.Builder builder = RpcResponse.newBuilder();
        builder.setSequenceNumber(sequence);
        builder.setStatus(Protocol.Status.OK).setResponse(makeValue(100).toByteString());
        RpcResponse response = builder.build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(out);
        try {
            codedOutput.writeRawLittleEndian32(response.getSerializedSize());
            response.writeTo(codedOutput);
            codedOutput.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        channel.setNextRead(out.toByteArray());

        // Call the read callback: the second callback should be triggered
        rpcChannel.readCallback(null);
        Counter.Value value = (Counter.Value) callback.getResult();
        assertEquals(100, value.getValue());
        callback.reset();
    }

    private final class MockSocketConnectFactory implements ProtoRpcChannel.ConnectFactory {
        public MockSocketChannel lastChannel;

        @Override
        public NonBlockingConnection startNewConnection() {
            lastChannel = new MockSocketChannel();
            return new NonBlockingConnection(lastChannel);
        }
    }

    @Test
    public void testUnconnected() {
        eventLoop.handler = null;

        MockSocketConnectFactory connector = new MockSocketConnectFactory();
        rpcChannel = new ProtoRpcChannel(eventLoop, connector);
        assertNull(eventLoop.handler);

        // The socket is not connected yet: queue the write
        MockSocketChannel mockSocket = connector.lastChannel;
        mockSocket.writeChannel.numBytesToAccept = 1;
        callAdd(42, callback);
        assertFalse(mockSocket.writeChannel.writeCalled);

        // Finish the connection: registered for reading and the write is sent: but it will block
        assertNull(eventLoop.handler);
        assertNull(eventLoop.writeHandler);
        rpcChannel.connectCallback(mockSocket);
        assertNotNull(eventLoop.handler);
        assertTrue(mockSocket.writeChannel.writeCalled);
        assertNotNull(eventLoop.writeHandler);

        mockSocket.writeChannel.writeCalled = false;
        eventLoop.writeHandler = null;
        rpcChannel.writeCallback(mockSocket);
        assertTrue(mockSocket.writeChannel.writeCalled);
        assertEquals(2, mockSocket.writeChannel.lastWrites.size());
        assertNull(eventLoop.writeHandler);
    }

    @Test
    public void testFailedConnection() throws IOException {
        MockSocketConnectFactory connector = new MockSocketConnectFactory();
        rpcChannel = new ProtoRpcChannel(eventLoop, connector);

        // Finish but fail the connection
        connector.lastChannel.close();
        try {
            rpcChannel.connectCallback(connector.lastChannel);
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof ConnectException);
        }
    }

    @Test
    public void testSetReconnectInterval() throws IOException {
        MockSocketConnectFactory connector = new MockSocketConnectFactory();
        rpcChannel = new ProtoRpcChannel(eventLoop, connector);

        // Enable reconnection
        rpcChannel.setReconnectInterval(5);

        // Finish but fail the connection: nothing should happen
        MockSocketChannel mockSocket = connector.lastChannel;
        connector.lastChannel = null;
        mockSocket.close();
        rpcChannel.connectCallback(mockSocket);
        assertNull(connector.lastChannel);
        assertSame(eventLoop.timerHandler, rpcChannel);

        // Any RPCs will fail until it attempts to reconnect
        ProtoRpcController controller = new ProtoRpcController();
        callAdd(42, callback, controller);
        assertNull(callback.getResult());
        assertTrue(controller.failed());

        // Trigger the reconnect
        rpcChannel.timerCallback();
        assertNotNull(connector.lastChannel);
        assertNotSame(connector.lastChannel, mockSocket);
        mockSocket = connector.lastChannel;

        // Any RPCs will fail until it attempts to reconnect
        callAdd(42, callback, controller);
        assertFalse(mockSocket.writeChannel.writeCalled);

        // Connection completed
        mockSocket.setConnecting();
        rpcChannel.connectCallback(mockSocket);
        assertTrue(mockSocket.writeChannel.writeCalled);
    }

    @Test
    public void testOutOfOrderResponses() {
        callAdd(42, callback);
        validateAdd(0, 42);

        // Make a second request
        callAdd(0, secondCallback);
        validateAdd(1, 0);

        // Put the answer for the second request in the connection
        respondAdd(1, secondCallback);

        // Put the answer for the first request in the connection
        respondAdd(0, callback);
    }

    /** When the channel is closed, all pending RPCs are failed. */
    @Test
    public void testPendingRpcCloseFailure() {
        ProtoRpcController controller = new ProtoRpcController();
        callAdd(42, callback, controller);
        assertFalse(callback.wasCalled());

        rpcChannel.close();
        assertTrue(callback.wasCalled());
        assertNull(callback.getResult());
        assertTrue(controller.failed());
        assertEquals("Connection closed", controller.errorText());

        // Closing more than once does not work
        try {
            rpcChannel.close();
            fail("expected exception");
        } catch (IllegalStateException e) {}
    }

    @Test
    public void testBadSequenceResponse() {
        callAdd(42, callback);
        validateAdd(0, 42);

        // Put an answer for a bad sequence number (NullPointerException / AssertionError)
        try {
            respondAdd(1, callback);
        } catch (AssertionError e) {
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void testBlockingSends() {
        assertNull(eventLoop.writeHandler);
        channel.numBytesToAccept = 1;
        callAdd(42, callback);
        assertTrue(channel.writeCalled);
        assertNotNull(eventLoop.writeHandler);

        // Call the write handler to write some more data: keep the handler registered
        channel.writeCalled = false;
        assertTrue(eventLoop.writeHandler.writeCallback(null));
        assertTrue(channel.writeCalled);

        // Unblock the write and try again
        channel.numBytesToAccept = -1;
        assertFalse(eventLoop.writeHandler.writeCallback(null));
        assertTrue(channel.writeCalled);
    }

    @Test
    public void testSendConcurrentBlock() {
        channel.numBytesToAccept = 1;
        callAdd(42, callback);
        assertNotNull(eventLoop.writeHandler);
        callAdd(43, secondCallback);

        // Unblock the write and try again
        channel.numBytesToAccept = -1;
        assertFalse(eventLoop.writeHandler.writeCallback(null));
        assertTrue(channel.writeCalled);
    }

    private static final class Listener {
        private final ServerSocket listenSocket;
        private final Thread listenThread;

        private volatile boolean quit = false;
        
        public Listener() {
            try {
                listenSocket = new ServerSocket(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Start the thread running
            listenThread = new Thread() {
                @Override
                public void run() {
                    acceptThread();
                }
            };
            listenThread.start();
        }

        private void acceptThread() {
            try {
                while (!quit) {
                    final Socket client = listenSocket.accept();
                    new Thread() {
                        @Override
                        public void run() {
                            byte[] data = new byte[64];
                            try {
                                while (true) {
                                    int count = client.getInputStream().read(data);
                                    if (count == -1) break;
                                    assert count > 0;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }.start();
                }
            } catch (SocketException e) {
                assert e.getMessage().equals("Socket closed");
                assert quit;
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        }

        public void shutdown() {
            assert listenThread.isAlive();
            assert !quit;
            quit = true;
            try {
                listenSocket.close();
                listenThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public InetSocketAddress getAddress() {
            return new InetSocketAddress(listenSocket.getLocalPort());
        }
    }

    private static final class TimerHandler extends AbstractEventHandler {
        boolean timerExpired;
        NIOEventLoop eventLoop;

        @Override
        public void timerCallback() {
            timerExpired = true;
            eventLoop.exitLoop();
        }
    }

    @Test(timeout=ProtoRpcChannel.TOTAL_CONNECT_TIMEOUT_MS*2)
    public void testConnectParallel() {
        NIOEventLoop realEventLoop = new NIOEventLoop();

        Listener l1 = new Listener();
        Listener l2 = new Listener();
        Listener l3 = new Listener();

        // Set a timer to expire after the parallel connect timer
        TimerHandler handler = new TimerHandler();
        handler.eventLoop = realEventLoop;
        realEventLoop.registerTimer(ProtoRpcChannel.TOTAL_CONNECT_TIMEOUT_MS + 2000, handler);

        InetSocketAddress[] addresses = { l1.getAddress(), l2.getAddress(), l3.getAddress() };
        ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(realEventLoop, addresses);
        assertEquals(3, channels.length);

        realEventLoop.run();
        assertTrue(handler.timerExpired);

        for (ProtoRpcChannel c : channels) {
            c.close();
        }

        l1.shutdown();
        l2.shutdown();
        l3.shutdown();
    }

    @Test
    public void testConnectParallel2() {
        NIOEventLoop realEventLoop = new NIOEventLoop();

        Listener l1 = new Listener();
        Listener l2 = new Listener();
        Listener l3 = new Listener();

        for (int i = 0; i < 100; ++i) {
            InetSocketAddress[] addresses = { l1.getAddress(), l2.getAddress(), l3.getAddress() };
            ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(realEventLoop, addresses);
            assertEquals(3, channels.length);

            for (ProtoRpcChannel c : channels) {
                c.close();
            }
        }

        l1.shutdown();
        l2.shutdown();
        l3.shutdown();
    }
}
