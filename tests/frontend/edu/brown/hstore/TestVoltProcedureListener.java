package edu.brown.hstore;

import static org.junit.Assert.assertArrayEquals;

import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.client.ProcedureCallback;

import edu.brown.protorpc.MockEventLoop;
import edu.brown.protorpc.MockServerSocketChannel;
import edu.brown.protorpc.NIOEventLoop;

import com.google.protobuf.RpcCallback;

import edu.brown.logging.LoggerUtil;
import edu.brown.hstore.VoltProcedureListener.ClientConnectionHandler;
import edu.brown.net.MockSocketChannel;

/**
 * 
 * @author pavlo
 */
public class TestVoltProcedureListener extends TestCase {
    
    static {
        LoggerUtil.setupLogging(); // HACK
    }

    private static final String HOST = "localhost";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "pass";
    private static final int PORT = Client.VOLTDB_SERVER_PORT;
    private static final String PROC_NAME = "FakeProcedure";

    private final Random rand = new Random(0);
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private VoltProcedureListener listener;
    private ListenerThread listenerThread;
    private MockHandler handler;
    
    public class MockHandler implements VoltProcedureListener.Handler {
        private Long parameter = null;
        
        @Override
        public long getInstanceId() {
            return 0;
        }
        public void procedureInvocation(byte[] serializedRequest, RpcCallback<byte[]> done) {
            StoredProcedureInvocation invocation = VoltProcedureListener.decodeRequest(serializedRequest);
            invocation.buildParameterSet();
            assertEquals(PROC_NAME, invocation.getProcName());
            assertNotNull(invocation.getParams());
            assertNotNull(invocation.getParams().toArray());
            this.parameter = (Long)invocation.getParams().toArray()[0];
            done.run(VoltProcedureListener.serializeResponse(new VoltTable[0], invocation.getClientHandle()));
        }
        public Long getParameter() {
            return this.parameter;
        }
    }

    private final class ListenerThread extends Thread {
        public void run() {
            eventLoop.run();
        }

        public void shutdown() {
            eventLoop.exitLoop();
            try {
                this.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        // Prop up a VoltProcedureListener and then the tests will iteratively try
        // the various pieces of the Volt proc protocol
        this.handler = new MockHandler();
        this.listener = new VoltProcedureListener(0, this.eventLoop, this.handler);
        this.listener.bind();

        // Run the event loop (and therefore the listener) in another thread
        listenerThread = new ListenerThread();
        listenerThread.start();
    }
    
    @Override
    protected void tearDown() {
        listenerThread.shutdown();
        listenerThread = null;
        listener.close();
    }
    
    /**
     * testAuthentication
     */
    public void testAuthentication() throws Exception {
        // Use Volt's ConnectionUtil to try to connect on our port
        Object props[] = ConnectionUtil.getAuthenticatedConnection(HOST, USERNAME, PASSWORD, PORT);
        assertNotNull(props);
        
        // Now just make sure that we got back the properties that we expected
        for (int i = 0; i < props.length; i++) {
            assertNotNull("The connection properties at [" + i + "] is null", props[i]);
            // Integer hostId, Long connectionId, Long timestamp (part of instanceId), Int leaderAddress (part of instanceId).
            if (i == 1) {
                long inner[] = (long[])props[i];
                assertNotNull(inner);
                for (int ii = 0; ii < inner.length; ii++) {
                    assertNotNull(inner[ii]);
                } // FOR
            }
        }
        // I guess we can check a bunch of stuff that we want to get back
        assert(props[0] instanceof SocketChannel);
    }
    
    /**
     * testBackupPressure
     */
    public void testBackupPressure() throws Exception {
        // Connect to listener and try to invoke something...
        Client client = ClientFactory.createClient();
        client.createConnection(null, HOST, Client.VOLTDB_SERVER_PORT, USERNAME, PASSWORD);

        final CountDownLatch latch = new CountDownLatch(1);
        final Long expected = rand.nextLong();
        final AtomicLong received = new AtomicLong(-1); 
        
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                assertNotNull(clientResponse.getResults());
                received.set(handler.getParameter());
                latch.countDown();
            }
        };
        
        boolean ret = client.callProcedure(callback, PROC_NAME, expected);
        assert(ret);
        latch.await();
        assertNotNull(this.handler.getParameter());
        assertEquals(expected, this.handler.getParameter());
    }
    
    /**
     * testExecution
     */
    public void testExecution() throws Exception {
        // Connect to listener and try to invoke something...
        Client client = ClientFactory.createClient();
        client.createConnection(null, HOST, Client.VOLTDB_SERVER_PORT, USERNAME, PASSWORD);
        
        Long expected = rand.nextLong();
        VoltTable[] result = client.callProcedure(PROC_NAME, expected).getResults();
        assertNotNull(result);
        assertNotNull(this.handler.getParameter());
        assertEquals(expected, this.handler.getParameter());
    }

    public void testBlockedWrites() {
        // Create a fake listener
        MockEventLoop mockEvent = new MockEventLoop();
        VoltProcedureListener listener = new VoltProcedureListener(0, mockEvent, handler);
        MockServerSocketChannel mockServer = new MockServerSocketChannel();
        listener.setServerSocketForTest(mockServer);

        // Make a fake connection arrive
        MockSocketChannel channel = new MockSocketChannel();
        channel.setConnected();
        mockServer.nextAccept = channel;
        assertTrue(mockEvent.handler == null);
        listener.acceptCallback(mockServer);
        assertTrue(mockEvent.handler != null);
        // TODO: Properly send a procedure request so we don't need this hacky cast, and don't
        // abuse private internals
        ClientConnectionHandler handler = (ClientConnectionHandler) mockEvent.handler;

        // Write back to the client
        final byte[] MESSAGE = { 0x1, 0x2, 0x3 };
        final byte[] EXPECTED_MESSAGE = { 0x0, 0x0, 0x0, 0x3, 0x1, 0x2, 0x3 };
        handler.run(MESSAGE);
        assertArrayEquals(EXPECTED_MESSAGE, channel.writeChannel.dequeueWrite());
        
        // Block the write
        assertTrue(mockEvent.writeHandler == null);
        channel.writeChannel.numBytesToAccept = 1;
        handler.run(MESSAGE);
        assertArrayEquals(new byte[] { 0x0 }, channel.writeChannel.dequeueWrite());
        assertEquals(handler, mockEvent.writeHandler);
        mockEvent.writeHandler = null;

        // Attempt a second write: does not re-register the handler
        handler.run(MESSAGE);
        assertNull(mockEvent.writeHandler);
        assertArrayEquals(new byte[0], channel.writeChannel.dequeueWrite());

        // Resume the write
        channel.writeChannel.numBytesToAccept = -1;
        boolean blocked = handler.writeCallback(channel);
        assertFalse(blocked);
        // The output is two copies of EXPECTED_MESSAGE. The first one is missing a byte.
        byte[] expected =  Arrays.copyOfRange(EXPECTED_MESSAGE, 1, EXPECTED_MESSAGE.length * 2);
        for (int i = 0; i < EXPECTED_MESSAGE.length; i++) {
            expected[i+EXPECTED_MESSAGE.length-1] = EXPECTED_MESSAGE[i]; 
        }
        assertArrayEquals(expected, channel.writeChannel.dequeueWrite());

        // Blocked again: re-registers the handler
        channel.writeChannel.numBytesToAccept = 0;
        handler.run(MESSAGE);
        assertEquals(handler, mockEvent.writeHandler);
        assertArrayEquals(new byte[0], channel.writeChannel.dequeueWrite());

        // unblocked while attempting to write: this would previously crash
        channel.writeChannel.numBytesToAccept = -1;
        handler.run(MESSAGE);
        expected = Arrays.copyOfRange(EXPECTED_MESSAGE, 0, EXPECTED_MESSAGE.length * 2);
        for (int i = 0; i < EXPECTED_MESSAGE.length; i++) {
            expected[i+EXPECTED_MESSAGE.length] = EXPECTED_MESSAGE[i]; 
        }
        assertArrayEquals(expected, channel.writeChannel.dequeueWrite());

        // calling the write handler should do nothing
        channel.writeChannel.writeCalled = false;
        assertFalse(handler.writeCallback(channel));
        assertFalse(channel.writeChannel.writeCalled);
    }
}
