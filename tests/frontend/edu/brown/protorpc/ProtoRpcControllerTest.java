package edu.brown.protorpc;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ca.evanjones.protorpc.Counter;
import ca.evanjones.protorpc.Protocol;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

public class ProtoRpcControllerTest {
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private final StoreResultCallback<Message> callback = new StoreResultCallback<Message>();
    private ProtoRpcController rpc;

    private static final ByteString COUNTER_VALUE =
            Counter.Value.newBuilder().setValue(42).build().toByteString();

    @Before
    public void setUp() {
        rpc = new ProtoRpcController();
    }

    @Test
    public void testFailedErrorTextSuccess() {
        try {
            rpc.failed();
            fail("expected exception");
        } catch (IllegalStateException e) {}
        try {
            rpc.errorText();
            fail("expected exception");
        } catch (IllegalStateException e) {}

        rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
        try {
            rpc.failed();
            fail("expected exception");
        } catch (IllegalStateException e) {}
        try {
            rpc.errorText();
            fail("expected exception");
        } catch (IllegalStateException e) {}

        rpc.finishRpcSuccess(COUNTER_VALUE);
        assertFalse(rpc.failed());
        assertEquals(42, ((Counter.Value) callback.getResult()).getValue());

        try {
            rpc.errorText();
            fail("expected exception");
        } catch (IllegalStateException e) {}
    }

    @Test
    public void testFailedRpc() {
        rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
        try {
            rpc.failed();
            fail("expected exception");
        } catch (IllegalStateException e) {}

        String message = "Some user error";
        rpc.finishRpcFailure(Protocol.Status.ERROR_USER, message);
        assertTrue(rpc.failed());
        assertEquals(message, rpc.errorText());

        rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
        try {
            rpc.errorText();
            fail("expected exception");
        } catch (IllegalStateException e) {}
    }

    @Test
    public void testReuseInCallback() {
        // Callback that reuses the controller.
        RpcCallback<Message> reuseCallback = new RpcCallback<Message>() {
            @Override
            public void run(Message parameter) {
                rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
            }
        };

        rpc.startRpc(eventLoop, Counter.Value.newBuilder(), reuseCallback);
        // triggers the reuse callback, then the "real" callback
        rpc.finishRpcSuccess(COUNTER_VALUE);
        assertNull(callback.getResult());
        rpc.finishRpcSuccess(COUNTER_VALUE);
        assertFalse(rpc.failed());
        assertEquals(42, ((Counter.Value) callback.getResult()).getValue());
    }

    @Test
    public void testFailedInCallback() {
        // Test that calling rpc.failed() works in the result callback
        RpcCallback<Message> callback = new RpcCallback<Message>() {
            public void run(Message arg) {
                assertFalse(rpc.failed());
            }
        };
        rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
        rpc.finishRpcSuccess(COUNTER_VALUE);
    }

    @Test
    public void testSetFailed() {
        try {
            rpc.setFailed(null);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {}
    }

    @Test
    public void testMockFinishRpcForTest() {
        // failed() does not work if the RPC has not completed
        try {
            rpc.failed();
            fail("expected exception");
        } catch (IllegalStateException e) {}

        // this "fakes" the transaction being done
        rpc.mockFinishRpcForTest();
        assertFalse(rpc.failed());

        // Re-using the RPC controller is okay: calling this again works
        rpc.mockFinishRpcForTest();
    }

    @Test
    public void testBadStartRpc() {
        try {
            rpc.startRpc(eventLoop, Counter.Value.newBuilder(), null);
            fail("expected exception");
        } catch (NullPointerException e) {}

        rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
        try {
            rpc.startRpc(eventLoop, Counter.Value.newBuilder(), callback);
            fail("expected exception");
        } catch (IllegalStateException e) {}
    }

    @Test
    public void testBlockTestCallback() {
        // For tests, we want to "hack" rpc.block() by making it return immediately
        rpc.mockFinishRpcForTest();
        rpc.block();
    }
}
