package ca.evanjones.protorpc;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import ca.evanjones.protorpc.Counter.CounterService;
import ca.evanjones.protorpc.Protocol.RpcRequest;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Descriptors.MethodDescriptor;

import edu.mit.net.MockByteChannel;
import edu.mit.net.NonBlockingConnection;

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
        rpcChannel = new ProtoRpcChannel(eventLoop, new NonBlockingConnection(null, channel));
    }

    private Counter.Value makeValue(int v) {
        return Counter.Value.newBuilder().setValue(v).build();
    }

    private void callAdd(int value, RpcCallback<Message> callback) {
        rpcChannel.callMethod(ADD_METHOD, new ProtoRpcController(), makeValue(value),
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
}
