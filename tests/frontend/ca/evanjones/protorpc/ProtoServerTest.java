package ca.evanjones.protorpc;

import static org.junit.Assert.*;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

import ca.evanjones.protorpc.Counter.*;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.net.MockSocketChannel;

public class ProtoServerTest {
    private byte[] prependLength(Message message) {
        int messageSize = message.getSerializedSize();
        byte[] data = new byte[messageSize + 4];
        CodedOutputStream out = CodedOutputStream.newInstance(data);
        try {
            out.writeRawLittleEndian32(messageSize);
            message.writeTo(out);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    private static class CounterImplementation extends CounterService {
        @Override
        public void add(RpcController controller, Value request,
                RpcCallback<Value> done) {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public void get(RpcController controller, GetRequest request,
                RpcCallback<Value> done) {
            lastRpc = controller;
            lastDone = done;
        }

        public RpcCallback<Value> lastDone;
        public RpcController lastRpc;
    }

    @SuppressWarnings("unchecked")
    public static <Type extends Message> Type parseLengthPrefixed(byte[] bytes, Type prototype) {
        CodedInputStream inStream = CodedInputStream.newInstance(bytes);
        try {
            int length = inStream.readRawLittleEndian32();
            assert length == bytes.length - 4;
            return (Type) prototype.newBuilderForType().mergeFrom(inStream).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final MockEventLoop eventLoop = new MockEventLoop();
    private ProtoServer server;
    private final MockServerSocketChannel serverChannel = new MockServerSocketChannel();
    private final MockSocketChannel channel = new MockSocketChannel();
    EventLoop.Handler connectionHandler;

    private final CounterImplementation counter = new CounterImplementation();

    @Before
    public void setUp() {
        server = new ProtoServer(eventLoop);
        server.setServerSocketForTest(serverChannel);
        channel.setConnected();
        serverChannel.nextAccept = channel;
        server.acceptCallback(serverChannel);
        connectionHandler = eventLoop.handler;
        server.register(counter);
    }

    @Test
    public void testOutOfOrderResponses() {
        // Read one request
        channel.nextRead = prependLength(ProtoRpcChannel.makeRpcRequest(
                0,
                CounterService.getDescriptor().findMethodByName("Get"),
                GetRequest.getDefaultInstance()));
        connectionHandler.readCallback(channel);

        RpcCallback<Value> firstCallback = counter.lastDone;
        assertNotNull(firstCallback);
        counter.lastDone = null;

        // Read a second request
        channel.nextRead = prependLength(ProtoRpcChannel.makeRpcRequest(
                1,
                CounterService.getDescriptor().findMethodByName("Get"),
                GetRequest.getDefaultInstance()));
        connectionHandler.readCallback(channel);

        RpcCallback<Value> secondCallback = counter.lastDone;
        assertNotNull(secondCallback);

        // Respond to the second request
        secondCallback.run(Value.newBuilder().setValue(2).build());
        try {
            RpcResponse request = parseLengthPrefixed(channel.writeChannel.dequeueWrite(),
                    RpcResponse.getDefaultInstance());
            assertEquals(1, request.getSequenceNumber());
            assertEquals(Protocol.Status.OK, request.getStatus());
            assertFalse(request.hasErrorReason());
            assertTrue(request.hasResponse());
            assertEquals(2, Value.parseFrom(request.getResponse()).getValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Respond to the first request
        firstCallback.run(Value.newBuilder().setValue(1).build());
        try {
            RpcResponse request = parseLengthPrefixed(channel.writeChannel.dequeueWrite(),
                    RpcResponse.getDefaultInstance());
            assertEquals(0, request.getSequenceNumber());
            assertEquals(Protocol.Status.OK, request.getStatus());
            assertFalse(request.hasErrorReason());
            assertTrue(request.hasResponse());
            assertEquals(1, Value.parseFrom(request.getResponse()).getValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFailedRpc() {
        // Read one request
        channel.nextRead = prependLength(ProtoRpcChannel.makeRpcRequest(
                0,
                CounterService.getDescriptor().findMethodByName("Get"),
                GetRequest.getDefaultInstance()));
        connectionHandler.readCallback(channel);

        // Fail the RPC and response
        try {
            counter.lastRpc.setFailed(null);
            fail("expected exception");
        } catch (NullPointerException e) {}
        counter.lastRpc.setFailed("failed");
        try {
            counter.lastRpc.setFailed("failed?");
            fail("expected exception");
        } catch (IllegalStateException e) {}
        counter.lastDone.run(null);

        RpcResponse request = parseLengthPrefixed(channel.writeChannel.dequeueWrite(),
                RpcResponse.getDefaultInstance());
        assertEquals(0, request.getSequenceNumber());
        assertEquals(Protocol.Status.ERROR_USER, request.getStatus());
        assertEquals("failed", request.getErrorReason());
        assertFalse(request.hasResponse());
    }

    @Test
    public void testBlockedWrite() {
        // Read one request
        channel.nextRead = prependLength(ProtoRpcChannel.makeRpcRequest(
                0,
                CounterService.getDescriptor().findMethodByName("Get"),
                GetRequest.getDefaultInstance()));
        connectionHandler.readCallback(channel);

        RpcCallback<Value> firstCallback = counter.lastDone;
        assertNotNull(firstCallback);
        counter.lastDone = null;

        // block the channel
        channel.writeChannel.numBytesToAccept = 1;

        // Respond to the request
        firstCallback.run(Value.newBuilder().setValue(1).build());
        assertEquals(1, channel.writeChannel.dequeueWrite().length);
        assertEquals(0, channel.writeChannel.numBytesToAccept);

        // trigger the write callback
        channel.writeChannel.numBytesToAccept = 1;
        assertTrue(eventLoop.writeHandler.writeCallback(channel));
        assertEquals(1, channel.writeChannel.dequeueWrite().length);
        assertEquals(0, channel.writeChannel.numBytesToAccept);

        // unblock the channel and finish writing
        channel.writeChannel.numBytesToAccept = -1;
        assertFalse(eventLoop.writeHandler.writeCallback(channel));
        assertTrue(channel.writeChannel.dequeueWrite().length > 0);
    }

    @Test
    public void testCallbackTwice() {
        // Read one request
        channel.nextRead = prependLength(ProtoRpcChannel.makeRpcRequest(
                0,
                CounterService.getDescriptor().findMethodByName("Get"),
                GetRequest.getDefaultInstance()));
        connectionHandler.readCallback(channel);

        // Respond to the request twice (incorrectly)
        counter.lastDone.run(Value.newBuilder().setValue(1).build());
        RpcResponse request = parseLengthPrefixed(channel.writeChannel.dequeueWrite(),
                RpcResponse.getDefaultInstance());
        assertEquals(0, request.getSequenceNumber());

        try {
            counter.lastDone.run(Value.newBuilder().setValue(1).build());
            fail("expected exception");
        } catch (IllegalStateException e) {}
        assertTrue(channel.writeChannel.lastWrites.isEmpty());
    }
}
