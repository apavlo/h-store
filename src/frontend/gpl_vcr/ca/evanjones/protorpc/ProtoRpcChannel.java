package ca.evanjones.protorpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

import ca.evanjones.protorpc.EventLoop.Handler;
import ca.evanjones.protorpc.Protocol.RpcRequest;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;

import edu.mit.net.NonBlockingConnection;

public class ProtoRpcChannel implements RpcChannel, Handler {
    private final EventLoop eventLoop;
    private int sequence;
    private final ProtoConnection connection;
    private final HashMap<Integer, ProtoRpcController> pendingRpcs =
            new HashMap<Integer, ProtoRpcController>();

    public ProtoRpcChannel(EventLoop eventLoop, InetSocketAddress address) {
        this.eventLoop = eventLoop;
        try {
            SocketChannel socket = SocketChannel.open();
            boolean finished = socket.connect(address);
            assert finished;
            // TODO: Support async connects?
//            if (finished) {
//                socket.finishConnect();
//            }

            connection = new ProtoConnection(new NonBlockingConnection(socket));
            this.eventLoop.registerRead(socket, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ProtoRpcChannel(EventLoop eventLoop, NonBlockingConnection connection) {
        this.eventLoop = eventLoop;
        this.connection = new ProtoConnection(connection);
    }

    public void callMethod(Descriptors.MethodDescriptor method,
            RpcController controller, Message request,
            Message responsePrototype, RpcCallback<Message> done) {
        ProtoRpcController rpc = (ProtoRpcController) controller;
        rpc.startRpc(eventLoop, responsePrototype.newBuilderForType(), done);

        // Package up the request and send it
        synchronized (this) {
            pendingRpcs.put(sequence, rpc);
            // System.err.println("Sending RPC sequence " + sequence);
            RpcRequest rpcRequest = makeRpcRequest(sequence, method, request);
            sequence += 1;
            boolean blocked = connection.tryWrite(rpcRequest);
            if (blocked) {
                // the write blocked: wait for write callbacks
                eventLoop.registerWrite(connection.getChannel(), this);
            }
        }
    }

    public static RpcRequest makeRpcRequest(
            int sequence, Descriptors.MethodDescriptor method, Message request) {
        RpcRequest.Builder requestBuilder = RpcRequest.newBuilder();
        requestBuilder.setSequenceNumber(sequence);
        requestBuilder.setMethodName(method.getFullName());
        requestBuilder.setRequest(request.toByteString());
        return requestBuilder.build();
    }

    @Override
    public void acceptCallback(SelectableChannel channel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readCallback(SelectableChannel channel) {
        while (true) {
            // TODO: Cache this builder object?
            RpcResponse.Builder builder = RpcResponse.newBuilder();
            ProtoConnection.Status status = connection.tryRead(builder);
            if (status == ProtoConnection.Status.CLOSED) {
                // TODO: Fail any subsequent RPCs
                throw new UnsupportedOperationException("Connection closed: not handled (for now).");
            } else if (status == ProtoConnection.Status.NO_MESSAGE) {
                break;
            }
            assert status == ProtoConnection.Status.MESSAGE;

            // Set the appropriate flags on the RPC object
            // TODO: Handle bad sequence number by ignoring/logging?
            RpcResponse response = builder.build();
            ProtoRpcController rpc = null;
            synchronized (this) {
                rpc = pendingRpcs.remove(response.getSequenceNumber());
                assert response.getStatus() == Protocol.Status.OK;
                assert rpc != null : "No ProtoRpcController for Sequence# " + response.getSequenceNumber();
            }
            rpc.finishRpc(response.getResponse());
        }
    }

    @Override
    public boolean writeCallback(SelectableChannel channel) {
        return connection.writeAvailable();
    }

    public void close() {
        // TODO: Make this non-final and set it to null?
        connection.close();
    }
}
