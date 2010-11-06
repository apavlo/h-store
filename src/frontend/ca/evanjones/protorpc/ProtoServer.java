package ca.evanjones.protorpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import ca.evanjones.protorpc.Protocol.RpcRequest;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import edu.mit.net.MessageConnection;
import edu.mit.net.NIOMessageConnection;

public class ProtoServer extends AbstractEventHandler {
    public ProtoServer(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public void acceptCallback(SelectableChannel channel) {
        // accept the connection
        assert channel == serverSocket;
        SocketChannel client;
        try {
            client = serverSocket.accept();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assert client != null;

        // wrap it in a message connection and register with event loop
        MessageConnection connection = new NIOMessageConnection(client);

        eventLoop.registerRead(client, new EventCallbackWrapper(connection));
//        SelectionKey clientKey = connection.register(selector);
//        clientKey.attach(connection);
//        eventQueue.add(new Event(connection, null));
    }

    private class EventCallbackWrapper extends AbstractEventHandler {
        public EventCallbackWrapper(MessageConnection connection) {
            this.connection = connection;
        }

        @Override
        public void readCallback(SelectableChannel channel) {
            read(this);
        }

        @Override
        public boolean writeCallback(SelectableChannel channel) {
            return connection.tryWrite();
        }

        public void registerWrite() {
            eventLoop.registerWrite(connection.getChannel(), this);
        }

        private final MessageConnection connection;
    }

    private void read(EventCallbackWrapper eventLoopCallback) {
        byte[] output;
        while ((output = eventLoopCallback.connection.tryRead()) != null) {
            if (output.length == 0) {
                // connection closed
                System.out.println("connection closed");
                eventLoopCallback.connection.close();
                return;
            }

            // Parse the request
            RpcRequest.Builder requestBuilder;
            try {
                requestBuilder = RpcRequest.newBuilder().mergeFrom(output);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            RpcRequest request = requestBuilder.build();
    //        System.out.println(request.getMethodName() + " " + request.getRequest().size());

            // Handle the request
            ProtoMethodInvoker invoker = serviceRegistry.getInvoker(request.getMethodName());
            // TODO: Reuse callback objects?
            ProtoServerCallback callback =
                    new ProtoServerCallback(eventLoopCallback, request.getSequenceNumber());
            try {
                invoker.invoke(callback.controller, request.getRequest(), callback);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class ProtoServerController implements RpcController {
        @Override
        public String errorText() {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public boolean failed() {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public boolean isCanceled() {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public void notifyOnCancel(RpcCallback<Object> callback) {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public void setFailed(String reason) {
            if (reason == null) {
                throw new NullPointerException("reason parameter must not be null");
            }

            if (status != Protocol.Status.OK) {
                throw new IllegalStateException("RPC already failed or returned");
            }
            assert errorReason == null;
            status = Protocol.Status.ERROR_USER;
            errorReason = reason;
        }

        @Override
        public void startCancel() {
            throw new UnsupportedOperationException("TODO: implement");
        }

        private Protocol.Status status = Protocol.Status.OK;
        private String errorReason;
    }

    private static final class ProtoServerCallback implements RpcCallback<Message> {
        private final ProtoServerController controller = new ProtoServerController();
        private EventCallbackWrapper eventLoopCallback;
        private final int sequence;

        public ProtoServerCallback(EventCallbackWrapper eventLoopCallback, int sequence) {
            this.eventLoopCallback = eventLoopCallback;
            this.sequence = sequence;
            assert this.eventLoopCallback != null;
        }

        @Override
        public void run(Message response) {
            if (eventLoopCallback == null) {
                throw new IllegalStateException("response callback must only be called once");
            }

            RpcResponse.Builder responseMessage = RpcResponse.newBuilder();
            responseMessage.setSequenceNumber(sequence);
            assert controller.status != Protocol.Status.INVALID;
            responseMessage.setStatus(controller.status);
            if (response != null) {
                responseMessage.setResponse(response.toByteString());
            } else {
                // No message: we must have failed
                assert controller.status != Protocol.Status.OK;
            }
            if (controller.errorReason != null) {
                assert controller.status != Protocol.Status.OK;
                responseMessage.setErrorReason(controller.errorReason);
            }
            byte[] output = responseMessage.build().toByteArray();
            // TODO: Rethink the thread safety carefully. Maybe this should be a method on
            // eventLoopCallback?
            synchronized (eventLoopCallback) {
                boolean blocked = eventLoopCallback.connection.write(output);
                if (blocked) {
                    // write blocked: wait for the write callback
                    eventLoopCallback.registerWrite();
                }
            }
            eventLoopCallback = null;
        }
    }

    public void bind(int port) {
        try {
            serverSocket = ServerSocketChannel.open();
            // Mac OS X: Must bind() before calling Selector.register, or you don't get accept() events
            serverSocket.socket().bind(new InetSocketAddress(port));
            eventLoop.registerAccept(serverSocket, this);
        } catch (IOException e) { throw new RuntimeException(e); }
    }
    
    public void close() {
        try {
            serverSocket.close();
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    public void setServerSocketForTest(ServerSocketChannel serverSocket) {
        this.serverSocket = serverSocket;
    }

    public void register(Service service) {
        serviceRegistry.register(service);
    }

    private EventLoop eventLoop;
    private ServerSocketChannel serverSocket;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
}
