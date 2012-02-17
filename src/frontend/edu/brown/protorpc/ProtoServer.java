package edu.brown.protorpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.Protocol;
import ca.evanjones.protorpc.Protocol.RpcRequest;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import edu.brown.net.NonBlockingConnection;

public class ProtoServer extends AbstractEventHandler {
    private static final Logger LOG = Logger.getLogger(ProtoServer.class);
    
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
        ProtoConnection connection = new ProtoConnection(new NonBlockingConnection(client));

        eventLoop.registerRead(client, new EventCallbackWrapper(connection));
//        SelectionKey clientKey = connection.register(selector);
//        clientKey.attach(connection);
//        eventQueue.add(new Event(connection, null));
    }

    private class EventCallbackWrapper extends AbstractEventHandler {
        public EventCallbackWrapper(ProtoConnection connection) {
            this.connection = connection;
        }

        @Override
        public void readCallback(SelectableChannel channel) {
            read(this);
        }

        @Override
        public synchronized boolean writeCallback(SelectableChannel channel) {
            return connection.writeAvailable();
        }

        private final ProtoConnection connection;

        public synchronized void writeResponse(RpcResponse output) {
            boolean blocked = connection.tryWrite(output);
            if (blocked) {
                // write blocked: wait for the write callback
                eventLoop.registerWrite(connection.getChannel(), this);
            }
        }
    }

    private void read(EventCallbackWrapper eventLoopCallback) {
        boolean isOpen = eventLoopCallback.connection.readAllAvailable();
        if (!isOpen) {
            // connection closed
            LOG.debug("Connection closed");
            eventLoopCallback.connection.close();
            return;
        }

        while (true) {
            RpcRequest.Builder requestBuilder = RpcRequest.newBuilder();
            boolean hasMessage = eventLoopCallback.connection.readBufferedMessage(requestBuilder);
            if (!hasMessage) {
                break;
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

            eventLoopCallback.writeResponse(responseMessage.build());
            eventLoopCallback = null;
        }
    }

    public void bind(int port) {
        try {
            serverSocket = ServerSocketChannel.open();
            // Avoid TIME_WAIT when killing the server
            serverSocket.socket().setReuseAddress(true);
            // Mac OS X: bind() before calling Selector.register or you don't get accept() events
            serverSocket.socket().bind(new InetSocketAddress(port));
            eventLoop.registerAccept(serverSocket, this);
        } catch (IOException e) { throw new RuntimeException("Failed to bind socket on port #" + port, e); }
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
