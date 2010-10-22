package ca.evanjones.protorpc;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.Protocol.RpcRequest;
import ca.evanjones.protorpc.Protocol.RpcResponse;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;

import edu.mit.net.NonBlockingConnection;

public class ProtoRpcChannel extends AbstractEventHandler implements RpcChannel {
    private static final Logger LOG = Logger.getLogger(ProtoRpcChannel.class);
    
    private final EventLoop eventLoop;
    private final ConnectFactory connector;
    private int sequence;
    private ProtoConnection connection;
    private final HashMap<Integer, ProtoRpcController> pendingRpcs =
            new HashMap<Integer, ProtoRpcController>();
    private int reconnectIntervalSeconds;

    /** A factory interface for connecting to an RPC server. */
    public interface ConnectFactory {
        /** Creates a new connection that is connecting. */
        public NonBlockingConnection startNewConnection();
    }

    public ProtoRpcChannel(EventLoop eventLoop, ConnectFactory connector) {
        this.eventLoop = eventLoop;
        this.connector = connector;

        startAsyncConnect();
    }

    private void startAsyncConnect() {
        assert connection == null;
        connection = new ProtoConnection(connector.startNewConnection());
        if (connection.getChannel() != null && !((SocketChannel) connection.getChannel()).isConnected()) { 
            eventLoop.registerConnect((SocketChannel) connection.getChannel(), this);
        } else {
            eventLoop.registerRead(connection.getChannel(), this);
        }
    }

    private static final class NIOConnectFactory implements ConnectFactory {
        private final InetSocketAddress address;

        public NIOConnectFactory(InetSocketAddress address) {
            this.address = address;
        }

        @Override
        public NonBlockingConnection startNewConnection() {
            try {
                SocketChannel socket = SocketChannel.open();
                NonBlockingConnection connection = new NonBlockingConnection(socket);

                // this connect is non-blocking and should always return false.
                boolean finished = ((SocketChannel) connection.getChannel()).connect(address);
                if (finished) {
                    throw new IllegalStateException("async connect finished instantly?");
                }
                return connection;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ProtoRpcChannel(EventLoop eventLoop, InetSocketAddress address) {
        this(eventLoop, new NIOConnectFactory(address));
    }

    private static final class StaticConnectFactory implements ConnectFactory {
        private NonBlockingConnection connection;

        public StaticConnectFactory(NonBlockingConnection connection) {
            this.connection = connection;
        }

        @Override
        public NonBlockingConnection startNewConnection() {
            NonBlockingConnection out = connection;
            connection = null;
            return out;
        }
    }

    /** 
     * Sets the number of seconds to wait before reconnecting, if the connect fails.
     * This permits a channel to be created without the server running.
     * 
     * @param reconnectSeconds number of seconds to wait between reconnect attempts. 0 disables
     *          reconnects (default).
     */
    public void setReconnectInterval(int reconnectSeconds) {
        assert reconnectSeconds >= 0;
        reconnectIntervalSeconds = reconnectSeconds;
    }

    public void callMethod(Descriptors.MethodDescriptor method,
            RpcController controller, Message request,
            Message responsePrototype, RpcCallback<Message> done) {
        ProtoRpcController rpc = (ProtoRpcController) controller;
        rpc.startRpc(eventLoop, responsePrototype.newBuilderForType(), done);
        if (connection == null) {
            // closed connection: fail the RPC
            rpc.finishRpcFailure(Protocol.Status.ERROR_COMMUNICATION, "Connection closed");
            return;
        }

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
            rpc.finishRpcSuccess(response.getResponse());
        }
    }

    @Override
    public void connectCallback(SocketChannel channel) {
        assert channel == connection.getChannel();
        try {
            boolean connected = channel.finishConnect();
            assert connected;
        } catch (ConnectException e) {
            // If the connection failed for some remote reason (timeout, connection refused)
            close();

            // Reconnection disabled: throw the connect exception
            if (reconnectIntervalSeconds == 0) {
                throw new RuntimeException(e);
            }

            assert reconnectIntervalSeconds > 0;
            // We are supposed to reconnect: re-create the connection and schedule a reconnect.
            eventLoop.registerTimer(reconnectIntervalSeconds * 1000, this);
            return;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // register for read events on this connection 
        eventLoop.registerRead(channel, this);

        boolean blocked = connection.writeAvailable();
        if (blocked) {
            eventLoop.registerWrite(connection.getChannel(), this);
        }
    }

    @Override
    public boolean writeCallback(SelectableChannel channel) {
        return connection.writeAvailable();
    }

    @Override
    public void timerCallback() {
        assert reconnectIntervalSeconds > 0;
        startAsyncConnect();
    }

    public void close() {
        if (connection == null) throw new IllegalStateException("connection closed");
        connection.close();
        connection = null;
        
        // Fail all pending RPCs
        for (ProtoRpcController rpc : pendingRpcs.values()) {
            // TODO: Define constants shared between C++ and Java?
            rpc.finishRpcFailure(Protocol.Status.ERROR_COMMUNICATION, "Connection closed");
        }
        pendingRpcs.clear();
    }

    private static final int RECONNECT_TIMEOUT_MS = 5000;
    private static final int TOTAL_CONNECT_TIMEOUT_MS = 30000;
    public static ProtoRpcChannel[] connectParallel(final EventLoop eventLoop,
            final InetSocketAddress[] addresses) {
        class ExitLoopHandler extends AbstractEventHandler {
            @Override
            public void timerCallback() {
                if (barrierCount == 0) {
                    if (LOG.isDebugEnabled()) LOG.debug("Timer callback; all connections done");
                } else {
                    ((NIOEventLoop) eventLoop).exitLoop();
                }
            }

            public void connectFinished() {
                barrierCount -= 1;
                assert barrierCount >= 0;
                if (barrierCount == 0) ((NIOEventLoop) eventLoop).exitLoop(); 
            }

            private int barrierCount = addresses.length;
        }
        final ExitLoopHandler exitLoopHandler = new ExitLoopHandler();
    
        class ConnectHandler extends AbstractEventHandler {
            public ConnectHandler(int index) {
                this.index = index;
                startConnect();
            }

            private void startConnect() {
                try {
                    channel = SocketChannel.open();
                    channel.configureBlocking(false);

                    // this connect is non-blocking and should always return false.
                    boolean finished = channel.connect(addresses[index]);
                    if (finished) {
                        throw new IllegalStateException("async connect finished instantly?");
                    }

                    eventLoop.registerConnect(channel, this);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void connectCallback(SocketChannel channel) {
                try {
                    boolean finished = channel.finishConnect();
                    assert finished;

                    exitLoopHandler.connectFinished();
                } catch (ConnectException e) {
                    // Some connection error occurred: retry after a timeout
                    channel = null;
                    eventLoop.registerTimer(RECONNECT_TIMEOUT_MS, this);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void timerCallback() {
                // reattempt the connection
                startConnect();
            }

            final int index;
            SocketChannel channel;
        }

        ConnectHandler[] channels = new ConnectHandler[addresses.length];
        for (int i = 0; i < channels.length; ++i) {
            channels[i] = new ConnectHandler(i);
        }

        eventLoop.registerTimer(TOTAL_CONNECT_TIMEOUT_MS, exitLoopHandler);
        eventLoop.run();

        if (exitLoopHandler.barrierCount == 0) {
            ProtoRpcChannel[] rpcChannels = new ProtoRpcChannel[addresses.length];
            for (int i = 0; i < channels.length; ++i) {
                rpcChannels[i] = new ProtoRpcChannel(eventLoop,
                        new StaticConnectFactory(new NonBlockingConnection(channels[i].channel)));
            }
            return rpcChannels;
        } else {
            // Close any open channels in case connects are pending
            for (ConnectHandler connectHandler : channels) {
                if (connectHandler.channel == null) {
                    try {
                        connectHandler.channel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            throw new RuntimeException("some connection failed after " + TOTAL_CONNECT_TIMEOUT_MS / 1000 + " seconds");
        }

    }
}
