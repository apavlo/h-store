package edu.brown.net;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SocketChannel;

public class MockSocketChannel extends SocketChannel {
    public enum State {
        UNCONNECTED,
        CONNECTING,
        CONNECTED,
        CLOSED,
    };

    public State state = State.UNCONNECTED;
    public byte[] nextRead;
    public final MockByteChannel writeChannel = new MockByteChannel();

    public void setConnected() {
        state = State.CONNECTED;
    }

    public void setConnecting() {
        state = State.CONNECTING;
    }

    public MockSocketChannel() {
        super(null);
    }

    @Override
    public Socket socket() {
        // TODO Auto-generated method stub
        return new Socket();
    }

    @Override
    public boolean isConnected() {
        return state == State.CONNECTED;
    }

    @Override
    public boolean isConnectionPending() {
        return state == State.CONNECTING;
    }

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        // TODO Auto-generated method stub
        // return false;
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (state == State.CLOSED) {
            throw new ConnectException("Connection refused");
        } else if (state == State.CONNECTED) {
            throw new IllegalStateException();
        }
        assert state == State.CONNECTING || state == State.UNCONNECTED;
        state = State.CONNECTED;
        return true;
    }

    @Override
    public int read(ByteBuffer destination) throws IOException {
        if (nextRead == null) {
            return 0;
        }

        destination.put(nextRead);
        int bytes = nextRead.length;
        nextRead = null;
        return bytes;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException {
        // TODO Auto-generated method stub
        // return 0;
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (state == State.CLOSED) {
            throw new ClosedChannelException();
        } else if (state == State.CONNECTING || state == State.UNCONNECTED) {
            throw new NotYetConnectedException();
        }
        assert state == State.CONNECTED;
        return writeChannel.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        // TODO Auto-generated method stub
        // return 0;
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        state = State.CLOSED;
    }

    @Override
    protected void implConfigureBlocking(boolean block) throws IOException {
        // do nothing
    }
    
    public SocketAddress getRemoteAddress() throws IOException {
        return (null);
    }
    public SocketChannel shutdownInput() throws IOException {
        return (null);
    }
    public SocketChannel shutdownOutput() throws IOException {
        return (null);
    }
}
