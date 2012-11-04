package edu.brown.protorpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public final class MockServerSocketChannel extends ServerSocketChannel {
    public MockServerSocketChannel() {
        super(null);
    }
    
    @Override
    public SocketChannel accept() throws IOException {
        if (nextAccept == null) {
            throw new IllegalStateException("no channel to accept()");
        }
        SocketChannel result = nextAccept;
        nextAccept = null;
        return result;
    }

    @Override
    public ServerSocket socket() {
        throw new UnsupportedOperationException("unimplemented stub");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        throw new UnsupportedOperationException("unimplemented stub");
    }

    @Override
    protected void implConfigureBlocking(boolean arg0) throws IOException {
        throw new UnsupportedOperationException("unimplemented stub");
    }

    public SocketChannel nextAccept;
    
    public <T> ServerSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        return (null);
    }
}
