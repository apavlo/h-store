package ca.evanjones.protorpc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class MockSocketChannel extends SocketChannel {
    public MockSocketChannel() {
        super(null);
    }

    @Override
    public boolean connect(SocketAddress arg0) throws IOException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public boolean finishConnect() throws IOException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public boolean isConnected() {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public boolean isConnectionPending() {
        throw new UnsupportedOperationException("TODO: implement");
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
    public long read(ByteBuffer[] arg0, int arg1, int arg2)
            throws IOException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public Socket socket() {
        return new Socket();
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        // TODO: Mostly copied from NIOWriteStreamTest. Combine?
        assert buffer.remaining() > 0;
        // Figure out how many bytes to take
        int bytesToAccept = numBytesToAccept;
        if (bytesToAccept == -1) {
            bytesToAccept = buffer.remaining();
        } else {
            if (buffer.remaining() < bytesToAccept) {
                bytesToAccept = buffer.remaining();
            }
            numBytesToAccept -= bytesToAccept;
        }

        lastWrite = new byte[bytesToAccept];
        buffer.get(lastWrite);
        return bytesToAccept;
    }

    @Override
    public long write(ByteBuffer[] arg0, int arg1, int arg2)
            throws IOException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    protected void implConfigureBlocking(boolean block) throws IOException {
        assert !block;
    }

    public byte[] nextRead;
    public byte[] lastWrite;
    public int numBytesToAccept = -1;
}
