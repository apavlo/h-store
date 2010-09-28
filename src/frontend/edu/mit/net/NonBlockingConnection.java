package edu.mit.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

/** Provides a non-blocking buffer for both the sending and receiving to/from a SocketChannel. */
public class NonBlockingConnection {
    private final SelectableChannel channel;
    private final NIOReadStream read;
    private final NIOWriteStream write;
    private boolean writeBlocked = false;

    public NonBlockingConnection(SocketChannel channel) {
        this(channel, channel);
        try {
            channel.socket().setTcpNoDelay(true);
            channel.configureBlocking(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Constructor provided mostly for unit tests. */
    public NonBlockingConnection(SelectableChannel selectable, ByteChannel channel) {
        this.channel = selectable;
        read = new NIOReadStream(channel);
        write = new NIOWriteStream(channel);
    }

    /** Returns an OutputStream that buffers output in this connection. */
    public OutputStream getOutputStream() {
        return new ZeroCopyOutputStreamAdaptor(write);
    }

    /** Attempt to write the data out to the buffer, if we can. This will either immediately
     *  perform the write, or it will buffer it.
     *  @return true if this connection blocked and now needs a write callback. */
    public boolean tryFlush() {
        if (!writeBlocked) {
            writeBlocked = write.flush();
            return writeBlocked;
        }

        return false;
    }

    /** Called when the channel can accept more data so we should try writing.
     *  
     * @return true if this connection is still blocked.
     */
    public boolean writeAvailable() {
        assert writeBlocked;
        writeBlocked = write.flush();
        return writeBlocked;
    }

    /** Read up to desired bytes of data into the internal buffer.
     * @return the number of bytes available, or -1 if the connection is closed. */
    public int readAvailable(int desired) {
        int available = read.tryRead(desired);
        return available;
    }

    public int available() {
        return read.dataAvailable();
    }

    public class NonBlockingConnectionInputStream extends InputStream {
        private int limitRemaining = -1;

        public void setLimit(int limit) {
            limitRemaining = limit;
        }

        @Override
        public int read() throws IOException {
            throw new UnsupportedOperationException("TODO: implement");
        }

        @Override
        public int read(byte[] destination, int offset, int length) {
            if (limitRemaining == 0) {
                return -1;
            }

            if (limitRemaining >= 0) {

                if (limitRemaining < length) {
                    length = limitRemaining;
                }
                limitRemaining -= length;
                assert limitRemaining >= 0;
            }
            read.getBytes(destination, offset, length);
            return length;
        }
    }

    /** Returns an OutputStream that buffers output in this connection. */
    public NonBlockingConnectionInputStream getInputStream() {
        return new NonBlockingConnectionInputStream();
    }

    /** Returns the underlying channel for registration with a selector. */
    public SelectableChannel getChannel() {
        return channel;
    }

    public void close() {
        read.close();
    }
}
