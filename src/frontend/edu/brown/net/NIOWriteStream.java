package edu.brown.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class NIOWriteStream implements ZeroCopyOutputStream {
    private final WritableByteChannel channel;
    private final ByteBufferFifo buffers = new ByteBufferFifo();

    public NIOWriteStream(WritableByteChannel channel) {
        this.channel = channel;
    }

    @Override
    public ByteBuffer getNext() {
        return buffers.getWriteBuffer();
    }

    /** @return true if there is more data to write and the write blocked. */
    public boolean flush() {
        ByteBuffer buffer;
        while ((buffer = buffers.getReadBuffer()) != null) {
            try {
                channel.write(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (buffer.remaining() > 0) {
                // write blocked: we are done
                return true;
            }
        }

        return false;
    }

    public void setBigEndian() {
        buffers.setBigEndian();
    }
}
