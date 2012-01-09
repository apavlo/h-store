package edu.brown.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.ArrayList;

public class MockByteChannel implements ByteChannel {
    private ByteBuffer nextRead;
    public boolean end = false;
    public boolean closed = false;

    public final ArrayList<byte[]> lastWrites = new ArrayList<byte[]>();
    public boolean writeCalled = false;
    public int numBytesToAccept = -1;

    public void setNextRead(byte[] data) {
        nextRead = ByteBuffer.wrap(data);
    }

    public byte[] dequeueWrite() {
        byte[] out = lastWrites.get(0);
        lastWrites.remove(0);
        return out;
    }

    public int read(ByteBuffer buffer) {
        if (nextRead == null) {
            if (end) return -1;
            return 0;
        }

        int copied = 0;
        if (nextRead.remaining() > buffer.remaining()) {
            // limited copy
            copied = buffer.remaining();
            assert nextRead.limit() == nextRead.capacity();
            nextRead.limit(nextRead.position() + copied);
            assert nextRead.remaining() == buffer.remaining();
            buffer.put(nextRead);
            assert buffer.remaining() == 0;
            nextRead.limit(nextRead.capacity());
            assert nextRead.remaining() > 0;
        } else {
            copied = nextRead.remaining();
            buffer.put(nextRead);
            assert nextRead.remaining() == 0;
            nextRead = null;
        }
        return copied;
    }

    @Override
    public boolean isOpen() { return !closed; }

    @Override
    public void close() {
        assert !closed;
        closed = true;
        end = true;
    }
    
    @Override
    public int write(ByteBuffer buffer) throws IOException {
        assert buffer.remaining() > 0;
        writeCalled = true;

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

        byte[] lastWrite = new byte[bytesToAccept];
        buffer.get(lastWrite);
        lastWrites.add(lastWrite);
        return bytesToAccept;
    }

    public void clear() {
        lastWrites.clear();
        writeCalled = false;
    }
}
