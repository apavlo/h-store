package edu.brown.net;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ZeroCopyOutputStreamAdaptor extends OutputStream {
    private final ZeroCopyOutputStream stream;

    public ZeroCopyOutputStreamAdaptor(ZeroCopyOutputStream stream) {
        this.stream = stream;
    }

    @Override
    public void write(int outputByte) throws IOException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public void write(byte[] source, int offset, int length) {
        while (length > 0) {
            ByteBuffer next = stream.getNext();
            assert next.remaining() > 0;

            int bytesToWrite = length;
            if (next.remaining() < bytesToWrite) bytesToWrite = next.remaining();
            next.put(source, offset, bytesToWrite);
            offset += bytesToWrite;
            length -= bytesToWrite;
            assert offset <= source.length;
            assert length >= 0;
        }
    }
}
