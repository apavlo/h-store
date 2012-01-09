/*
Copyright (c) 2008
Evan Jones
Massachusetts Institute of Technology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package edu.brown.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
Provides a non-blocking stream-like interface on top of the Java NIO ReadableByteChannel. It calls
the underlying read() method only when needed.
*/
public class NIOReadStream {
    NIOReadStream(ReadableByteChannel channel) {
        this.channel = channel;
    }

    /** @returns the number of bytes available to be read. */
    public int dataAvailable() {
        return totalAvailable;
    }

    public int getInt() {
        // TODO: Optimize?
        byte[] intbytes = new byte[4];
        getBytes(intbytes);
        int output = (((int)intbytes[0]) & 0xff);
        output |= (((int)intbytes[1]) & 0xff) << 8;
        output |= (((int)intbytes[2]) & 0xff) << 16;
        output |= (((int)intbytes[3]) & 0xff) << 24;
        return output;
    }

    public int getIntBigEndian() {
        // TODO: Optimize?
        byte[] intbytes = new byte[4];
        getBytes(intbytes);
        int output = (((int)intbytes[0]) & 0xff) << 24;
        output |= (((int) intbytes[1]) & 0xff) << 16; 
        output |= (((int) intbytes[2]) & 0xff) << 8; 
        output |= (((int) intbytes[3]) & 0xff); 
        return output;
    }

    public void getBytes(byte[] output) {
        getBytes(output, 0, output.length);
    }

    public void getBytes(byte[] output, int offset, int length) {
        if (totalAvailable < length) {
            throw new IllegalStateException("Requested " + length + " bytes; only have "
                    + totalAvailable + " bytes; call tryRead() first");
        }

        int originalOffset = offset;
        while (length > 0) {
            ByteBuffer first = buffers.getReadBuffer();
            assert first.remaining() > 0;

            // Copy bytes from first into output
            int bytesToCopy = length;
            if (first.remaining() < bytesToCopy) bytesToCopy = first.remaining();
            first.get(output, offset, bytesToCopy);
            offset += bytesToCopy;
            length -= bytesToCopy;
            assert offset <= output.length;
            assert length >= 0;
        }
        int bytesCopied = offset - originalOffset;
        totalAvailable -= bytesCopied;
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) { throw new RuntimeException(e); }
        buffers.clear();
    }

    /** Reads all available data from input. Returns true if data was read or the connection is
     * still open. False indicates no data was read and the connection is closed.
     * 
     * This allows the application an opportunity to consume the last bytes in the stream, without
     * getting stuck if the connection is closed with a partial message.
    */
    // TODO: Remove tryRead()?
    public boolean readAllAvailable() {
        // Read until it doesn't fill the buffer: indicates no more data available
        boolean first = true;
        int lastRead = -1;
        while (true) {
            ByteBuffer writeBuffer = buffers.getWriteBuffer();
            assert writeBuffer.remaining() > 0;

            try {
                lastRead = channel.read(writeBuffer);
            } catch (IOException e) { throw new RuntimeException(e); }

            if (lastRead > 0) {
                totalAvailable += lastRead;
            }

            if (writeBuffer.remaining() > 0) {
                break;
            }
            assert lastRead > 0;
            first = false;
        }

        if (lastRead == -1 && first) {
            // first read returned -1: connection is closed and buffer probably has incomplete data 
            return false;
        }

        return true;
    }
    
    /** Reads until we have at least desiredAvailable bytes buffered, there is no more data, or
    the channel is closed.
    * @returns number of bytes available for reading, or -1 if less than desiredAvailable bytes
    are available, and the channel is closed. */
    public int tryRead(int desiredAvailable) {
        // Read until we have enough, or read returns 0 or -1
        int lastRead = 1;
        while (lastRead > 0 && totalAvailable < desiredAvailable) {
            ByteBuffer writeBuffer = buffers.getWriteBuffer();
            assert writeBuffer.remaining() > 0;

            try {
                lastRead = channel.read(writeBuffer);
            } catch (IOException e) { throw new RuntimeException(e); }
            if (lastRead > 0) {
                totalAvailable += lastRead;
            }
        }

        if (totalAvailable < desiredAvailable && lastRead == -1) {
            return -1;
        }
        return totalAvailable;
    }

    private final ReadableByteChannel channel;
    private final ByteBufferFifo buffers = new ByteBufferFifo();
    private int totalAvailable = 0;
}
