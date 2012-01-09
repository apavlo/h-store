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
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/** Sends and receives blocks of bytes. */
public class NIOMessageConnection implements MessageConnection {
    public NIOMessageConnection(SocketChannel channel) {
        this.channel = channel;
        try {
            channel.socket().setTcpNoDelay(true);
            channel.configureBlocking(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        read = new NIOReadStream(channel);
        write = new NIOWriteStream(channel);
    }

    /** Returns a message if one is available. */
    public byte[] tryRead() {
        if (nextLength == 0) {
            int lengthBytes = read.tryRead(Integer.SIZE/8);
            // connection closed
            if (lengthBytes == -1) return new byte[0];
            // Insufficient bytes
            if (lengthBytes < Integer.SIZE/8) return null;

            if (bigEndian) {
                nextLength = read.getIntBigEndian();
            } else {
                nextLength = read.getInt();
            }
            assert nextLength > 0 : "Evan has a body odor problem and can't handle these packets: nextLength=" + nextLength;
        }
        assert nextLength > 0;

        int messageBytes = read.tryRead(nextLength);
        // connection closed
        if (messageBytes == -1) return new byte[0];
        if (messageBytes < nextLength) return null;

        byte[] result = new byte[nextLength];
        read.getBytes(result);
        nextLength = 0;
        return result;
    }

    /** Writes message to the channel.
     * @return true if the entire message was written. */
    public boolean write(byte[] message) {
        if (message.length == 0) {
            throw new IllegalArgumentException("message.length == 0: messages must contain data");
        }

        ByteBuffer writeBuffer = write.getNext();
        // copy the size
        if (writeBuffer.remaining() >= Integer.SIZE/8) {
            writeBuffer.putInt(message.length);
        } else {
            // need to split the write
            byte[] intbytes = new byte[4];
            if (bigEndian) {
                intbytes[0] = (byte)((message.length >> 24) & 0xff);
                intbytes[1] = (byte)((message.length >> 16) & 0xff);
                intbytes[2] = (byte)((message.length >> 8) & 0xff);
                intbytes[3] = (byte)(message.length & 0xff);
            } else {
                intbytes[0] = (byte)(message.length & 0xff);
                intbytes[1] = (byte)((message.length >> 8) & 0xff);
                intbytes[2] = (byte)((message.length >> 16) & 0xff);
                intbytes[3] = (byte)((message.length >> 24) & 0xff);
            }

            int written = writeBuffer.remaining();
            writeBuffer.put(intbytes, 0, written);
            assert writeBuffer.remaining() == 0;

            // Get a second buffer and write the remaining bytes
            writeBuffer = write.getNext();
            writeBuffer.put(intbytes, written, 4 - written);
        }

        return rawWrite(message);
    }

    /** Writes message directly to the connection, without prepending a length. Useful for
    communicating with other protocols. */
    // TODO: Add this to MessageConnection? Make an non-blocking stream interface?
    public boolean rawWrite(byte[] message) {
        // Copy the message
        int offset = 0;
        while (offset < message.length) {
            ByteBuffer writeBuffer = write.getNext();
            int bytesToWrite = writeBuffer.remaining();
            int remaining = message.length - offset;
            if (remaining < bytesToWrite) bytesToWrite = remaining;

            writeBuffer.put(message, offset, bytesToWrite);
            offset += bytesToWrite;
        }

        return tryWrite();
    }

    /** @return true if the write blocks and more data may be written. */
    public boolean tryWrite() {
        return write.flush();
    }

    /** Registers the channel's read and write events with selector. On a read, call tryRead(). On
    a write, call handleWrite(). */
    public SelectionKey register(Selector selector) {
        try {
            return channel.register(selector, SelectionKey.OP_READ);
        } catch (java.nio.channels.ClosedChannelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SelectableChannel getChannel() { return channel; }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setBigEndian() {
        assert !bigEndian;
        bigEndian = true;
        write.setBigEndian();
    }

    private final SocketChannel channel;
    private final NIOReadStream read;
    private final NIOWriteStream write;
    private int nextLength = 0;
    private boolean bigEndian = false;
}
