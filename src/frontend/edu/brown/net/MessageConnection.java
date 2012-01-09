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

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/** Sends and receives blocks of bytes with a non-blocking interface. This interface does not use
checked exceptions. It re-throws any exceptions wrapped in a RuntimeException. */
public interface MessageConnection {
    /** Returns a message if one is available. */
    public byte[] tryRead();

    /** Writes message to the channel. If the underlying channel would block, this will require
    future calls to tryWrite() when it becomes available again. See register().
    @return true if the write blocks and more data needs to be written. */
    public boolean write(byte[] message);

    /** Attempts to write any buffered data out the connection.
    @return true if the write blocks and more data needs to be written. */
    public boolean tryWrite();

    /** Registers the channel's read and write events with selector. On a read, call tryRead(). On
    a write, call tryWrite(). */
    public SelectionKey register(Selector selector);

    /** Returns the underlying channel for registration with a selector. */
    // TODO: Remove register()?
    public SelectableChannel getChannel();

    /** Closes the underlying channel. */
    public void close();
}
