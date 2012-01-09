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

public class MockMessageConnection implements MessageConnection {
    public byte[] tryRead() {
        assert !closed;
        byte[] result = nextRead;
        nextRead = null;
        return result;
    }

    public boolean write(byte[] message) {
        assert !closed;
        assert message.length > 0;  // zero length messages are prohibited
        lastWrite = message;
        return writeBlocked;
    }

    public boolean tryWrite() {
        tryWriteCalled = true;
        return writeBlocked;
    }

    public SelectionKey register(Selector selector) { assert false; return null; }

    @Override
    public SelectableChannel getChannel() { return null; }

    public boolean isOpen() { return !closed; }

    public void close() {
        assert !closed;
        closed = true;
    }

    public byte[] nextRead = null;
    public byte[] lastWrite = null;
    public boolean writeBlocked = false;
    public boolean tryWriteCalled = false;

    private boolean closed = false;
}
