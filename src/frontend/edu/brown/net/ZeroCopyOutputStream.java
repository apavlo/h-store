package edu.brown.net;

import java.nio.ByteBuffer;

/** Inspired by the Google Protocol Buffer's C++ implementation. */
public interface ZeroCopyOutputStream {
    /** Returns the next buffer to be filled with data. For best performance when writing to
     * network interfaces, this will be a direct ByteBuffer. Do not rely on the array() method. */
    public ByteBuffer getNext();
}
