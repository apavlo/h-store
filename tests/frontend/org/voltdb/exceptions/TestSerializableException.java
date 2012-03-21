package org.voltdb.exceptions;

import java.nio.ByteBuffer;

import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

import com.google.protobuf.ByteString;

import junit.framework.TestCase;

public class TestSerializableException extends TestCase {

    public void testSerializeToBuffer() throws Exception {
        SerializableException error = null;
        try {
            throw new NullPointerException();
        } catch (Exception ex) {
            error = new SerializableException(ex);
        }
        assertNotNull(error);
        
        int size = error.getSerializedSize();
        ByteBuffer b = ByteBuffer.allocate(size);
        error.serializeToBuffer(b);
        b.rewind();
        int expected = b.getInt();
        assertTrue(expected > 0);
        assertTrue(expected <= size);
        b.rewind();

        // Make sure we can still do this with a BufferPool
        DBBPool buffer_pool = new DBBPool(false, false);
        BBContainer bc = buffer_pool.acquire(size);
        error.serializeToBuffer(bc.b);
        bc.b.rewind();
        assertEquals(expected, bc.b.getInt());
        bc.b.rewind();
        
        ByteString bs = ByteString.copyFrom(b);
        ByteBuffer bs_b = bs.asReadOnlyByteBuffer();
//        System.err.println("NEW: " + StringUtil.md5sum(b));
        
        assertEquals(expected, bs_b.getInt());
    }
    
}
