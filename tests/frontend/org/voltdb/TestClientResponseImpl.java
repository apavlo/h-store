package org.voltdb;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool;

import edu.brown.hstore.Hstore;

public class TestClientResponseImpl extends TestCase {

    final DBBPool buffer_pool = new DBBPool(true, false);
    
    ClientResponseImpl cr = null;
    long txn_id = 10001;
    long client_handle = Integer.MAX_VALUE;
    Hstore.Status status = Hstore.Status.OK;
    VoltTable results[] = new VoltTable[0];
    String statusString = "Squirrels!";
    
    @Override
    protected void setUp() throws Exception {
        cr = new ClientResponseImpl(txn_id, client_handle, 1, status, results, statusString);
        assertNotNull(cr);
    }
    
    /**
     * testSetServerTimestamp
     */
    public void testSetServerTimestamp() throws Exception {
        byte[] invocation_bytes = FastSerializer.serialize(cr);
        assertNotNull(invocation_bytes);
        
        for (int i = -10; i < 99; i++) {
            ByteBuffer b = ByteBuffer.wrap(invocation_bytes);
            ClientResponseImpl.setServerTimestamp(b, i);
            FastDeserializer fds = new FastDeserializer(invocation_bytes);
            ClientResponseImpl clone = fds.readObject(ClientResponseImpl.class);
            assertNotNull(clone);
            assertEquals(i, clone.getServerTimestamp());
        } // FOR
    }
    
    /**
     * testSetThrottleFlag
     */
    public void testSetThrottleFlag() throws Exception {
        byte[] invocation_bytes = FastSerializer.serialize(cr);
        assertNotNull(invocation_bytes);
        
        for (boolean throttle : new boolean[] { true, false, true, false, false, true }) {
            ByteBuffer b = ByteBuffer.wrap(invocation_bytes);
            ClientResponseImpl.setThrottleFlag(b, throttle);
            FastDeserializer fds = new FastDeserializer(invocation_bytes);
            ClientResponseImpl clone = fds.readObject(ClientResponseImpl.class);
            assertNotNull(clone);
            assertEquals(throttle, clone.getThrottleFlag());
        } // FOR
    }
    
}
