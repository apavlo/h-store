package org.voltdb;

import java.nio.ByteBuffer;

import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool;

import junit.framework.TestCase;

public class TestClientResponseImpl extends TestCase {

    final DBBPool buffer_pool = new DBBPool(true, false);
    
    ClientResponseImpl cr = null;
    long txn_id = 10001;
    byte status = ClientResponse.SUCCESS;
    VoltTable results[] = new VoltTable[0];
    String statusString = "Squirrels!";
    
    @Override
    protected void setUp() throws Exception {
        cr = new ClientResponseImpl(txn_id, status, results, statusString);
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
