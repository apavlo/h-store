package org.voltdb;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool;

import edu.brown.hstore.Hstoreservice.Status;

public class TestClientResponseImpl extends TestCase {

    final DBBPool buffer_pool = new DBBPool(true, false);
    
    ClientResponseImpl cr = null;
    long txn_id = 10001;
    long client_handle = Integer.MAX_VALUE;
    Status status = Status.OK;
    VoltTable results[] = new VoltTable[0];
    String statusString = "Squirrels!";
    
    @Override
    protected void setUp() throws Exception {
        cr = new ClientResponseImpl(txn_id, client_handle, 1, status, results, statusString);
        assertNotNull(cr);
    }
    
    /**
     * testSetRestartCounter
     */
    public void testSetRestartCounter() throws Exception {
        byte[] invocation_bytes = FastSerializer.serialize(cr);
        assertNotNull(invocation_bytes);
        
        for (int i = 0; i < 99; i++) {
            ByteBuffer b = ByteBuffer.wrap(invocation_bytes);
            ClientResponseImpl.setRestartCounter(b, i);
            FastDeserializer fds = new FastDeserializer(invocation_bytes);
            ClientResponseImpl clone = fds.readObject(ClientResponseImpl.class);
            assertNotNull(clone);
            assertEquals(i, clone.getRestartCounter());
        } // FOR
    }
    
    /**
     * testSetBasePartition
     */
    public void testSetBasePartition() throws Exception {
        byte[] invocation_bytes = FastSerializer.serialize(cr);
        assertNotNull(invocation_bytes);
        
        for (int partition : new int[]{ 1, 10, 100}) {
            ByteBuffer b = ByteBuffer.wrap(invocation_bytes);
            ClientResponseImpl.setBasePartition(b, partition);
            FastDeserializer fds = new FastDeserializer(invocation_bytes);
            ClientResponseImpl clone = fds.readObject(ClientResponseImpl.class);
            assertNotNull(clone);
            assertEquals(partition, clone.getBasePartition());
        } // FOR
    }
    
    /**
     * testSetStatus
     */
    public void testSetStatus() throws Exception {
        byte[] invocation_bytes = FastSerializer.serialize(cr);
        assertNotNull(invocation_bytes);
        
        for (Status s : Status.values()) {
            ByteBuffer b = ByteBuffer.wrap(invocation_bytes);
            ClientResponseImpl.setStatus(b, s);
            FastDeserializer fds = new FastDeserializer(invocation_bytes);
            ClientResponseImpl clone = fds.readObject(ClientResponseImpl.class);
            assertNotNull(clone);
            assertEquals(s, clone.getStatus());
        } // FOR
    }
    
}
