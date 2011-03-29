package org.voltdb;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestStoredProcedureInvocation extends BaseTestCase {

    private static final String TARGET_PROCEDURE = "GetNewDestination";
    private static final long CLIENT_HANDLE = 1l;

    private static final Object PARAMS[] = {
        0l, // S_ID
        1l, // SF_TYPE
        2l, // START_TIME
        3l, // END_TIME
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }

    /**
     * testMarkRawBytesAsRedirected
     */
    public void testMarkRawBytesAsRedirected() throws Exception {
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        assertFalse(invocation.hasBasePartition());
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        for (int partition = 0; partition < 100; partition+=3) {
            StoredProcedureInvocation.markRawBytesAsRedirected(partition, invocation_bytes);
            FastDeserializer fds = new FastDeserializer(invocation_bytes);
            StoredProcedureInvocation clone = fds.readObject(StoredProcedureInvocation.class);
            assertNotNull(clone);
            assert(clone.hasBasePartition());
            assertEquals(partition, clone.getBasePartition());
        } // FOR
    }
    
    /**
     * testSerialization
     */
    public void testSerialization() throws Exception {
        // Try with referencing the params directly
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE,
                                    PARAMS[0], PARAMS[1], PARAMS[2], PARAMS[3]);
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        // Try with referencing the params directly
        invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
    }
    
    /**
     * testDeserialization
     */
    public void testDeserialization() throws Exception {
        // Try with referencing the params directly
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        final Set<Integer> partitions = new HashSet<Integer>();
        partitions.add(19);
        partitions.add(85);
        partitions.add(-1);
        invocation.addPartitions(partitions);
        
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);

        // Let 'er rip!
        FastDeserializer fds = new FastDeserializer(invocation_bytes);
        StoredProcedureInvocation clone = fds.readObject(StoredProcedureInvocation.class);
        assertNotNull(clone);
        clone.buildParameterSet();
        
        assertEquals(invocation.getClientHandle(), clone.getClientHandle());
        assertEquals(invocation.getProcName(), clone.getProcName());
        assertNotNull(clone.getParams());
        assertArrayEquals(invocation.getParams().toArray(), clone.getParams().toArray());
        assert(clone.hasPartitions());
        assertEquals(partitions.size(), clone.getPartitions().size());
        assert(partitions.containsAll(clone.getPartitions()));
        
    }
}
