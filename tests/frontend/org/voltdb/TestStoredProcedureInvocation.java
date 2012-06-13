package org.voltdb;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.utils.ProjectType;

public class TestStoredProcedureInvocation extends BaseTestCase {

    private static final String TARGET_PROCEDURE = GetNewDestination.class.getSimpleName();
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
     * testProcedureId
     */
    public void testProcedureId() throws Exception {
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, "@DatabaseDump", PARAMS);
        invocation.setProcedureId(catalog_proc.getId());
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        ByteBuffer buffer = ByteBuffer.wrap(invocation_bytes);
        assertEquals(catalog_proc.getId(), StoredProcedureInvocation.getProcedureId(buffer));
    }
//    
//    /**
//     * testIsSysProc
//     */
//    public void testIsSysProc() throws Exception {
//        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, "@DatabaseDump", PARAMS);
//        byte[] invocation_bytes = FastSerializer.serialize(invocation);
//        assertNotNull(invocation_bytes);
//        
//        ByteBuffer buffer = ByteBuffer.wrap(invocation_bytes);
//        boolean sysproc = StoredProcedureInvocation.isSysProc(buffer);
//        assertEquals(true, sysproc);
//    }

    /**
     * testGetProcedureName
     */
    public void testGetProcedureName() throws Exception {
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        ByteBuffer buffer = ByteBuffer.wrap(invocation_bytes);
        String proc_name = StoredProcedureInvocation.getProcedureName(buffer);
        assertEquals(TARGET_PROCEDURE, proc_name);
    }
    
    /**
     * testGetClientHandle
     */
    public void testGetClientHandle() throws Exception {
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        ByteBuffer buffer = ByteBuffer.wrap(invocation_bytes);
        long handle = StoredProcedureInvocation.getClientHandle(buffer);
        assertEquals(CLIENT_HANDLE, handle);
    }
    
    /**
     * testGetBasePartition
     */
    public void testGetBasePartition() throws Exception {
        int expected = 25;
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        invocation.setBasePartition(expected);
        assert(invocation.hasBasePartition());
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        ByteBuffer buffer = ByteBuffer.wrap(invocation_bytes);
        int partition = StoredProcedureInvocation.getBasePartition(buffer);
        assertEquals(expected, partition);
    }
    
    /**
     * testGetParameterSet
     */
    public void testGetParameterSet() throws Exception {
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE,
                                                                             TARGET_PROCEDURE,
                                                                             PARAMS);
        byte[] invocation_bytes = FastSerializer.serialize(invocation);
        assertNotNull(invocation_bytes);
        
        ByteBuffer buffer = ByteBuffer.wrap(invocation_bytes);
        ByteBuffer paramsBuffer = StoredProcedureInvocation.getParameterSet(buffer);
        assertNotNull(paramsBuffer);
        
        ParameterSet cloneParams = new ParameterSet();
        FastDeserializer fds = new FastDeserializer(paramsBuffer);
        cloneParams.readExternal(fds);
        
        assertEquals(PARAMS.length, cloneParams.size());
        for (int i = 0; i < PARAMS.length; i++) {
            assertEquals(PARAMS[i], cloneParams.toArray()[i]);
        } 
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
            StoredProcedureInvocation.setBasePartition(partition, ByteBuffer.wrap(invocation_bytes));
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
     * testDeserializationBase
     */
    public void testDeserializationBase() throws Exception {
        // Try with referencing the params directly
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
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
    }
    
//    /**
//     * testDeserializationWithPartitions
//     */
//    public void testDeserializationWithPartitions() throws Exception {
//        // Try with referencing the params directly
//        StoredProcedureInvocation invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
//        final Set<Integer> partitions = new HashSet<Integer>();
//        partitions.add(19);
//        partitions.add(85);
//        partitions.add(-1);
//        invocation.addPartitions(partitions);
//        
//        byte[] invocation_bytes = FastSerializer.serialize(invocation);
//        assertNotNull(invocation_bytes);
//
//        // Let 'er rip!
//        FastDeserializer fds = new FastDeserializer(invocation_bytes);
//        StoredProcedureInvocation clone = fds.readObject(StoredProcedureInvocation.class);
//        assertNotNull(clone);
//        clone.buildParameterSet();
//        
//        assertEquals(invocation.getClientHandle(), clone.getClientHandle());
//        assertEquals(invocation.getProcName(), clone.getProcName());
//        assertNotNull(clone.getParams());
//        assertArrayEquals(invocation.getParams().toArray(), clone.getParams().toArray());
//        assert(clone.hasPartitions());
//        assertEquals(partitions.size(), clone.getPartitions().size());
//        assert(partitions.containsAll(clone.getPartitions()));
//        
//    }
}
