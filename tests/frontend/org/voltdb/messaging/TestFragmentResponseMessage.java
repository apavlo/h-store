package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * @author pavlo
 */
public class TestFragmentResponseMessage extends TestCase {
    
    protected final DBBPool buffer_pool = new DBBPool(true, false);
    protected FragmentResponseMessage f;
    protected FragmentResponseMessage f_nodata;
    
    protected static int initiatorSiteId = 1;
    protected static int coordinatorSiteId = 1;
    protected static long txnId = 1;
    protected static long clientHandle = 1;
    protected static boolean isReadOnly = false;
    protected static long[] fragmentIds = { 0, 1, 2 };
    protected static int[] inputDependencyIds = { 0, 1, 2 };
    protected static int[] outputDependencyIds = {  };
    protected static int[] stmtIndexes = { 0, 0, 0  };
    protected static ByteBuffer parameterSets[] = new ByteBuffer[fragmentIds.length];
    protected static boolean isFinal = true;
    
    static {
        Object params[] = { new Long(1), new Long(2) };

        for (int i = 0, cnt = fragmentIds.length; i < cnt; i++) {
            ParameterSet p = new ParameterSet();
            p.setParameters(params);
            FastSerializer fs = new FastSerializer();
            try {
                fs.writeObject(p);
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            }
            parameterSets[i] = fs.getBuffer();
        } // FOR
    } // STATIC
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        result.addRow(1);
        
        FragmentTaskMessage ftask = new FragmentTaskMessage(initiatorSiteId, coordinatorSiteId, txnId, clientHandle, isReadOnly, fragmentIds, inputDependencyIds, outputDependencyIds, parameterSets, stmtIndexes, isFinal);
        
        f = new FragmentResponseMessage(ftask);
        f.addDependency(1, result);
        f.setDirtyFlag(false);
        f.setStatus(FragmentResponseMessage.SUCCESS, null);
        
        f_nodata = new FragmentResponseMessage(ftask);
        f_nodata.addDependency(1);
        f_nodata.setDirtyFlag(false);
        f_nodata.setStatus(FragmentResponseMessage.SUCCESS, null);

    }
    
    public void testToBuffer() {
        BBContainer bc = f.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
        assertFalse(f.m_nullDependencies);
    }
    
    public void testToBufferNoDependencies() {
        BBContainer bc = f_nodata.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
        assert(f_nodata.m_nullDependencies);
    }
    
    public void testFromBuffer() {
        BBContainer bc = f.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
        
        FragmentResponseMessage f2 = (FragmentResponseMessage)VoltMessage.createMessageFromBuffer(bc.b.asReadOnlyBuffer(), false);
        assertEquals(f.getDestinationPartitionId(), f2.getDestinationPartitionId());
        assertEquals(f.getSourcePartitionId(), f2.getSourcePartitionId());
        assertEquals(f.m_nullDependencies, f2.m_nullDependencies);
        assertEquals(f.m_dirty, f2.m_dirty);
        assertEquals(f.m_status, f2.m_status);
        assertEquals(f.m_txnId, f2.m_txnId);
        
        
        assertEquals(f.m_dependencyCount, f2.m_dependencyCount);
        for (int i = 0, cnt = f.getTableCount(); i < cnt; i++) {
            VoltTable vt1 = f.getTableAtIndex(i);
            VoltTable vt2 = f2.getTableAtIndex(i);
            assertEquals(vt1.getRowCount(), vt2.getRowCount());
        } // FOR
    }
    
    public void testFromBufferNoDependencies() {
        BBContainer bc = f_nodata.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
        
        FragmentResponseMessage f2 = (FragmentResponseMessage)VoltMessage.createMessageFromBuffer(bc.b.asReadOnlyBuffer(), false);
        assertEquals(f_nodata.getDestinationPartitionId(), f2.getDestinationPartitionId());
        assertEquals(f_nodata.getSourcePartitionId(), f2.getSourcePartitionId());
        assertEquals(f_nodata.m_nullDependencies, f2.m_nullDependencies);
        assertEquals(f_nodata.m_dirty, f2.m_dirty);
        assertEquals(f_nodata.m_status, f2.m_status);
        assertEquals(f_nodata.m_txnId, f2.m_txnId);
        
        assertEquals(f_nodata.m_dependencyCount, f2.m_dependencyCount);
    }

}