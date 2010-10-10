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
public class TestFragmentTaskMessage extends TestCase {
    
    protected final DBBPool buffer_pool = new DBBPool(true, false);
    protected FragmentTaskMessage f;
    
    protected static int initiatorSiteId = 1;
    protected static int coordinatorSiteId = 1;
    protected static long txnId = 1;
    protected static long clientHandle = 1;
    protected static boolean isReadOnly = false;
    protected static long[] fragmentIds = { 0, 1, 2 };
    protected static int[] inputDependencyIds = { 0, 1, 2 };
    protected static int[] outputDependencyIds = { 0, 1, 2 };
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
        
        f = new FragmentTaskMessage(initiatorSiteId, coordinatorSiteId, txnId, clientHandle, isReadOnly, fragmentIds, inputDependencyIds, outputDependencyIds, parameterSets, stmtIndexes, isFinal);
    }
    
    public void testToBuffer() {
        BBContainer bc = f.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
    }
    
    public void testFromBuffer() {
        BBContainer bc = f.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
        
        FragmentTaskMessage f2 = (FragmentTaskMessage)VoltMessage.createMessageFromBuffer(bc.b.asReadOnlyBuffer(), false);
        assertEquals(f.getSourcePartitionId(), f2.getSourcePartitionId());
        assertEquals(f.getClientHandle(), f2.getClientHandle());
        assertEquals(f.getDestinationPartitionId(), f2.getDestinationPartitionId());
        assertEquals(f.getTxnId(), f2.getTxnId());
        
        assertEquals(f.getFragmentCount(), f2.getFragmentCount());
        for (int i = 0, cnt = f.getFragmentCount(); i < cnt; i++) {
            assertEquals(f.getFragmentIds()[i], f2.getFragmentIds()[i]);
            assertEquals(f.getFragmentStmtIndexes()[i], f2.getFragmentStmtIndexes()[i]);
            assertEquals(f.getOnlyInputDepId(i), f2.getOnlyInputDepId(i));
            assertEquals(f.getOutputDependencyIds()[i], f2.getOutputDependencyIds()[i]);
        } // FOR
    }
}
