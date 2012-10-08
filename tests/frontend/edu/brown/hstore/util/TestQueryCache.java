package edu.brown.hstore.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.utils.ProjectType;

/**
 * TestQueryCache
 * @author pavlo
 */
public class TestQueryCache extends BaseTestCase {

    private static final int globalBufferSize = 10;
    private static final int txnBufferSize = 10;
    private static Class<? extends VoltProcedure> TARGET_PROCEDURE = DeleteCallForwarding.class;
    private static String TARGET_STATEMENT = "query";
    private static VoltTable.ColumnInfo TARGET_RESULT[] = {
        new VoltTable.ColumnInfo("S_ID", VoltType.BIGINT)
    };
    
    QueryCache cache;
    Procedure catalog_proc;
    Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.cache = new QueryCache(globalBufferSize, txnBufferSize);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_stmt = this.getStatement(catalog_proc, TARGET_STATEMENT);
    }
    
    /**
     * testTxnCacheGet
     */
    public void testTxnCacheGet() throws Exception {
        Long txnId = new Long(123456);
        int baseFragmentId = 1111;
        int partitionId = 1;
        long baseValue = 9900;
        ParameterSet params = new ParameterSet("Squirrels", 1981);
        
        Map<Integer, Long> expectedValues = new HashMap<Integer, Long>();
        for (int i = 0; i < 5; i++) {
            int fragmentId = baseFragmentId + i;
            long expected = baseValue + i;
            VoltTable result = new VoltTable(TARGET_RESULT);
            result.addRow(expected);
            expectedValues.put(fragmentId, expected);
        
            // First store it in the cache
            this.cache.addResult(txnId, fragmentId, partitionId, params, result);
        } // FOR
//        System.err.println(StringUtil.formatMaps(expectedValues));
//        System.err.println("---------------");
//        System.err.println(this.cache.toString());
        
        // Then ask for them back in a shuffled order
        List<Integer> fragmentIds = new ArrayList<Integer>(expectedValues.keySet());
        Collections.shuffle(fragmentIds);
        for (Integer fragmentId : fragmentIds) {
            long expected = expectedValues.get(fragmentId);
            VoltTable cacheResult = this.cache.getResult(txnId, fragmentId, partitionId, params);
            assertNotNull(fragmentId.toString(), cacheResult);
            assertEquals(1, cacheResult.getRowCount());
            assertEquals(expected, cacheResult.asScalarLong());
        } // FOR
        
        // If we change the params, we should never get back our results
        params = new ParameterSet("WuTang!", 1981);
        for (Integer fragmentId : fragmentIds) {
            VoltTable cacheResult = this.cache.getResult(txnId, fragmentId, partitionId, params);
            assertNull(fragmentId.toString(), cacheResult);
        } // FOR
        
        // Now create a new ParameteSet with the same values and make
        // sure get back the same results.
        params = new ParameterSet("Squi" + "rrels", 1981);
        for (Integer fragmentId : fragmentIds) {
            long expected = expectedValues.get(fragmentId);
            VoltTable cacheResult = this.cache.getResult(txnId, fragmentId, partitionId, params);
            assertNotNull(fragmentId.toString(), cacheResult);
            assertEquals(1, cacheResult.getRowCount());
            assertEquals(expected, cacheResult.asScalarLong());
        } // FOR
    }
}
