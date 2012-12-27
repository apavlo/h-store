package edu.brown.hashing;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.benchmark.tpcc.TPCCConstants;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

public class TestDefaultHasher extends BaseTestCase {

    private static final int NUM_PARTITIONS = 200;
    private DefaultHasher hasher;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        hasher = new DefaultHasher(null, NUM_PARTITIONS);
    }
    
    /**
     * testHashValue
     */
    public void testHashValue() throws Exception {
        // Stupid test for now...
        long val0 = 28;
        int hash0 = this.hasher.hash(val0);
        
        long val1 = val0 + 1;
        int hash1 = this.hasher.hash(val1);
        
        assertNotSame(hash0, hash1);
//        System.err.println("hash0[" + val0 + "] = " + hash0);
//        System.err.println("hash1[" + val1 + "] = " + hash1);
    }

    /**
     * testMultiValueHash
     */
    public void testMultiValueHash() throws Exception {
        int num_values0 = 50; // # of Warehouses
        int num_values1 = TPCCConstants.DISTRICTS_PER_WAREHOUSE;
        int num_partitions = 100;
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        
        DefaultHasher hasher = new DefaultHasher(null, num_partitions);

        Map<Integer, Map<Integer, Integer>> hashes = new HashMap<Integer, Map<Integer,Integer>>();
        for (int i = 0; i < num_values0; i++) {
            hashes.put(i, new HashMap<Integer, Integer>());
            for (int ii = 0; ii < num_values1; ii++) {
                String s = String.format("[%d, %d] => ", i, ii);
                int hash = hasher.multiValueHash(i, ii);
                assert(hash >= 0) : s + "Invalid Hash: " + hash;
                assert(hash < num_partitions) : s + "Invalid Hash: " + hash;
                h.put(hash);
                hashes.get(i).put(ii, hash);
                
                // Check that if we throw it in a array, we can get the same hash
                int arr_hash = hasher.multiValueHash(new Integer[]{i, ii});
                assert(arr_hash >= 0) : s + "Invalid Hash: " + arr_hash;
                assert(arr_hash < num_partitions) : s + "Invalid Hash: " + arr_hash;
                assertEquals(s + " Array Hash Mismatch", hash, arr_hash);
                
                // Check to make sure objects give the same hash too
                Long obj_i = new Long(i);
                Long obj_ii = new Long(ii);
                int obj_hash = hasher.multiValueHash(obj_i, obj_ii);
                assert(obj_hash >= 0) : s + "Invalid Hash: " + obj_hash;
                assert(obj_hash < num_partitions) : s + "Invalid Hash: " + obj_hash;
                assertEquals(s + " Object Hash Mismatch", hash, obj_hash);
            } // FOR
        } // FOR

        hasher = new DefaultHasher(null, num_partitions);
        
        // Now go through again and make sure we get the same values
        for (int i = 0; i < num_values0; i++) {
            Map<Integer, Integer> expected = hashes.get(i);
            assertNotNull("Null: " + i, expected);
            assertFalse("Empty: " + i, expected.isEmpty());
            
            for (int ii = 0; ii < num_values1; ii++) {
                String s = String.format("[%d, %d] => ", i, ii);
                int hash = hasher.multiValueHash(i, ii);
                assert(hash >= 0) : s + "Invalid Hash: " + hash;
                assert(hash < num_partitions) : s + "Invalid Hash: " + hash;
                assertEquals(s + "Mismatch!", expected.get(ii).intValue(), hash);
            } // FOR
        } // FOR

        
        double fudgey_the_whale_factor = ((num_values0 * num_values1) / (double)num_partitions) * 0.75d;
        for (int i = 0; i < num_partitions; i++) {
            assert(h.contains(i)) : "Empty count : " + i;
            long cnt = h.get(i);
            boolean in_limit = cnt > fudgey_the_whale_factor;
            if (!in_limit) System.err.println(h);     
            assert(in_limit) : "[" + i + "] " + cnt + " > " + fudgey_the_whale_factor;
        } // FOR
    }
}
