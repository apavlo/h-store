package edu.brown.hstore.estimators.fixed;

import org.junit.Test;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.TimestampType;

import edu.brown.BaseTestCase;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.fixed.AbstractFixedEstimator;
import edu.brown.hstore.estimators.fixed.FixedTPCCEstimator;
import edu.brown.markov.EstimationThresholds;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public class TestFixedTPCCEstimator extends BaseTestCase {

    private static final int NUM_PARTITIONS = 8;
    private static final DefaultRandomGenerator rng = new DefaultRandomGenerator(0);
    private static final EstimationThresholds thresholds = new EstimationThresholds();
    
    private FixedTPCCEstimator estimator;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        this.estimator = AbstractFixedEstimator.factory(p_estimator, catalogContext);
        assertNotNull(this.estimator);
    }

    /**
     * testNewOrder
     */
    @Test
    public void testNewOrder() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        AbstractHasher hasher = p_estimator.getHasher();
        
        long txn_id = 1000;
        for (short w_id = 1; w_id <= NUM_PARTITIONS; w_id++) {
            short remote_w_id = (short)rng.numberExcluding(1, NUM_PARTITIONS, w_id);
            
            // ORDER_LINE ITEMS
            int num_items = rng.number(TPCCConstants.MIN_OL_CNT, TPCCConstants.MAX_OL_CNT);
            int item_id[] = new int[num_items];
            short supware[] = new short[num_items];
            int quantity[] = new int[num_items];
            for (int i = 0; i < num_items; i++) { 
                item_id[i] = rng.nextInt();
                supware[i] = remote_w_id;
                quantity[i] = rng.number(TPCCConstants.MIN_QUANTITY, TPCCConstants.MAX_QUANTITY);
            } // FOR
            
            Object args[] = {
                w_id,                   // W_ID
                (byte)rng.nextInt(10),  // D_ID
                rng.nextInt(),          // C_ID
                new TimestampType(),    // TIMESTAMP
                item_id,                // ITEM_IDS
                supware,                // SUPPLY W_IDS
                quantity                // QUANTITIES
            };
            
            EstimatorState state = this.estimator.startTransaction(txn_id, catalog_proc, args);
            assertNotNull(state);
            assertEquals(txn_id, state.getTransactionId().longValue());
//            System.err.printf("W_ID=%d / S_W_ID=%s\n", w_id, remote_w_id);
            
            // Make sure that it identifies that we are a distributed transaction and 
            // that we expect to touch both the W_ID partition and the REMOTE_W_ID partition
            Estimate est = state.getInitialEstimate();
            assertNotNull(est);
            PartitionSet partitions = est.getTouchedPartitions(thresholds);
            assertEquals(2, partitions.size());
            for (int expected : new int[]{ w_id, remote_w_id }) {
                expected = hasher.hash(expected);
                assertTrue(Integer.toString(expected) + "->" + partitions, partitions.contains(expected));
            } // FOR
            assertEquals(hasher.hash(w_id), state.getBasePartition());
            txn_id++;
        } // FOR
    }
    
}
