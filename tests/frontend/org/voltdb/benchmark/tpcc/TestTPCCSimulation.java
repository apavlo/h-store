package org.voltdb.benchmark.tpcc;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ProjectType;


public class TestTPCCSimulation extends BaseTestCase {

    private static int NUM_WAREHOUSES = 8;
    
    private ScaleParameters scaleParams;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_WAREHOUSES);
        
        this.scaleParams = ScaleParameters.makeDefault(NUM_WAREHOUSES);
    }
    
    /**
     * testGeneratePairedWarehouse
     */
    public void testGeneratePairedWarehouse() throws Exception {
        for (int w_id = scaleParams.starting_warehouse; w_id <= scaleParams.last_warehouse; w_id++) {
            Histogram<Integer> h = new ObjectHistogram<Integer>();
            for (int i = 0; i < 1000; i++) {
                int id = TPCCSimulation.generatePairedWarehouse(w_id,
                                                                scaleParams.starting_warehouse,
                                                                scaleParams.last_warehouse);
                h.put(id);
            } // FOR
            System.err.println("W_ID=" + w_id + "\n" + h.toString());
            System.err.println();
            assertFalse(h.isEmpty());
            assertEquals(h.toString(), 1, h.getValueCount());
        } // FOR
    }
    
}
