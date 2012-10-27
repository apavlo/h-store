package org.voltdb.benchmark.tpcc;

import org.voltdb.benchmark.Clock;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.MockVoltClient;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ProjectType;


public class TestTPCCSimulation extends BaseTestCase {

    private static int NUM_WAREHOUSES = 8;
    private static double SKEW_FACTOR = 0.0;
    
    private ScaleParameters scaleParams;
    private TPCCSimulation tpccSimulation;
    private TPCCClient tpccClient;
    private TPCCConfig tpccConfig = TPCCConfig.defaultConfig();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_WAREHOUSES);
        
        MockVoltClient mockClient = new MockVoltClient();
        MockRandomGenerator generator = new MockRandomGenerator();
        Clock clock = new Clock.RealTime();
        
        this.scaleParams = ScaleParameters.makeDefault(NUM_WAREHOUSES);
        this.tpccClient = new TPCCClient(mockClient, generator, clock, this.scaleParams, this.tpccConfig) {
            public Catalog getCatalog() {
                return catalogContext.catalog;
            }
        };
        this.tpccSimulation = new TPCCSimulation(this.tpccClient,
                                                 generator,
                                                 clock,
                                                 scaleParams,
                                                 this.tpccConfig,
                                                 SKEW_FACTOR,
                                                 catalogContext.catalog);
    }
    
    
    /**
     * testGeneratePairedWarehouse
     */
    public void testGeneratePairedWarehouse() throws Exception {
        for (int w_id = scaleParams.starting_warehouse; w_id <= scaleParams.last_warehouse; w_id++) {
            Histogram<Integer> h = new Histogram<Integer>();
            for (int i = 0; i < 1000; i++) {
                h.put((int)this.tpccSimulation.generatePairedWarehouse((short)w_id));
            } // FOR
            System.err.println("W_ID=" + w_id + "\n" + h.toString());
            System.err.println();
            assertFalse(h.isEmpty());
            assertEquals(h.toString(), 1, h.getValueCount());
        } // FOR
    }
    
}
