package edu.brown.benchmark.ycsb;

import edu.brown.BaseTestCase;
import edu.brown.profilers.ProfileMeasurement;
//import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;
import edu.brown.utils.ProjectType;

public class TestYCSBUtil extends BaseTestCase {

//    private long init_record_count = 1000;
//    private double skewFactor = YCSBConstants.ZIPFIAN_CONSTANT;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.YCSB);
//        this.init_record_count = (long)Math.round(YCSBConstants.NUM_RECORDS * 1d);
    }
    
    public void testGenParams() throws Exception {
        ProfileMeasurement pm = new ProfileMeasurement("params");
        for (int ctr = 0; ctr < 1000000; ctr++) {
            pm.start();
            String fields[] = new String[YCSBConstants.NUM_COLUMNS];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH);
            } // FOR
            pm.stop();
        } // FOR
        System.err.println(pm.debug());
    }
    
}
