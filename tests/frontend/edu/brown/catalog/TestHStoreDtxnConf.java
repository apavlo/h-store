package edu.brown.catalog;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestHStoreDtxnConf extends BaseTestCase {

    private static final int NUM_PARTITIONS = 10;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
    }
    
    /**
     * testToHStoreDtxnConf
     */
    public void testToHStoreDtxnConf() throws Exception {
        String contents = HStoreDtxnConf.toHStoreDtxnConf(catalog);
        assertNotNull(contents);
        assertFalse(contents.isEmpty());
        
        int localhost_ctr = 0;
        for (String line : contents.split("\n")) {
            if (line.contains("localhost")) localhost_ctr++;
        } // FOR
        assertEquals(NUM_PARTITIONS, localhost_ctr);
    }
    
    /**
     * testSortOrder
     */
    public void testSortOrder() throws Exception {
        String contents = HStoreDtxnConf.toHStoreDtxnConf(catalog);
        assertNotNull(contents);
        assertFalse(contents.isEmpty());
        
        int last_partition = Integer.MIN_VALUE;
        for (String line : contents.split("\n")) {
            if (!(line.isEmpty() || line.contains("localhost"))) {
                int partition = Integer.parseInt(line);
                assert(partition > last_partition) :
                    "Last Partition=" + last_partition + ", Current Partition=" + partition; 
            }
        } // FOR

    }
}