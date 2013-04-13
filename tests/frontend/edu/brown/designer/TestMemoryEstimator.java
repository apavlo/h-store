package edu.brown.designer;

import java.io.File;

import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ProjectType;

public class TestMemoryEstimator extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 4;
    
    private WorkloadStatistics stats;
    private AbstractHasher hasher;
    private MemoryEstimator m_estimator;
    
    @Override
    protected void setUp() throws Exception {
        setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        // Load up the stats file.
        // Super hack! Walk back the directories and find out workload directory
        if (stats == null) {
            File stats_file = this.getStatsFile(ProjectType.TM1);
            stats = new WorkloadStatistics(catalog_db);
            stats.load(stats_file, catalog_db);
        }
        
        hasher = new DefaultHasher(catalogContext, NUM_PARTITIONS);
        m_estimator = new MemoryEstimator(stats, hasher);
        assertNotNull(m_estimator);
    }
    
    /**
     * testEstimateTable
     */
    public void testEstimateTable() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        long size = this.m_estimator.estimate(catalog_tbl, NUM_PARTITIONS);
        // System.err.println("SIZE: " + size);
        assert(size > 0);
    }

}
