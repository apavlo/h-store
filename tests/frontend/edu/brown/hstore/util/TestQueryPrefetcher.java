package edu.brown.hstore.util;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestQueryPrefetcher extends BaseTestCase {

    QueryPrefetcher prefetcher;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.prefetcher = new QueryPrefetcher(catalog_db, p_estimator);
    }
    
    /**
     * testGenerateWorkFragments
     */
    public void testGenerateWorkFragments() throws Exception {
        
    }
    
    
    
}
