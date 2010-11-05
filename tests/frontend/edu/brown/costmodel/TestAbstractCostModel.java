package edu.brown.costmodel;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;

import edu.brown.workload.*;
import edu.brown.workload.filters.ProcedureLimitFilter;

public class TestAbstractCostModel extends TestCase {

    protected final static String workload_path = "/home/pavlo/Documents/H-Store/SVN-Vertica/workloads/server.trace";
    
    protected Catalog catalog;
    protected Database catalog_db;
    protected Workload workload;
    
    /*
    @Override
    protected void setUp() throws Exception {
        catalog = new TPCCProjectBuilder() {
            @Override
            public void addDefaultSchema() {
                addSchema(TPCCProjectBuilder.class.getResource("tpcc-ddl-fkeys.sql"));
            }
        }.createTPCCSchemaCatalog();
        assertNotNull(catalog);
        catalog_db = catalog.getClusters().get(0).getDatabases().get(0);
        assertNotNull(catalog_db);
        
        workload = new Workload(catalog);
        workload.load(workload_path, catalog_db, new ProcedureLimitFilter(10l));
    }
    */
    
    /**
     * testIsSingleSited
     */
    public void testIsSingleSited() throws Exception {
        return;
        /*
        SingleSitedCostModel model = new SingleSitedCostModel(catalog_db);
        
        for (AbstractTraceElement element : workload) {
            if (element instanceof TransactionTrace) {
                model.estimateCost((TransactionTrace)element, null);
            }
        } // FOR
        */
    }
}
