package edu.brown.hstore;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestTransactionInitializer extends BaseTestCase {

    private static final int NUM_PARTITIONS = 2;
    private static final int WORKLOAD_XACT_LIMIT = 100;
    private static final int BASE_PARTITION = 1;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    
    private static ParameterMappingsSet mappings;
    private static File markovsFile;
    
    private HStoreSite hstore_site;
    private HStoreSite.Debug hstore_debug;
    private HStoreConf hstore_conf;
    private TransactionInitializer txnInitializer;
    private Client client;
    private Procedure catalog_proc;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.hstore_conf = HStoreConf.singleton();
        
        if (isFirstSetup()) {
            File file = this.getParameterMappingsFile(ProjectType.TPCC);
            mappings = new ParameterMappingsSet();
            mappings.load(file, catalogContext.database);
            
            // Workload Filter:
            //  (1) Only include TARGET_PROCEDURE traces
            //  (2) Only include traces with 10 orderline items
            //  (3) Only include traces that execute on the BASE_PARTITION
            //  (4) Limit the total number of traces to WORKLOAD_XACT_LIMIT
            List<ProcParameter> array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
            Filter filter = new ProcedureNameFilter(false)
                  .include(TARGET_PROCEDURE.getSimpleName())
                  .attach(new NoAbortFilter())
                  .attach(new ProcParameterArraySizeFilter(array_params.get(0), 10, ExpressionType.COMPARE_EQUAL))
                  .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                  .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            
            file = this.getWorkloadFile(ProjectType.TPCC);
            Workload workload = new Workload(catalogContext.catalog).load(file, catalogContext.database, filter);
            
            // GENERATE MARKOV GRAPHS
            Map<Integer, MarkovGraphsContainer> markovs = MarkovGraphsContainerUtil.createMarkovGraphsContainers(
                                                                catalogContext,
                                                                workload,
                                                                p_estimator,
                                                                MarkovGraphsContainer.class);
            assertNotNull(markovs);
            markovsFile = FileUtil.getTempFile("markovs", true);
            MarkovGraphsContainerUtil.save(markovs, markovsFile);
        }
        
        
    }
    
    private void initHStore() throws Exception {
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.hstore_debug = this.hstore_site.getDebugContext();
        this.txnInitializer = this.hstore_site.getTransactionInitializer();
        this.client = createClient();
    }
    
    /**
     * 
     */
    public void testMarkovInitialization() {
        this.hstore_conf.site.markov_enable = true;
        
    }
}
