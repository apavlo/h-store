package edu.brown.hstore;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.voltdb.types.ExpressionType;

import com.sun.tools.javac.code.Attribute.Array;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.MultiPartitionTxnFilter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestTransactionInitializer extends BaseTestCase {

    private static final int NUM_PARTITIONS = 2;
    private static final int WORKLOAD_XACT_LIMIT = 100;
    private static final int BASE_PARTITION = 0;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    
    private static File markovsFile;
    private static Workload workload;
    
    private HStoreSite hstore_site;
    private HStoreSite.Debug hstore_debug;
    private HStoreConf hstore_conf;
    private TransactionInitializer txnInitializer;
    private PartitionExecutor executors[];
    private TransactionTrace txnTrace;
    private Client client;
    private Procedure catalog_proc;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.txn_client_debug = true;
        
        if (isFirstSetup()) {
            
            // Workload Filter:
            //  (1) Only include TARGET_PROCEDURE traces
            //  (2) Only include traces with 10 orderline items
            //  (3) Only include traces that execute on the BASE_PARTITION
            //  (4) Limit the total number of traces to WORKLOAD_XACT_LIMIT
            List<ProcParameter> array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
            Filter filter = new ProcedureNameFilter(false)
                  .include(TARGET_PROCEDURE.getSimpleName())
                  .attach(new NoAbortFilter())
                  .attach(new MultiPartitionTxnFilter(p_estimator, false))
                  .attach(new ProcParameterArraySizeFilter(array_params.get(0), 10, ExpressionType.COMPARE_EQUAL))
                  .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalogContext.catalog).load(file, catalogContext.database, filter);
            
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
        assertTrue(markovsFile.exists());
        
        
    }
    
    private void initHStore() throws Exception {
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.hstore_debug = this.hstore_site.getDebugContext();
        this.txnInitializer = this.hstore_site.getTransactionInitializer();
        this.executors = new PartitionExecutor[NUM_PARTITIONS];
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            this.executors[partition] = hstore_site.getPartitionExecutor(partition);
        } // FOR
        
        this.client = createClient();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    /**
     * testMarkovInitialization
     */
    public void testMarkovInitialization() throws Exception {
        this.hstore_conf.site.markov_enable = true;
        this.hstore_conf.site.markov_path = markovsFile.getAbsolutePath();
        this.hstore_conf.site.markov_dtxn_updates = true;
        this.hstore_conf.site.markov_dtxn_updates = true;
        this.initHStore();
        
        for (PartitionExecutor executor : this.executors) {
            TransactionEstimator estimator = executor.getTransactionEstimator();
            assertNotNull(estimator);
            assertEquals(MarkovEstimator.class, estimator.getClass());
        } // FOR
        
        Object params[] = CollectionUtil.first(workload).getParams();
        for (int i = 0; i < params.length; i++) {
            if (ClassUtil.isArray(params[i])) {
                Object inner[] = (Object[])params[i];
                int newInner[] = new int[inner.length];
                for (int ii = 0; ii < inner.length; ii++) {
                    newInner[ii] = ((Number)inner[ii]).intValue();
                }
                params[i] = newInner;
            }
        } // FOR
        
        this.client.callProcedure(new NullCallback(), this.catalog_proc.getName(), params);
        
        final AtomicReference<LocalTransaction> lastTxn = new AtomicReference<LocalTransaction>();
        final CountDownLatch latch = new CountDownLatch(1);
        EventObserver<LocalTransaction> newTxnObserver = new EventObserver<LocalTransaction>() {
            @Override
            public void update(EventObservable<LocalTransaction> o, LocalTransaction ts) {
                lastTxn.set(ts);
                latch.countDown();
            }
        };
        this.txnInitializer.getNewTxnObservable().addObserver(newTxnObserver);
        
        boolean result = latch.await(10000, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        LocalTransaction ts = lastTxn.get();
        assertNotNull(ts);
        assertFalse(ts.isPredictSinglePartition());
        
        EstimatorState estState = ts.getEstimatorState();
        assertNotNull(estState);
        assertEquals(MarkovEstimatorState.class, estState.getClass());
        assertTrue(estState.isInitialized());
        
        Estimate est = estState.getInitialEstimate();
        assertNotNull(est);
        assertEquals(MarkovEstimate.class, est.getClass());
        
        
    }
}
