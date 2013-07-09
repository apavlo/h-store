package edu.brown.costmodel;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.CatalogContext;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogCloner;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.hstore.HStoreConstants;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.Workload;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestSingleSitedCostModel extends BaseTestCase {

    private static final int PROC_COUNT = 3;
    
    @SuppressWarnings("unchecked")
    private static final Class<? extends VoltProcedure> PROCEDURES[] = new Class[]{
        DeleteCallForwarding.class,
        GetAccessData.class,
        GetNewDestination.class,
    };
    private static final String TARGET_PROCEDURES[] = new String[PROCEDURES.length];
    static {
        for (int i = 0; i < TARGET_PROCEDURES.length; i++) {
            TARGET_PROCEDURES[i] = PROCEDURES[i].getSimpleName();
        } // FOR
    };
    private static final boolean TARGET_PROCEDURES_SINGLEPARTITION_DEFAULT[] = {
        false,  // DeleteCallForwarding
        true,   // GetAccessData
        true,   // GetNewDestination
    };
    
    private static final int NUM_PARTITIONS = 10;
    
    // Reading the workload takes a long time, so we only want to do it once
    private static Workload workload;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        ThreadUtil.setMaxGlobalThreads(1);
        
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.TM1); 
            workload = new Workload(catalogContext.catalog);
            
            // Workload Filter
            ProcedureNameFilter filter = new ProcedureNameFilter(false);
            long total = 0;
            for (String proc_name : TARGET_PROCEDURES) {
                filter.include(proc_name, PROC_COUNT);
                total += PROC_COUNT;
            }
            ((Workload)workload).load(workload_file, catalogContext.database, filter);
            assertEquals(total, workload.getTransactionCount());
            assertEquals(TARGET_PROCEDURES.length, workload.getProcedureHistogram().getValueCount());
            // System.err.println(workload.getProcedureHistogram());
        }
    }
    
    private TransactionTrace getMultiPartitionTransaction() {
        Procedure catalog_proc = null;
        for (int i = 0; i < TARGET_PROCEDURES.length; i++) {
            if (TARGET_PROCEDURES_SINGLEPARTITION_DEFAULT[i] == false) {
                catalog_proc = this.getProcedure(TARGET_PROCEDURES[i]);
                break;
            }
        } // FOR
        assertNotNull(catalog_proc);
        TransactionTrace multip_txn = null;
        for (TransactionTrace txn_trace : workload) {
            if (txn_trace.getCatalogItem(catalogContext.database).equals(catalog_proc)) {
                multip_txn = txn_trace;
                break;
            }
        } // FOR
        assertNotNull(multip_txn);
        return (multip_txn);
    }
    
    public static Map<Field, Histogram<?>> getHistograms(AbstractCostModel cost_model) throws Exception {
        Map<Field, Histogram<?>> ret = new HashMap<Field, Histogram<?>>();
        Class<?> clazz = cost_model.getClass().getSuperclass();
        for (Field f : clazz.getDeclaredFields()) {
            if (ClassUtil.getInterfaces(f.getType()).contains(Histogram.class)) {
                ret.put(f, (Histogram<?>)f.get(cost_model));
            }
        } // FOR
        assertFalse(ret.isEmpty());
        return (ret);
    }
    
    /**
     * testWeightedTxnEstimation
     */
    public void testWeightedTxnEstimation() throws Exception {
        // Make a new workload that only has multiple copies of the same multi-partition transaction
        Workload new_workload = new Workload(catalogContext.catalog);
        int num_txns = 13;
        TransactionTrace multip_txn = this.getMultiPartitionTransaction();
        Procedure catalog_proc = multip_txn.getCatalogItem(catalogContext.database);
        for (int i = 0; i < num_txns; i++) {
            TransactionTrace clone = (TransactionTrace)multip_txn.clone();
            clone.setTransactionId(i);
            new_workload.addTransaction(catalog_proc, clone);
        } // FOR
        assertEquals(num_txns, new_workload.getTransactionCount());
        
        // We now want to calculate the cost of this new workload
        final SingleSitedCostModel orig_costModel = new SingleSitedCostModel(catalogContext);
        final double orig_cost = orig_costModel.estimateWorkloadCost(catalogContext, new_workload);
        assert(orig_cost > 0);
        // if (orig_costModel.getMultiPartitionProcedureHistogram().isEmpty()) System.err.println(orig_costModel.getTransactionCacheEntry(0).debug());
        assertEquals(num_txns, orig_costModel.getMultiPartitionProcedureHistogram().getSampleCount());
        assertEquals(0, orig_costModel.getSinglePartitionProcedureHistogram().getSampleCount());
        
        // Only the base partition should be touched (2 * num_txns). Everything else should
        // be touched num_txns
        Integer base_partition = CollectionUtil.first(orig_costModel.getQueryPartitionAccessHistogram().getMaxCountValues());
        assertNotNull(base_partition);
        for (Integer p : orig_costModel.getQueryPartitionAccessHistogram().values()) {
            if (p.equals(base_partition)) {
                assertEquals(2 * num_txns, orig_costModel.getQueryPartitionAccessHistogram().get(p).intValue());
            } else {
                assertEquals(num_txns, orig_costModel.getQueryPartitionAccessHistogram().get(p).intValue());
            }
        } // FOR
        
        // Now change make a new workload that has the same multi-partition transaction
        // but this time it only has one but with a transaction weight
        // We should get back the exact same cost
        new_workload = new Workload(catalogContext.catalog);
        TransactionTrace clone = (TransactionTrace)multip_txn.clone();
        clone.setTransactionId(1000);
        clone.setWeight(num_txns);
        new_workload.addTransaction(catalog_proc, clone);
        final SingleSitedCostModel new_costModel = new SingleSitedCostModel(catalogContext);
        final double new_cost = new_costModel.estimateWorkloadCost(catalogContext, new_workload);
        assert(new_cost > 0);
        assertEquals(orig_cost, new_cost, 0.001);
        
        // Now make sure the histograms match up
        Map<Field, Histogram<?>> orig_histograms = getHistograms(orig_costModel);
        assertFalse(orig_histograms.isEmpty());
        Map<Field, Histogram<?>> new_histograms = getHistograms(new_costModel);
        assertFalse(new_histograms.isEmpty());
        for (Field f : orig_histograms.keySet()) {
            Histogram<?> orig_h = orig_histograms.get(f);
            assertNotNull(orig_h);
            Histogram<?> new_h = new_histograms.get(f);
            assert(orig_h != new_h);
            assertNotNull(new_h);
            assertEquals(orig_h, new_h);
        } // FOR
    }
    
    /**
     * testWeightedQueryEstimation
     */
    public void testWeightedQueryEstimation() throws Exception {
        // Make a new workload that has its single queries duplicated multiple times
        Workload new_workload = new Workload(catalogContext.catalog);
        int num_dupes = 7;
        TransactionTrace multip_txn = this.getMultiPartitionTransaction();
        Procedure catalog_proc = multip_txn.getCatalogItem(catalogContext.database);
        
        final TransactionTrace orig_txn = (TransactionTrace)multip_txn.clone();
        List<QueryTrace> clone_queries = new ArrayList<QueryTrace>();
        for (int i = 0; i < num_dupes; i++) {
            for (QueryTrace query_trace : multip_txn.getQueries()) {
                QueryTrace clone_query = (QueryTrace)query_trace.clone();
                clone_queries.add(clone_query);
            } // FOR
        } // FOR
        orig_txn.setQueries(clone_queries);
        new_workload.addTransaction(catalog_proc, orig_txn);
        assertEquals(1, new_workload.getTransactionCount());
        assertEquals(multip_txn.getQueryCount() * num_dupes, orig_txn.getQueryCount());
        
        // We now want to calculate the cost of this new workload
        final SingleSitedCostModel orig_costModel = new SingleSitedCostModel(catalogContext);
        final double orig_cost = orig_costModel.estimateWorkloadCost(catalogContext, new_workload);
        assert(orig_cost > 0);
        TransactionCacheEntry orig_txnEntry = orig_costModel.getTransactionCacheEntry(orig_txn);
        assertNotNull(orig_txnEntry);
        assertEquals(orig_txn.getQueryCount(), orig_txnEntry.getExaminedQueryCount());
//        System.err.println(orig_txnEntry.debug());
//        System.err.println("=========================================");
        
        // Now change make a new workload that has the same multi-partition transaction
        // but this time it only has one but with a transaction weight
        // We should get back the exact same cost
        new_workload = new Workload(catalogContext.catalog);
        final TransactionTrace new_txn = (TransactionTrace)multip_txn.clone();
        clone_queries = new ArrayList<QueryTrace>();
        for (QueryTrace query_trace : multip_txn.getQueries()) {
            QueryTrace clone_query = (QueryTrace)query_trace.clone();
            clone_query.setWeight(num_dupes);
            clone_queries.add(clone_query);
        } // FOR
        new_txn.setQueries(clone_queries);
        new_workload.addTransaction(catalog_proc, new_txn);
        assertEquals(1, new_workload.getTransactionCount());
        assertEquals(multip_txn.getQueryCount(), new_txn.getQueryCount());
        assertEquals(multip_txn.getQueryCount() * num_dupes, new_txn.getWeightedQueryCount());
        
        final SingleSitedCostModel new_costModel = new SingleSitedCostModel(catalogContext);
        final double new_cost = new_costModel.estimateWorkloadCost(catalogContext, new_workload);
        assert(new_cost > 0);
        assertEquals(orig_cost, new_cost, 0.001);
        
        
        // Now make sure the histograms match up
        Map<Field, Histogram<?>> orig_histograms = getHistograms(orig_costModel);
        Map<Field, Histogram<?>> new_histograms = getHistograms(new_costModel);
        for (Field f : orig_histograms.keySet()) {
            Histogram<?> orig_h = orig_histograms.get(f);
            assertNotNull(orig_h);
            Histogram<?> new_h = new_histograms.get(f);
            assert(orig_h != new_h);
            assertNotNull(new_h);
            assertEquals(orig_h, new_h);
        } // FOR
        
        // Compare the TransactionCacheEntries
        
        TransactionCacheEntry new_txnEntry = new_costModel.getTransactionCacheEntry(new_txn);
        assertNotNull(new_txnEntry);
//        System.err.println(new_txnEntry.debug());
        
        assertEquals(orig_txnEntry.getExaminedQueryCount(), new_txnEntry.getExaminedQueryCount());
        assertEquals(orig_txnEntry.getSingleSiteQueryCount(), new_txnEntry.getSingleSiteQueryCount());
        assertEquals(orig_txnEntry.getMultiSiteQueryCount(), new_txnEntry.getMultiSiteQueryCount());
        assertEquals(orig_txnEntry.getUnknownQueryCount(), new_txnEntry.getUnknownQueryCount());
        assertEquals(orig_txnEntry.getTotalQueryCount(), new_txnEntry.getTotalQueryCount());
        assertEquals(orig_txnEntry.getAllTouchedPartitionsHistogram(), new_txnEntry.getAllTouchedPartitionsHistogram());
    }
    
    /**
     * testWeightedTxnInvalidateCache
     */
    public void testWeightedTxnInvalidateCache() throws Throwable {
        // Make a new workload that only has a single weighted copy of our multi-partition transaction
        Workload new_workload = new Workload(catalogContext.catalog);
        int weight = 16;
        TransactionTrace multip_txn = this.getMultiPartitionTransaction();
        Procedure catalog_proc = multip_txn.getCatalogItem(catalogContext.database);
        TransactionTrace clone = (TransactionTrace)multip_txn.clone();
        clone.setTransactionId(1000);
        clone.setWeight(weight);
        new_workload.addTransaction(catalog_proc, clone);
        assertEquals(1, new_workload.getTransactionCount());
        
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalogContext);
        final double orig_cost = cost_model.estimateWorkloadCost(catalogContext, new_workload);
        assert(orig_cost > 0);
        
        // Only the base partition should be touched (2 * num_txns). Everything else should
        // be touched num_txns
        Integer base_partition = CollectionUtil.first(cost_model.getQueryPartitionAccessHistogram().getMaxCountValues());
        assertNotNull(base_partition);
        for (Integer p : cost_model.getQueryPartitionAccessHistogram().values()) {
            if (p.equals(base_partition)) {
                assertEquals(2 * weight, cost_model.getQueryPartitionAccessHistogram().get(p).intValue());
            } else {
                assertEquals(weight, cost_model.getQueryPartitionAccessHistogram().get(p).intValue());
            }
        } // FOR
        
        
//        System.err.println(cost_model.debugHistograms(catalogContext.database));
//        System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
        
        // Now invalidate the cache for the first query in the procedure
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        Table catalog_tbl = CollectionUtil.first(CatalogUtil.getReferencedTables(catalog_stmt));
        assertNotNull(catalog_tbl);
        try {
            cost_model.invalidateCache(catalog_tbl);
        } catch (Throwable ex) {
            System.err.println(cost_model.debugHistograms(catalogContext));
            throw ex;
        }
        assertEquals(0, cost_model.getMultiPartitionProcedureHistogram().getSampleCount());
        assertEquals(weight, cost_model.getSinglePartitionProcedureHistogram().getSampleCount());
        assertEquals(weight, cost_model.getQueryPartitionAccessHistogram().getSampleCount());
    }
    
    /**
     * testWeightedQueryInvalidateCache
     */
    public void testWeightedQueryInvalidateCache() throws Throwable {
        // Make a new workload that only has a single weighted copy of our multi-partition transaction
        Workload new_workload = new Workload(catalogContext.catalog);
        int weight = 6;
        TransactionTrace multip_txn = this.getMultiPartitionTransaction();
        Procedure catalog_proc = multip_txn.getCatalogItem(catalogContext.database);
        TransactionTrace clone = (TransactionTrace)multip_txn.clone();
        clone.setTransactionId(1000);
        for (QueryTrace qt : clone.getQueries()) {
            qt.setWeight(weight);
        }
        new_workload.addTransaction(catalog_proc, clone);
        assertEquals(1, new_workload.getTransactionCount());
        assertEquals(multip_txn.getQueryCount(), new_workload.getQueryCount());
        
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalogContext);
        final double orig_cost = cost_model.estimateWorkloadCost(catalogContext, new_workload);
        assert(orig_cost > 0);
        assertEquals(new_workload.getTransactionCount(), cost_model.getMultiPartitionProcedureHistogram().getSampleCount());
        assertEquals(0, cost_model.getSinglePartitionProcedureHistogram().getSampleCount());
        
        // Now invalidate the cache for the first query in the procedure
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        Table catalog_tbl = CollectionUtil.first(CatalogUtil.getReferencedTables(catalog_stmt));
        assertNotNull(catalog_tbl);
        try {
            cost_model.invalidateCache(catalog_tbl);
        } catch (Throwable ex) {
            System.err.println(cost_model.debugHistograms(catalogContext));
            throw ex;
        }
        assertEquals(0, cost_model.getMultiPartitionProcedureHistogram().getSampleCount());
        assertEquals(new_workload.getTransactionCount(), cost_model.getSinglePartitionProcedureHistogram().getSampleCount());
        assertEquals(weight, cost_model.getQueryPartitionAccessHistogram().getSampleCount());
    }
    
    /**
     * testIsAlwaysSinglePartition
     */
    public void testIsAlwaysSinglePartition() throws Exception {
        Map<Procedure, Boolean> expected = new ListOrderedMap<Procedure, Boolean>();
        for (int i = 0; i < TARGET_PROCEDURES.length; i++) {
            expected.put(this.getProcedure(TARGET_PROCEDURES[i]), TARGET_PROCEDURES_SINGLEPARTITION_DEFAULT[i]);
        } // FOR
        expected.put(this.getProcedure(UpdateLocation.class), null);
        assertEquals(TARGET_PROCEDURES.length + 1, expected.size());
        
        // Throw our workload at the costmodel and check that the procedures have the 
        // expected result for whether they are single sited or not
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalogContext);
        cost_model.estimateWorkloadCost(catalogContext, workload);
        
        for (Entry<Procedure, Boolean> e : expected.entrySet()) {
            Boolean sp = cost_model.isAlwaysSinglePartition(e.getKey());
            assertEquals(e.getKey().getName(), e.getValue(), sp);
        } // FOR
    }
    
    /**
     * testHistograms
     */
    public void testHistograms() throws Exception {
        // Grab a txn with a JOIN
        TransactionTrace xact_trace = null;
        for (TransactionTrace xact : workload.getTransactions()) {
            assertNotNull(xact);
            if (xact.getCatalogItemName().equals(TARGET_PROCEDURES[2])) {
                xact_trace = xact;
                break;
            }
        } // FOR
        assertNotNull(xact_trace);

        // Change the catalog so that the data from the tables are in different partitions
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        assertNotNull(clone_db);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        Table catalog_tbl = clone_db.getTables().get(TM1Constants.TABLENAME_CALL_FORWARDING);
        assertNotNull(catalog_tbl);
        Column catalog_col = catalog_tbl.getColumns().get("START_TIME");
        assertNotNull(catalog_col);
        catalog_tbl.setPartitioncolumn(catalog_col);
        
        // Throw the TransactionTrace at the costmodel and make sure that there is a TransactionCacheEntry
        SingleSitedCostModel cost_model = new SingleSitedCostModel(clone_catalogContext);
        cost_model.estimateTransactionCost(clone_catalogContext, xact_trace);
        cost_model.setCachingEnabled(true);
        TransactionCacheEntry entry = cost_model.getTransactionCacheEntry(xact_trace);
        assertNotNull(entry);
//        System.err.println(entry.toString());

        // --------------------------
        // Query Counters
        // --------------------------
        assertEquals(xact_trace.getQueries().size(), entry.getTotalQueryCount());
        assertEquals(xact_trace.getQueries().size(), entry.getExaminedQueryCount());
        assertEquals(0, entry.getSingleSiteQueryCount());
        assertEquals(1, entry.getMultiSiteQueryCount());
        
        // ---------------------------------------------
        // Partitions Touched by Txns
        // This should be all partitions because the query is trying to do a 
        // range look-up on the partitioning column
        // ---------------------------------------------
        Histogram<Integer> txn_h = cost_model.getTxnPartitionAccessHistogram();
        assertNotNull(txn_h);
//        System.err.println(xact_trace.debug(catalogContext.database));
//        System.err.println("Transaction Partitions:\n" + txn_h);
        assertEquals(NUM_PARTITIONS, txn_h.getSampleCount());
        assertEquals(NUM_PARTITIONS, txn_h.getValueCount());

        // ---------------------------------------------
        // Partitions Touched By Queries
        // ---------------------------------------------
        Histogram<Integer> query_h = cost_model.getQueryPartitionAccessHistogram();
        assertNotNull(query_h);
//        System.err.println("Query Partitions:\n" + query_h);
        assertEquals(NUM_PARTITIONS, query_h.getSampleCount());
        assertEquals(NUM_PARTITIONS, query_h.getValueCount());
        
        // ---------------------------------------------
        // Partitions Executing Java Control Code 
        // ---------------------------------------------
        Histogram<Integer> java_h = cost_model.getJavaExecutionHistogram();
        assertNotNull(java_h);
//        System.err.println("Java Execution:\n" + java_h);
        assertEquals(1, java_h.getSampleCount());
        assertEquals(1, java_h.getValueCount());

        // ---------------------------------------------
        // Single-Partition Procedures
        // ---------------------------------------------
        Histogram<String> sproc_h = cost_model.getSinglePartitionProcedureHistogram();
        assertNotNull(sproc_h);
//        System.err.println("SinglePartition:\n" + sproc_h);
        assertEquals(0, sproc_h.getSampleCount());
        assertEquals(0, sproc_h.getValueCount());

        // ---------------------------------------------
        // Multi-Partition Procedures
        // ---------------------------------------------
        Histogram<String> mproc_h = cost_model.getMultiPartitionProcedureHistogram();
        assertNotNull(mproc_h);
//        System.err.println("MultiPartition:\n" + mproc_h);
        assertEquals(1, mproc_h.getSampleCount());
        assertEquals(1, mproc_h.getValueCount());

        // Now throw the same txn back at the costmodel. All of our histograms should come back the same
        cost_model.estimateTransactionCost(catalogContext, xact_trace);

        Histogram<Integer> new_txn_h = cost_model.getTxnPartitionAccessHistogram();
        assertNotNull(new_txn_h);
        assertEquals(txn_h, new_txn_h);

        Histogram<Integer> new_query_h = cost_model.getQueryPartitionAccessHistogram();
        assertNotNull(new_query_h);
        assertEquals(query_h, new_query_h);
        
        Histogram<Integer> new_java_h = cost_model.getJavaExecutionHistogram();
        assertNotNull(new_java_h);
        assertEquals(java_h, new_java_h);
        
        Histogram<String> new_sproc_h = cost_model.getSinglePartitionProcedureHistogram();
        assertNotNull(new_sproc_h);
        assertEquals(sproc_h, new_sproc_h);

        Histogram<String> new_mproc_h = cost_model.getMultiPartitionProcedureHistogram();
        assertNotNull(new_mproc_h);
        assertEquals(mproc_h, new_mproc_h);

    }
    
    /**
     * testEstimateCost
     */
    public void testEstimateCost() throws Exception {
        TransactionTrace xact_trace = null;
        for (TransactionTrace xact : workload.getTransactions()) {
            assertNotNull(xact);
            if (xact.getCatalogItemName().equals(TARGET_PROCEDURES[0])) {
                xact_trace = xact;
                break;
            }
        } // FOR
        assertNotNull(xact_trace);
        
        // System.err.println(xact_trace.debug(catalogContext.database));
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalogContext);
        cost_model.estimateTransactionCost(catalogContext, xact_trace);
        TransactionCacheEntry entry = cost_model.getTransactionCacheEntry(xact_trace);
        assertNotNull(entry);
        
        assertEquals(xact_trace.getQueries().size(), entry.getTotalQueryCount());
        assertEquals(xact_trace.getQueries().size(), entry.getExaminedQueryCount());
        assertEquals(1, entry.getSingleSiteQueryCount());
        assertEquals(1, entry.getMultiSiteQueryCount());
        
        // Check Partition Access Histogram
        Histogram<Integer> hist_access = cost_model.getQueryPartitionAccessHistogram();
        assertNotNull(hist_access);
        assertEquals(NUM_PARTITIONS, hist_access.getValueCount());
        Integer multiaccess_partition = null;
        for (Object o : hist_access.values()) {
            Integer partition = (Integer)o;
            assertNotNull(partition);
            long count = hist_access.get(partition);
            assert(count > 0);
            if (count > 1) multiaccess_partition = partition;
        } // FOR
        assertNotNull(multiaccess_partition);
        
        // Check Java Execution Histogram
        // 2011-03-23
        // This always going to be empty because of how we store null ProcParameters...
        Histogram<Integer> hist_execute = cost_model.getJavaExecutionHistogram();
        assertNotNull(hist_execute);
        // System.err.println("HISTOGRAM:\n" + hist_execute);
        // assertEquals(0, hist_execute.getValueCount());
    }
    
    /**
     * testEstimateCostInvalidateCache
     */
    public void testEstimateCostInvalidateCache() throws Exception {
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalogContext);
        
        List<TransactionTrace> xacts = new ArrayList<TransactionTrace>();
        for (TransactionTrace xact_trace : workload.getTransactions()) {
            assertNotNull(xact_trace);
            if (xact_trace.getCatalogItemName().equals(TARGET_PROCEDURES[0])) {
                cost_model.estimateTransactionCost(catalogContext, xact_trace);
                xacts.add(xact_trace);
            }
        } // FOR
        
        // Now invalidate the cache for the SUBSCRIBER and CALL_FORWARDING table
        // This will cause the Txn entry to be thrown out completely because all the
        // queries have been invalidated
        cost_model.invalidateCache(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        cost_model.invalidateCache(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING));
        
        TransactionTrace xact_trace = xacts.get(0);
        assertNotNull(xact_trace);
        TransactionCacheEntry entry = cost_model.getTransactionCacheEntry(xact_trace);
        assertNull(entry);
        
        // Make sure that we updated the Execution Histogram
        Histogram<Integer> hist = cost_model.getJavaExecutionHistogram();
        assert(hist.isEmpty());
        
        // And make sure that we updated the Partition Access Histogram
        hist = cost_model.getQueryPartitionAccessHistogram();
        assert(hist.isEmpty());
    }
    
    /**
     * testEstimateCostInvalidateCachePartial
     */
    public void testEstimateCostInvalidateCachePartial() throws Exception {
        // DeleteCallForwarding has two queries, one that touches SUBSCRIBER and one that
        // touches CALL_FORWARDING. So we're going to invalidate SUBSCRIBER and check to make
        // sure that the our cache still has information for the second query but not the first.
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalogContext);
        Procedure catalog_proc = this.getProcedure(DeleteCallForwarding.class);
        Statement valid_stmt = catalog_proc.getStatements().get("update");
        assertNotNull(valid_stmt);
        Statement invalid_stmt = catalog_proc.getStatements().get("query");
        assertNotNull(invalid_stmt);
        
        List<TransactionTrace> xacts = new ArrayList<TransactionTrace>();
        for (TransactionTrace xact_trace : workload.getTransactions()) {
            assertNotNull(xact_trace);
            if (xact_trace.getCatalogItemName().equals(catalog_proc.getName())) {
                cost_model.estimateTransactionCost(catalogContext, xact_trace);
                xacts.add(xact_trace);
            }
        } // FOR
        assertFalse(xacts.isEmpty());

        // Invalidate that mofo!
        cost_model.invalidateCache(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        
        Histogram<Integer> expected_touched = new ObjectHistogram<Integer>();
        for (TransactionTrace txn_trace : xacts) {
            TransactionCacheEntry txn_entry = cost_model.getTransactionCacheEntry(txn_trace);
            assertNotNull(txn_entry);
//            expected_touched.put(txn_entry.getExecutionPartition());
            
            for (QueryCacheEntry query_entry : cost_model.getQueryCacheEntries(txn_trace)) {
                QueryTrace query_trace = txn_trace.getQueries().get(query_entry.getQueryIdx());
                assertNotNull(query_trace);
                assertNotNull(query_trace.getCatalogItemName());
                Boolean should_be_invalid = (query_trace.getCatalogItemName().equals(invalid_stmt.getName()) ? true :
                                            (query_trace.getCatalogItemName().equals(valid_stmt.getName()) ? false : null));
                assertNotNull(should_be_invalid);
                assertEquals(should_be_invalid.booleanValue(), query_entry.isInvalid());
                
                if (should_be_invalid) {
                    assert(query_entry.getAllPartitions().isEmpty());
                } else {
                    assertFalse(query_entry.getAllPartitions().isEmpty());
                    expected_touched.put(query_entry.getAllPartitions());
                }
            } // FOR
            
            assertEquals(catalog_proc.getStatements().size(), txn_entry.getTotalQueryCount());
            assert(txn_entry.isSinglePartitioned());
            assertFalse(txn_entry.isComplete());
            assertEquals(1, txn_entry.getExaminedQueryCount());
            assertEquals(0, txn_entry.getMultiSiteQueryCount());
            assertEquals(1, txn_entry.getSingleSiteQueryCount());
        } // FOR
        
        // Grab the histograms about what partitions got touched and make sure that they are updated properly
        Histogram<Integer> txn_partitions = cost_model.getTxnPartitionAccessHistogram();
        Histogram<Integer> query_partitions = cost_model.getQueryPartitionAccessHistogram();

        // The txn touched partitions histogram should now only contain the entries from
        // the single-sited query (plus the java execution) and not all of the partitions
        // from the broadcast query
        assertEquals(expected_touched.getValueCount(), txn_partitions.getValueCount());
        assertEquals(xacts.size(), query_partitions.getSampleCount());
    }
    
    /**
     * testProcParameterEstimate
     */
    public void testProcParameterEstimate() throws Exception {
        // So here we want to throw a txn at the cost model first without a partitioning ProcParameter
        // and then with one. We should see that the TransactionCacheEntry gets updated properly
        // and that the txn becomes multi-sited
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        assertNotNull(clone_db);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog()); 
        
        Procedure catalog_proc = this.getProcedure(clone_db, GetAccessData.class);
        TransactionTrace target_txn = null;
        for (TransactionTrace txn : workload.getTransactions()) {
            if (txn.getCatalogItemName().equals(catalog_proc.getName())) {
                target_txn = txn;
                break;
            }
        } // FOR
        assertNotNull(target_txn);
        TransactionCacheEntry entry = null;
        int orig_partition_parameter = catalog_proc.getPartitionparameter();
        
        // Ok, now let's disable the ProcParameter
        SingleSitedCostModel cost_model = new SingleSitedCostModel(clone_catalogContext);
        catalog_proc.setPartitionparameter(NullProcParameter.PARAM_IDX);
        cost_model.setCachingEnabled(true);
        cost_model.estimateTransactionCost(clone_catalogContext, target_txn);
        entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(entry);
        assertEquals(HStoreConstants.NULL_PARTITION_ID, entry.getExecutionPartition());
        assert(entry.isSinglePartitioned());
        
        // Make something else the ProcParameter
        cost_model.invalidateCache(catalog_proc);
        catalog_proc.setPartitionparameter(orig_partition_parameter + 1);
        cost_model.getPartitionEstimator().initCatalog(clone_catalogContext);
        cost_model.estimateTransactionCost(clone_catalogContext, target_txn);
        entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(entry);
        assert(entry.getExecutionPartition() != HStoreConstants.NULL_PARTITION_ID);
        // assertFalse(entry.isSingleSited());
        
        // Now let's put S_ID back in as the ProcParameter
        cost_model.invalidateCache(catalog_proc);
        catalog_proc.setPartitionparameter(orig_partition_parameter);
        cost_model.getPartitionEstimator().initCatalog(clone_catalogContext);
        cost_model.estimateTransactionCost(clone_catalogContext, target_txn);
        entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(entry);
        assert(entry.getExecutionPartition() != HStoreConstants.NULL_PARTITION_ID);
        if (!entry.isSinglePartitioned()) System.err.println(entry.debug());
        assert(entry.isSinglePartitioned());
    }
    
    /**
     * testMultiColumnPartitioning
     */
    public void testMultiColumnPartitioning() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        Procedure catalog_proc = this.getProcedure(clone_db, GetAccessData.class);
        TransactionTrace target_txn = null;
        for (TransactionTrace txn : workload.getTransactions()) {
            if (txn.getCatalogItemName().equals(catalog_proc.getName())) {
                target_txn = txn;
                break;
            }
        } // FOR
        assertNotNull(target_txn);
        System.err.println(target_txn.debug(catalogContext.database));
        
        // Now change partitioning
        MultiProcParameter catalog_param = MultiProcParameter.get(catalog_proc.getParameters().get(0), catalog_proc.getParameters().get(1));
        assertNotNull(catalog_param);
        catalog_proc.setPartitionparameter(catalog_param.getIndex());
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_ACCESS_INFO);
        Column columns[] = {
            this.getColumn(clone_db, catalog_tbl, "S_ID"),
            this.getColumn(clone_db, catalog_tbl, "AI_TYPE"),
        };
        MultiColumn catalog_col = MultiColumn.get(columns);
        assertNotNull(catalog_col);
        catalog_tbl.setPartitioncolumn(catalog_col);
//        System.err.println(catalog_tbl + ": " + catalog_col);

        SingleSitedCostModel cost_model = new SingleSitedCostModel(clone_catalogContext);
        cost_model.setCachingEnabled(true);
        cost_model.estimateTransactionCost(clone_catalogContext, target_txn);
        TransactionCacheEntry txn_entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(txn_entry);
        assertNotNull(txn_entry.getExecutionPartition());
        assert(txn_entry.isSinglePartitioned());
//        System.err.println(txn_entry.debug());
    }
}
