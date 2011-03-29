package edu.brown.costmodel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestSingleSitedCostModel extends BaseTestCase {

    private static final int PROC_COUNT = 3;
    private static final String TARGET_PROCEDURES[] = {
        DeleteCallForwarding.class.getSimpleName(),
        GetAccessData.class.getSimpleName(),
        GetNewDestination.class.getSimpleName(),
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
        
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.TM1); 
            workload = new Workload(catalog);
            
            // Workload Filter
            ProcedureNameFilter filter = new ProcedureNameFilter();
            long total = 0;
            for (String proc_name : TARGET_PROCEDURES) {
                filter.include(proc_name, PROC_COUNT);
                total += PROC_COUNT;
            }
            ((Workload)workload).load(workload_file.getAbsolutePath(), catalog_db, filter);
            assertEquals(total, workload.getTransactionCount());
            assertEquals(TARGET_PROCEDURES.length, workload.getProcedureHistogram().getValueCount());
            // System.err.println(workload.getProcedureHistogram());
        }
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
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalog_db);
        cost_model.estimateCost(catalog_db, workload);
        
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
        Database clone_db = CatalogUtil.cloneDatabase(catalog_db);
        assertNotNull(clone_db);
        Table catalog_tbl = clone_db.getTables().get(TM1Constants.TABLENAME_CALL_FORWARDING);
        assertNotNull(catalog_tbl);
        Column catalog_col = catalog_tbl.getColumns().get("START_TIME");
        assertNotNull(catalog_col);
        catalog_tbl.setPartitioncolumn(catalog_col);
        
        // Throw the TransactionTrace at the costmodel and make sure that there is a TransactionCacheEntry
        SingleSitedCostModel cost_model = new SingleSitedCostModel(clone_db);
        cost_model.estimateTransactionCost(clone_db, xact_trace);
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
        // ---------------------------------------------
        Histogram txn_h = cost_model.getTxnPartitionAccessHistogram();
        assertNotNull(txn_h);
//        System.err.println("Transaction Partitions:\n" + txn_h);
        assertEquals(2, txn_h.getSampleCount());
        assertEquals(2, txn_h.getValueCount());

        // ---------------------------------------------
        // Partitions Touched By Queries
        // ---------------------------------------------
        Histogram query_h = cost_model.getQueryPartitionAccessHistogram();
        assertNotNull(query_h);
//        System.err.println("Query Partitions:\n" + query_h);
        assertEquals(2, query_h.getSampleCount());
        assertEquals(2, query_h.getValueCount());
        
        // ---------------------------------------------
        // Partitions Executing Java Control Code 
        // ---------------------------------------------
        Histogram java_h = cost_model.getJavaExecutionHistogram();
        assertNotNull(java_h);
//        System.err.println("Java Execution:\n" + java_h);
        assertEquals(1, java_h.getSampleCount());
        assertEquals(1, java_h.getValueCount());

        // ---------------------------------------------
        // Single-Partition Procedures
        // ---------------------------------------------
        Histogram sproc_h = cost_model.getSinglePartitionProcedureHistogram();
        assertNotNull(sproc_h);
//        System.err.println("SinglePartition:\n" + sproc_h);
        assertEquals(0, sproc_h.getSampleCount());
        assertEquals(0, sproc_h.getValueCount());

        // ---------------------------------------------
        // Multi-Partition Procedures
        // ---------------------------------------------
        Histogram mproc_h = cost_model.getMultiPartitionProcedureHistogram();
        assertNotNull(mproc_h);
//        System.err.println("MultiPartition:\n" + mproc_h);
        assertEquals(1, mproc_h.getSampleCount());
        assertEquals(1, mproc_h.getValueCount());

        // Now throw the same txn back at the costmodel. All of our histograms should come back the same
        cost_model.estimateTransactionCost(clone_db, xact_trace);

        Histogram new_txn_h = cost_model.getTxnPartitionAccessHistogram();
        assertNotNull(new_txn_h);
        assertEquals(txn_h, new_txn_h);

        Histogram new_query_h = cost_model.getQueryPartitionAccessHistogram();
        assertNotNull(new_query_h);
        assertEquals(query_h, new_query_h);
        
        Histogram new_java_h = cost_model.getJavaExecutionHistogram();
        assertNotNull(new_java_h);
        assertEquals(java_h, new_java_h);
        
        Histogram new_sproc_h = cost_model.getSinglePartitionProcedureHistogram();
        assertNotNull(new_sproc_h);
        assertEquals(sproc_h, new_sproc_h);

        Histogram new_mproc_h = cost_model.getMultiPartitionProcedureHistogram();
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
        
        // System.err.println(xact_trace.debug(catalog_db));
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalog_db);
        cost_model.estimateTransactionCost(catalog_db, xact_trace);
        TransactionCacheEntry entry = cost_model.getTransactionCacheEntry(xact_trace);
        assertNotNull(entry);
        
        assertEquals(xact_trace.getQueries().size(), entry.getTotalQueryCount());
        assertEquals(xact_trace.getQueries().size(), entry.getExaminedQueryCount());
        assertEquals(1, entry.getSingleSiteQueryCount());
        assertEquals(1, entry.getMultiSiteQueryCount());
        
        // Check Partition Access Histogram
        Histogram hist_access = cost_model.getQueryPartitionAccessHistogram();
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
        Histogram hist_execute = cost_model.getJavaExecutionHistogram();
        assertNotNull(hist_execute);
        // System.err.println("HISTOGRAM:\n" + hist_execute);
        assertEquals(0, hist_execute.getValueCount());
    }
    
    /**
     * testEstimateCostInvalidateCache
     */
    public void testEstimateCostInvalidateCache() throws Exception {
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalog_db);
        
        List<TransactionTrace> xacts = new ArrayList<TransactionTrace>();
        for (TransactionTrace xact_trace : workload.getTransactions()) {
            assertNotNull(xact_trace);
            if (xact_trace.getCatalogItemName().equals(TARGET_PROCEDURES[0])) {
                cost_model.estimateTransactionCost(catalog_db, xact_trace);
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
        Histogram hist = cost_model.getJavaExecutionHistogram();
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
        SingleSitedCostModel cost_model = new SingleSitedCostModel(catalog_db);
        Procedure catalog_proc = this.getProcedure(DeleteCallForwarding.class);
        Statement valid_stmt = catalog_proc.getStatements().get("update");
        assertNotNull(valid_stmt);
        Statement invalid_stmt = catalog_proc.getStatements().get("query");
        assertNotNull(invalid_stmt);
        
        List<TransactionTrace> xacts = new ArrayList<TransactionTrace>();
        for (TransactionTrace xact_trace : workload.getTransactions()) {
            assertNotNull(xact_trace);
            if (xact_trace.getCatalogItemName().equals(catalog_proc.getName())) {
                cost_model.estimateTransactionCost(catalog_db, xact_trace);
                xacts.add(xact_trace);
            }
        } // FOR
        assertFalse(xacts.isEmpty());

        // Invalidate that mofo!
        cost_model.invalidateCache(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        
        Histogram expected_touched = new Histogram();
        for (TransactionTrace txn_trace : xacts) {
            TransactionCacheEntry txn_entry = cost_model.getTransactionCacheEntry(txn_trace);
            assertNotNull(txn_entry);
//            expected_touched.put(txn_entry.getExecutionPartition());
            
            for (QueryCacheEntry query_entry : cost_model.getQueryCacheEntries(txn_trace)) {
                QueryTrace query_trace = txn_trace.getQuery(query_entry.getQueryId());
                assertNotNull(query_trace);
                Boolean should_be_invalid = (query_trace.getCatalogItemName().equals(invalid_stmt.getName()) ? true :
                                            (query_trace.getCatalogItemName().equals(valid_stmt.getName()) ? false : null));
                assertNotNull(should_be_invalid);
                assertEquals(should_be_invalid.booleanValue(), query_entry.isInvalid());
                
                if (should_be_invalid) {
                    assert(query_entry.getAllPartitions().isEmpty());
                } else {
                    assertFalse(query_entry.getAllPartitions().isEmpty());
                    expected_touched.putAll(query_entry.getAllPartitions());
                }
            } // FOR
            
            assertEquals(catalog_proc.getStatements().size(), txn_entry.getTotalQueryCount());
            assert(txn_entry.isSingleSited());
            assertFalse(txn_entry.isComplete());
            assertEquals(1, txn_entry.getExaminedQueryCount());
            assertEquals(0, txn_entry.getMultiSiteQueryCount());
            assertEquals(1, txn_entry.getSingleSiteQueryCount());
        } // FOR
        
        // Grab the histograms about what partitions got touched and make sure that they are updated properly
        Histogram txn_partitions = cost_model.getTxnPartitionAccessHistogram();
        Histogram query_partitions = cost_model.getQueryPartitionAccessHistogram();

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
        Database clone_db = CatalogUtil.cloneDatabase(catalog_db);
        assertNotNull(clone_db);
        
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
        SingleSitedCostModel cost_model = new SingleSitedCostModel(clone_db);
        catalog_proc.setPartitionparameter(NullProcParameter.PARAM_IDX);
        cost_model.setCachingEnabled(true);
        cost_model.estimateTransactionCost(clone_db, target_txn);
        entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(entry);
        assertNull(entry.getExecutionPartition());
        assert(entry.isSingleSited());
        
        // Make something else the ProcParameter
        cost_model.invalidateCache(catalog_proc);
        catalog_proc.setPartitionparameter(orig_partition_parameter + 1);
        cost_model.getPartitionEstimator().initCatalog(clone_db);
        cost_model.estimateTransactionCost(clone_db, target_txn);
        entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(entry);
        assertNotNull(entry.getExecutionPartition());
        assertFalse(entry.isSingleSited());
        
        // Now let's put S_ID back in as the ProcParameter
        cost_model.invalidateCache(catalog_proc);
        catalog_proc.setPartitionparameter(orig_partition_parameter);
        cost_model.getPartitionEstimator().initCatalog(clone_db);
        cost_model.estimateTransactionCost(clone_db, target_txn);
        entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(entry);
        assertNotNull(entry.getExecutionPartition());
        if (!entry.isSingleSited()) System.err.println(entry.debug());
        assert(entry.isSingleSited());
    }
    
    /**
     * testMultiColumnPartitioning
     */
    public void testMultiColumnPartitioning() throws Exception {
        Database clone_db = CatalogUtil.cloneDatabase(catalog_db);
        Procedure catalog_proc = this.getProcedure(clone_db, GetAccessData.class);
        TransactionTrace target_txn = null;
        for (TransactionTrace txn : workload.getTransactions()) {
            if (txn.getCatalogItemName().equals(catalog_proc.getName())) {
                target_txn = txn;
                break;
            }
        } // FOR
        assertNotNull(target_txn);
        
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

        SingleSitedCostModel cost_model = new SingleSitedCostModel(clone_db);
        cost_model.setCachingEnabled(true);
        cost_model.estimateTransactionCost(clone_db, target_txn);
        TransactionCacheEntry txn_entry = cost_model.getTransactionCacheEntry(target_txn);
        assertNotNull(txn_entry);
        assertNotNull(txn_entry.getExecutionPartition());
        assert(txn_entry.isSingleSited());
//        System.err.println(txn_entry.debug());
    }
}
