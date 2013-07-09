package edu.brown.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.catalog.CatalogCloner;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.hstore.HStoreConstants;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * 
 * @author pavlo
 *
 */
public class TestPartitionEstimatorMultiSite extends BaseTestCase {

    protected static AbstractHasher hasher;
    protected static final int num_partitions = 10;
    protected static final int base_partition = 1;
    
    private final PartitionSet partitions = new PartitionSet();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        if (hasher == null) {
            this.addPartitions(num_partitions);
            hasher = new DefaultHasher(catalogContext);
        }
    }
    
    /**
     * testGetPartitionsTransactionTrace
     */
    public void testGetPartitionsTransactionTrace() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        Object txn_params[] = new Object[] { 1l, 4l, 0l, 5l };
        
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetData");
        Object query_params[] = new Object[] { 1l, 1l, 4l, 0l, 5l };
        
        TransactionTrace txn_trace = new TransactionTrace(1001l, catalog_proc, txn_params);
        QueryTrace query_trace = new QueryTrace(catalog_stmt, query_params, 0);
        txn_trace.addQuery(query_trace);
        txn_trace.stop();
        
        p_estimator.getAllPartitions(partitions, txn_trace);
        assertNotNull(partitions);
        assertEquals(partitions.toString(), 1, partitions.size());
    }
    
    /**
     * testGetPartitionsProcedure
     */
    public void testGetPartitionsProcedure() throws Exception {
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        Object params[] = new Object[] {
            new Long(1000),     // S_ID
            0l,                 // BIT_1
            0l,                 // DATA_A
            0l,                 // SF_TYPE
        };
        assert(p_estimator.getBasePartition(catalog_proc, params) != HStoreConstants.NULL_PARTITION_ID);
    }
    
    /**
     * testGetPartitionsStatement
     */
    public void testGetPartitionsStatement() throws Exception {
        Procedure catalog_proc = this.getProcedure("InsertCallForwarding");
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get("query1");
        assertNotNull(catalog_stmt);
        
        Object params[] = new Object[] { new String("Doesn't Matter") };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, base_partition);
        // System.out.println(catalog_stmt.getName() + " Partitions: " + partitions);
        assertEquals(num_partitions, partitions.size());
    }
    
    /**
     * testGetPartitionsPlanFragment
     */
    public void testGetPartitionsPlanFragment() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        Statement catalog_stmt = catalog_proc.getStatements().get("GetData");
        assertNotNull(catalog_stmt);
        
        // System.out.println("Num Partitions: " + CatalogUtil.getNumberOfPartitions(catalog_db));
        Object params[] = new Object[] {
            new Long(54),   // S_ID
            new Long(3),    // SF_TYPE
            new Long(0),    // START_TIME
            new Long(22),   // END_TIME
        };
        
        // We should see one PlanFragment with no partitions and then all others need something
        boolean internal_flag = false;
        for (PlanFragment catalog_frag : catalog_stmt.getMs_fragments()) {
            partitions.clear();
            p_estimator.getPartitions(partitions, catalog_frag, params, base_partition);
            if (partitions.isEmpty()) {
                assertFalse(internal_flag);
                internal_flag = true;
            }
        } // FOR
        assertTrue(internal_flag);
    }
    
    /**
     * testGetAllFragmentPartitions
     */
    public void testGetAllFragmentPartitions() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        Statement catalog_stmt = catalog_proc.getStatements().get("GetData");
        assertNotNull(catalog_stmt);
        
        // System.out.println("Num Partitions: " + CatalogUtil.getNumberOfPartitions(catalog_db));
        Object params[] = new Object[] {
            new Long(54),   // S_ID
            new Long(3),    // SF_TYPE
            new Long(0),    // START_TIME
            new Long(22),   // END_TIME
        };

        Map<PlanFragment, PartitionSet> all_partitions = new HashMap<PlanFragment, PartitionSet>();
        CatalogMap<PlanFragment> fragments = catalog_stmt.getMs_fragments();
        p_estimator.getAllFragmentPartitions(all_partitions, fragments.values(), params, base_partition);
        
        // We should see one PlanFragment with that only has our local partition and then all others need something
        boolean internal_flag = false;
        for (PlanFragment catalog_frag : fragments) {
            Collection<Integer> partitions = all_partitions.get(catalog_frag);
            assertNotNull(catalog_frag.fullName(), partitions);
            if (partitions.size() == 1 && partitions.contains(base_partition)) {
                assertFalse(internal_flag);
                internal_flag = true;
            }
        } // FOR
        assertTrue(internal_flag);
    }
    
    /**
     * testMultiColumnPartitioning
     */
    public void testMultiColumnPartitioning() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog()); 
        PartitionEstimator p_estimator = new PartitionEstimator(clone_catalogContext);

        Procedure catalog_proc = this.getProcedure(clone_db, GetNewDestination.class);
        ProcParameter catalog_params[] = new ProcParameter[] {
            this.getProcParameter(clone_db, catalog_proc, 0),   // S_ID
            this.getProcParameter(clone_db, catalog_proc, 1),   // SF_TYPE
        };
        MultiProcParameter mpp = MultiProcParameter.get(catalog_params);
        assertNotNull(mpp);
        assert(mpp.getIndex() >= 0);
        catalog_proc.setPartitionparameter(mpp.getIndex());
        
        // First Test: Only partition one of the tables and make sure that
        // the MultiColumns don't map to the same partition
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_SPECIAL_FACILITY);
        Column catalog_cols[] = new Column[] {
            this.getColumn(clone_db, catalog_tbl, "S_ID"),
            this.getColumn(clone_db, catalog_tbl, "SF_TYPE"),
        };
        MultiColumn mc = MultiColumn.get(catalog_cols);
        assertNotNull(mc);
        catalog_tbl.setPartitioncolumn(mc);
        p_estimator.initCatalog(clone_catalogContext);
        
        Statement catalog_stmt = this.getStatement(clone_db, catalog_proc, "GetData");
        Long params[] = new Long[] {
            new Long(1111), // S_ID
            new Long(1111), // S_ID
            new Long(2222), // SF_TYPE
            new Long(3333), // START_TIME
            new Long(4444), // END_TIME
        };
        Map<String, PartitionSet> partitions = p_estimator.getTablePartitions(catalog_stmt, params, base_partition);
        assertNotNull(partitions);
        assertFalse(partitions.isEmpty());
        
        PartitionSet touched = new PartitionSet();
        for (String table_key : partitions.keySet()) {
            assertFalse(table_key, partitions.get(table_key).isEmpty());
            touched.addAll(partitions.get(table_key));
        } // FOR
        assertEquals(2, touched.size());
        
        // Second Test: Now make the other table multi-column partitioned and make
        // sure that the query goes to just one partition
        
        catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_CALL_FORWARDING);
        catalog_cols = new Column[] {
            this.getColumn(clone_db, catalog_tbl, "S_ID"),
            this.getColumn(clone_db, catalog_tbl, "SF_TYPE"),
        };
        mc = MultiColumn.get(catalog_cols);
        assertNotNull(mc);
        catalog_tbl.setPartitioncolumn(mc);
        p_estimator.initCatalog(clone_catalogContext);

        partitions = p_estimator.getTablePartitions(catalog_stmt, params, base_partition);
        assertNotNull(partitions);
        assertFalse(partitions.isEmpty());
        
        touched.clear();
        for (String table_key : partitions.keySet()) {
            assertFalse(table_key, partitions.get(table_key).isEmpty());
            touched.addAll(partitions.get(table_key));
        } // FOR
        assertEquals(1, touched.size());
    }
    
    /**
     * testMultiColumnPartitioningIncomplete
     */
    public void testMultiColumnPartitioningIncomplete() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog()); 
        PartitionEstimator p_estimator = new PartitionEstimator(clone_catalogContext);

        Procedure catalog_proc = this.getProcedure(clone_db, GetAccessData.class);
        ProcParameter catalog_params[] = new ProcParameter[] {
            this.getProcParameter(clone_db, catalog_proc, 0),   // S_ID
            this.getProcParameter(clone_db, catalog_proc, 1),   // AI_TYPE
        };
        MultiProcParameter mpp = MultiProcParameter.get(catalog_params);
        assertNotNull(mpp);
        assert(mpp.getIndex() >= 0);
        catalog_proc.setPartitionparameter(mpp.getIndex());
        p_estimator.initCatalog(clone_catalogContext);
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_ACCESS_INFO);
        Column catalog_cols[] = new Column[] {
            this.getColumn(clone_db, catalog_tbl, "S_ID"),
            this.getColumn(clone_db, catalog_tbl, "DATA1"),
        };
        MultiColumn mc = MultiColumn.get(catalog_cols);
        assertNotNull(mc);
        catalog_tbl.setPartitioncolumn(mc);
        p_estimator.initCatalog(clone_catalogContext);
        
        Statement catalog_stmt = this.getStatement(clone_db, catalog_proc, "GetData");
        Long params[] = new Long[] {
            new Long(1111), // S_ID
            new Long(2222), // AI_TYPE
        };
        Map<String, PartitionSet> partitions = p_estimator.getTablePartitions(catalog_stmt, params, base_partition);
        assertNotNull(partitions);
        assertFalse(partitions.isEmpty());
        // System.err.println("partition = " + partitions);
        
        PartitionSet touched = new PartitionSet();
        for (String table_key : partitions.keySet()) {
            assertFalse(table_key, partitions.get(table_key).isEmpty());
            touched.addAll(partitions.get(table_key));
        } // FOR
        assertEquals(num_partitions, touched.size());
    }
}
