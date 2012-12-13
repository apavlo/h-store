package edu.brown.designer.partitioners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.CatalogContext;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.InsertCallForwarding;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogCloner;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.TestAbstractPartitioner.MockPartitioner;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * 
 * @author pavlo
 */
public class TestVerticalPartitionerUtil extends BasePartitionerTestCase {

    private MockPartitioner partitioner;
    private AccessGraph agraph;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new SingleSitedCostModel(catalogContext));
        this.info.setPartitionerClass(MockPartitioner.class);
        assertNotNull(info.getStats());
        
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (MockPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, this.partitioner.generateAccessGraph());
        assertNotNull(this.agraph);
        
        // HACK: Assign partitioning ProcParameters for Procedures that don't have one
        if (isFirstSetup()) {
            for (Procedure catalog_proc : catalog_db.getProcedures()) {
                if (catalog_proc.getParameters().size() > 0 && catalog_proc.getPartitionparameter() == NullProcParameter.PARAM_IDX) {
                    catalog_proc.setPartitionparameter(this.getProcParameter(catalog_proc, 0).getIndex());
                }
            } // FOR
        }
    }
    
    private Map<String, Object> generateFieldMap(Statement catalog_stmt) {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (String f : catalog_stmt.getFields()) {
            Object val = catalog_stmt.getField(f);
            if (f.endsWith("exptree") || f.endsWith("fullplan")) {
                val = StringUtil.md5sum(val.toString());
            }
            m.put(f, val);
        } // FOR
        return (m);
    }
    
    /**
     * testTimeIntervalCostModel
     */
    public void testTimeIntervalCostModel() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog()); 
        
        info = this.generateInfo(clone_catalogContext);
        TimeIntervalCostModel<SingleSitedCostModel> costModel = new TimeIntervalCostModel<SingleSitedCostModel>(clone_catalogContext, SingleSitedCostModel.class, 10);
        costModel.setCachingEnabled(true);
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(target_col, info.stats);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionColumn vpc = CollectionUtil.first(candidates);
        assertNotNull(vpc);
        assertFalse(vpc.isUpdateApplied());

        // Create a filter that only has the procedures that will be optimized by our VerticalPartitionColumn
        ProcedureNameFilter filter = new ProcedureNameFilter(false);
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            filter.include(catalog_stmt.getParent().getName(), 1);
        } // FOR
        
        // Calculate the cost *BEFORE* applying the vertical partition optimization
        double expected_cost = costModel.estimateWorkloadCost(clone_catalogContext, workload, filter, null);
        System.err.println("ORIGINAL COST: " + expected_cost);
        
        // Now apply the update and get the new cost. It should be lower
        // We have to clear the cache for these queries first though
        vpc.applyUpdate();
        costModel.invalidateCache(vpc.getOptimizedQueries());
        double new_cost = costModel.estimateWorkloadCost(clone_catalogContext, workload, filter, null);
        System.err.println("NEW COST: " + new_cost);
        assert(new_cost < expected_cost) : String.format("%f < %f", new_cost, expected_cost);
    }
    
    /**
     * testSingleSitedCostModel
     */
    public void testSingleSitedCostModel() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog()); 
        
        info = this.generateInfo(clone_catalogContext);
        SingleSitedCostModel costModel = new SingleSitedCostModel(clone_catalogContext);
        costModel.setCachingEnabled(true);
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(target_col, info.stats);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionColumn vpc = CollectionUtil.first(candidates);
        assertNotNull(vpc);
        assertFalse(vpc.isUpdateApplied());

        // Create a filter that only has the procedures that will be optimized by our VerticalPartitionColumn
        ProcedureNameFilter filter = new ProcedureNameFilter(false);
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            filter.include(catalog_stmt.getParent().getName(), 1);
        } // FOR
        
        // Calculate the cost *BEFORE* applying the vertical partition optimization
        double expected_cost = costModel.estimateWorkloadCost(clone_catalogContext, workload, filter, null);
        System.err.println("ORIGINAL COST: " + expected_cost);
        Map<Long, TransactionCacheEntry> expected_entries = new HashMap<Long, TransactionCacheEntry>(); 
        for (TransactionCacheEntry txn_entry : costModel.getTransactionCacheEntries()) {
            // There should be no unknown queries and all transactions should be multi-sited
            assertEquals(txn_entry.toString(), 0, txn_entry.getUnknownQueryCount());
            assertFalse(txn_entry.isSinglePartitioned());
            
            TransactionCacheEntry clone = (TransactionCacheEntry)txn_entry.clone();
            assertNotSame(txn_entry, clone);
            expected_entries.put(txn_entry.getTransactionId(), clone);
            // System.err.println(StringUtil.columns(txn_entry.debug(), clone.debug()));
            // System.err.println(StringUtil.SINGLE_LINE);
        } // FOR
        assertFalse(expected_entries.isEmpty());
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            Collection<QueryCacheEntry> entries = costModel.getQueryCacheEntries(catalog_stmt);
            assertNotNull(entries);
            assertFalse(entries.isEmpty());
        } // FOR
        
        // Now apply the update and get the new cost. We don't care what the cost
        // is because SingleSitedCostModel only looks to see whether a txn is single-partition
        // and not how many partition it actually touches 
        // We have to clear the cache for these queries first though
        vpc.applyUpdate();
        costModel.invalidateCache(vpc.getOptimizedQueries());
        double new_cost = costModel.estimateWorkloadCost(clone_catalogContext, workload, filter, null);
        System.err.println("NEW COST: " + new_cost);
        Collection<TransactionCacheEntry> new_entries = costModel.getTransactionCacheEntries();
        assertNotNull(new_entries);
        assertEquals(expected_entries.size(), new_entries.size());
        for (TransactionCacheEntry txn_entry : costModel.getTransactionCacheEntries()) {
            TransactionCacheEntry expected = expected_entries.get(txn_entry.getTransactionId());
            assertNotNull(expected);
            
            assertEquals(expected.getUnknownQueryCount(), txn_entry.getUnknownQueryCount());
            assertEquals(expected.getExaminedQueryCount(), txn_entry.getExaminedQueryCount());
            assertEquals(expected.getTotalQueryCount(), txn_entry.getTotalQueryCount());
            assertEquals(expected.getExecutionPartition(), txn_entry.getExecutionPartition());
            
            assertThat(expected.getMultiSiteQueryCount(), not(equalTo(txn_entry.getMultiSiteQueryCount())));
            assertThat(expected.getSingleSiteQueryCount(), not(equalTo(txn_entry.getSingleSiteQueryCount())));
            
            // None of the queries should touch all of the partitions
            for (QueryCacheEntry query_entry : costModel.getQueryCacheEntries(txn_entry.getTransactionId())) {
                assertNotNull(query_entry);
                assertFalse(query_entry.isInvalid());
                assertFalse(query_entry.isUnknown());
                assertEquals(1, query_entry.getAllPartitions().size());
            } // FOR
        } // FOR
    }
    
    /**
     * testPartitionEstimator
     */
    public void testPartitionEstimator() throws Exception {
        Integer base_partition = 1;
        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog()); 
        PartitionEstimator p_estimator = new PartitionEstimator(clone_catalogContext);
        info = this.generateInfo(clone_catalogContext);
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(target_col, info.stats);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionColumn vpc = CollectionUtil.first(candidates);
        assertNotNull(vpc);
        assertFalse(vpc.isUpdateApplied());
        
        // Get the original partitions for the queries before we apply the optimizations
        Map<Statement, Object[]> stmt_params = new HashMap<Statement, Object[]>();
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            // We first need to generate random input parameters
            Object params[] = new Object[catalog_stmt.getParameters().size()];
            for (int i = 0; i < params.length; i++) {
                StmtParameter catalog_param = catalog_stmt.getParameters().get(i);
                VoltType vtype = VoltType.get(catalog_param.getJavatype()); 
                params[i] = VoltTypeUtil.getRandomValue(vtype);
            } // FOR
            stmt_params.put(catalog_stmt, params);
            
            // Then get the list of partitions that it will access
            // This should always be *all* partitions
            PartitionSet partitions = new PartitionSet();
            p_estimator.getAllPartitions(partitions, catalog_stmt, params, base_partition);
            assertNotNull(partitions);
            assertEquals(CatalogUtil.getNumberOfPartitions(clone_db), partitions.size());
        } // FOR
        
        // Now apply the optimized queries
        // The number of partitions that our Statements touch should be reduced to one
        vpc.applyUpdate();
        assert(vpc.isUpdateApplied());
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            Object params[] = stmt_params.get(catalog_stmt);
            assertNotNull(params);
            PartitionSet partitions = new PartitionSet();
            p_estimator.getAllPartitions(partitions, catalog_stmt, params, base_partition);
            assertNotNull(partitions);
            assertEquals(1, partitions.size());
        } // FOR

    }
    
    /**
     * testCatalogUpdates
     */
    public void testCatalogUpdates() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        info = this.generateInfo(clone_catalogContext);
        
        Table catalog_tbl = this.getTable(clone_db, TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(target_col, info.stats);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionColumn vpc = CollectionUtil.first(candidates);
        assertNotNull(vpc);
        
        // BEFORE!
        Map<Statement, Map<String, Object>> fields_before = new ListOrderedMap<Statement, Map<String, Object>>();
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            fields_before.put(catalog_stmt, this.generateFieldMap(catalog_stmt));
        } // FOR
//        System.err.println("BEFORE:\n" + StringUtil.formatMaps(fields_before));
        
        // AFTER!
        MaterializedViewInfo catalog_view = vpc.applyUpdate();
        assertNotNull(catalog_view);
        assertEquals(CatalogUtil.getVerticalPartition(catalog_tbl), catalog_view);
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            Map<String, Object> before_m = fields_before.get(catalog_stmt);
            assertNotNull(before_m);
            Map<String, Object> after_m = this.generateFieldMap(catalog_stmt);
            assertEquals(before_m.keySet(), after_m.keySet());
            //System.err.println(StringUtil.columns(StringUtil.formatMaps(before_m),
            //                   StringUtil.formatMaps(after_m)));
            
            for (String f : before_m.keySet()) {
                // Use the MD5 checksum to make sure that these fields have changed
                // Yes I could just compare the original strings but... well, uh... I forget why I did this...
                if (f.endsWith("fullplan")) {
                    assertThat(catalog_stmt.fullName() +" ["+f+"]", before_m.get(f), not(equalTo(after_m.get(f))));
                // Sometimes the Expression tree will be different, sometimes it will be the same
                // So just make sure it's not null/empty
                } else if (f.endsWith("exptree")) {
                    assertNotNull(after_m.get(f));
                    assertFalse(catalog_stmt.fullName() +" ["+f+"]", after_m.get(f).toString().isEmpty());
                // All the other fields should be the same except for secondaryindex + replicated
                } else if (f.equals("secondaryindex") == false && f.equals("replicatedonly") == false) {
                    assertEquals(catalog_stmt.fullName() +" ["+f+"]", before_m.get(f), after_m.get(f));
                }
            } // FOR
        } // FOR
        System.err.println(StringUtil.SINGLE_LINE);
        
        // REVERT!
        vpc.revertUpdate();
        assertNull(CatalogUtil.getVerticalPartition(catalog_tbl));
        for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
            Map<String, Object> before_m = fields_before.get(catalog_stmt);
            assertNotNull(before_m);
            Map<String, Object> revert_m = this.generateFieldMap(catalog_stmt);
            assertEquals(before_m.keySet(), revert_m.keySet());
            //System.err.println(StringUtil.columns(StringUtil.formatMaps(before_m),
            //                   StringUtil.formatMaps(revert_m)));
            
            // Now everything should be the same again
            for (String f : before_m.keySet()) {
                if (f.equals("secondaryindex") == false && f.equals("replicatedonly") == false) {
                    assertEquals(catalog_stmt.fullName() +" ["+f+"]", before_m.get(f), revert_m.get(f));
                }
            } // FOR
        } // FOR
    }
    
    /**
     * testCompileOptimizedStatements
     */
    public void testCompileOptimizedStatements() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        if (CatalogUtil.getVerticalPartition(catalog_tbl) != null) {
            catalog_tbl.getViews().clear();
            assert(catalog_tbl.getViews().isEmpty());
        }
        Column catalog_cols[] = {
            this.getColumn(catalog_tbl, "S_ID"),
//            this.getColumn(catalog_tbl, "SUB_NBR"),
//            this.getColumn(catalog_tbl, "VLR_LOCATION"),
        };
        Set<VerticalPartitionColumn> candidates = new HashSet<VerticalPartitionColumn>();
        for (Column catalog_col : catalog_cols) {
            Collection<VerticalPartitionColumn> col_candidates = VerticalPartitionerUtil.generateCandidates(catalog_col, info.stats);
            assertNotNull(col_candidates);
            candidates.addAll(col_candidates);
        } // FOR
        assertFalse(candidates.isEmpty());
        
        for (VerticalPartitionColumn vpc : candidates) {
            // HACK: Clear out the query plans that could have been generated from other tests
            vpc.clear();
            assertTrue(vpc.getOptimizedQueries().isEmpty());
            for (Statement catalog_stmt : vpc.getOptimizedQueries()) {
                assertNull(catalog_stmt.fullName(), vpc.getOptimizedQuery(catalog_stmt));
            } // FOR
        } // FOR
        
//        Collection<VerticalPartitionColumn> new_candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, hp_col, hints);
//        for (VerticalPartitionColumn c : new_candidates) {
//            System.err.println(c);
//            assertFalse(c.getStatements().isEmpty());
//            for (Statement catalog_stmt : c.getStatements()) {
//                assertNotNull(c.getOptimizedQuery(catalog_stmt));
//            } // FOR
//        } // FOR
        
        // Lastly, our table should not still have a vertical partition
//        assert(CatalogUtil.getVerticalPartition(catalog_tbl) == null);
    }
    
    /**
     * testGenerateCandidates
     */
    public void testGenerateCandidates() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(target_col, info.stats);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionColumn vpc = CollectionUtil.first(candidates);
        assertNotNull(vpc);

        Collection<Column> expected_cols = CollectionUtil.addAll(new HashSet<Column>(), this.getColumn(catalog_tbl, "SUB_NBR"),
                                                                                        this.getColumn(catalog_tbl, "S_ID"));
        assertEquals(expected_cols.size(), vpc.getVerticalMultiColumn().size());
        assertTrue(expected_cols + " <=> " + vpc.getVerticalPartitionColumns(), expected_cols.containsAll(vpc.getVerticalPartitionColumns()));
        
        Collection<Statement> expected_stmts = new HashSet<Statement>();
        expected_stmts.add(this.getStatement(this.getProcedure(DeleteCallForwarding.class), "query"));
        expected_stmts.add(this.getStatement(this.getProcedure(InsertCallForwarding.class), "query1"));
        expected_stmts.add(this.getStatement(this.getProcedure(UpdateLocation.class), "getSubscriber"));
        assertEquals(expected_stmts.size(), vpc.getOptimizedQueries().size());
        assert(expected_stmts.containsAll(vpc.getOptimizedQueries()));
    }
    
    /**
     * testGenerateCandidatesAllColumns
     */
    public void testGenerateCandidatesAllColumns() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = catalog_tbl.getPartitioncolumn();
        assertNotNull(target_col);
        
        for (Column catalog_col : catalog_tbl.getColumns()) {
            Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(catalog_col, info.stats);
            assertEquals(candidates.toString(), catalog_col.equals(target_col), candidates.size() > 0);
        } // FOR
    }
    
}
