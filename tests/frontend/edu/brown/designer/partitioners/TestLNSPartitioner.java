/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.catalog.CatalogCloner;
import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.Designer;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public class TestLNSPartitioner extends BasePartitionerTestCase {

    private LNSPartitioner partitioner;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(LNSPartitioner.class);
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (LNSPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
    }
    
    /**
     * testGenerate
     */
    public void testVerticalPartitioning() throws Exception {
        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
        assert(clone_db.hashCode() != catalog_db.hashCode());
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        
        System.err.println("catalog_db => " + catalog_db.hashCode());
        System.err.println("clone_db => " + clone_db.hashCode());
        int num_intervals = info.getNumIntervals();
        info = this.generateInfo(clone_catalogContext);
        info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(clone_catalogContext, SingleSitedCostModel.class, num_intervals));
        info.setPartitionerClass(LNSPartitioner.class);
        
        hints.enable_vertical_partitioning = true;
        hints.max_memory_per_partition = Long.MAX_VALUE;
        hints.enable_costmodel_multipartition_penalty = true;
        hints.enable_replication_readmostly = false;
        hints.enable_replication_readonly = false;
        hints.weight_costmodel_multipartition_penalty = 100.0d;
        hints.relaxation_min_size = clone_db.getTables().size();
        hints.limit_local_time = 30;
        hints.limit_total_time = 30;
        
        designer = new Designer(info, hints, info.getArgs());
        LNSPartitioner partitioner = (LNSPartitioner)designer.getPartitioner();
        assertEquals(clone_db, partitioner.info.catalogContext.database);

        // 2012-07-20: This is broken for some reason....
        // PartitionPlan pplan = partitioner.generate(hints);
        // assertNotNull(pplan);
        // System.err.println(pplan);
    }
    
    /**
     * testInit
     */
    @Test
    public void testInit() throws Exception {
        this.partitioner.init(this.hints);
        
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable()) continue;
            
            // Table Attributes
            assertTrue(catalog_tbl.toString(), this.partitioner.orig_table_attributes.containsKey(catalog_tbl));
            assertFalse(catalog_tbl.toString(), this.partitioner.orig_table_attributes.get(catalog_tbl).isEmpty());
            
            // Table Procedures
            assertTrue(catalog_tbl.toString(), this.partitioner.table_procedures.containsKey(catalog_tbl));
            assertFalse(catalog_tbl.toString(), this.partitioner.table_procedures.get(catalog_tbl).isEmpty());
            
            // Table Sizes
            assertTrue(catalog_tbl.toString(), this.partitioner.table_replicated_size.containsKey(catalog_tbl));
            assertTrue(catalog_tbl.toString(), this.partitioner.table_replicated_size.get(catalog_tbl) > 0);
            assertTrue(catalog_tbl.toString(), this.partitioner.table_nonreplicated_size.containsKey(catalog_tbl));
            assertTrue(catalog_tbl.toString(), this.partitioner.table_nonreplicated_size.get(catalog_tbl) > 0);

            for (Column catalog_col : this.partitioner.orig_table_attributes.get(catalog_tbl)) {
                assertNotNull(catalog_col);
                if (catalog_col instanceof MultiColumn) continue;
                
                // Column Procedures
                assert(this.partitioner.column_procedures.containsKey(catalog_col));
                assertFalse(this.partitioner.column_procedures.get(catalog_col).isEmpty());
//                System.err.println(CatalogUtil.getDisplayName(catalog_col) + ": " + this.partitioner.column_procedures.get(catalog_col));
                
                // Column Swap Procedures
                assert(this.partitioner.columnswap_procedures.containsKey(catalog_col));
                assertFalse(this.partitioner.columnswap_procedures.get(catalog_col).isEmpty());
//                for (Column other : this.partitioner.table_attributes.get(catalog_tbl)) {
//                    if (catalog_col.equals(other)) {
//                        assertFalse(this.partitioner.columnswap_procedures.get(catalog_col).containsKey(other));
//                        continue;
//                    }
//                    assert(this.partitioner.columnswap_procedures.get(catalog_col).containsKey(other));
//                    
//                    // assert(!this.partitioner.columnswap_procedures.get(catalog_col).get(other).isEmpty()) : "No intersection between " + catalog_col + " + " + other;
//                    assertEquals(this.partitioner.columnswap_procedures.get(catalog_col).get(other), this.partitioner.columnswap_procedures.get(other).get(catalog_col));
////                    System.err.println("   " + CatalogUtil.getDisplayName(other) + ": " + this.partitioner.columnswap_procedures.get(catalog_col).get(other));
//                } // FOR
            } // FOR
//            System.err.println();
        } // FOR
    }
    
    /**
     * testProcedureColumns
     */
    public void testProcedureColumns() throws Exception {
        this.partitioner.init(this.hints);
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        Table catalog_tbl0 = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Table catalog_tbl1 = this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY);
        Column expected[] = {
            this.getColumn(catalog_tbl0, "S_ID"),
            this.getColumn(catalog_tbl0, "BIT_1"),
            this.getColumn(catalog_tbl1, "DATA_A"),
            this.getColumn(catalog_tbl1, "S_ID"),
            this.getColumn(catalog_tbl1, "SF_TYPE")
        };
        
        Collection<Column> columns = this.partitioner.proc_columns.get(catalog_proc);
        assertNotNull(columns);
        assertEquals(expected.length, columns.size());
        for (Column col : expected) {
            assert(columns.contains(col)) : "Missing " + CatalogUtil.getDisplayName(col);
        } // FOR
    }
    
    /**
     * testProcedureColumnAccessHistogramSimple
     */
    public void testProcedureColumnAccessHistogramSimple() throws Exception {
        this.partitioner.init(this.hints);
        
        Histogram<String> proc_histogram = workload.getProcedureHistogram();
        
        // Just make sure that each Histogram isn't empty
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (!PartitionerUtil.isPartitionable(catalog_proc)) continue;
            String proc_key = CatalogKey.createKey(catalog_proc);
            if (catalog_proc.getSystemproc() || !proc_histogram.contains(proc_key)) continue;
            Histogram<Column> h = this.partitioner.proc_column_histogram.get(catalog_proc);
            assert(h != null) : "Null Column Access Histogram: " + catalog_proc;
            assert(!h.isEmpty()) : "Empty Column Access Histogram: " + catalog_proc;
//            System.err.println(catalog_proc + ":\n" + h);
        } // FOR
    }
    
    /**
     * testProcedureColumnAccessHistogram
     */
    public void testProcedureColumnAccessHistogram() throws Exception {
        this.partitioner.init(this.hints);
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        Table catalog_tbl0 = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Table catalog_tbl1 = this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY);
        Column expected[] = {
            this.getColumn(catalog_tbl0, "S_ID"),
            this.getColumn(catalog_tbl0, "BIT_1"),
            this.getColumn(catalog_tbl1, "DATA_A"),
            this.getColumn(catalog_tbl1, "S_ID"),
            this.getColumn(catalog_tbl1, "SF_TYPE")
        };
        
        Histogram<Column> h = this.partitioner.proc_column_histogram.get(catalog_proc);
        assertNotNull(h);
        assertEquals(expected.length, h.getValueCount());
        for (Column col : expected) {
            assert(h.contains(col)) : "Missing " + CatalogUtil.getDisplayName(col);
        } // FOR
    }
    
    /**
     * testFindBestProcParameter
     */
    public void testFindBestProcParameter() throws Exception {
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        ProcParameter catalog_param = null;

        this.partitioner.init(this.hints);
        catalog_param = this.partitioner.findBestProcParameter(this.hints, catalog_proc);
        assertNotNull(catalog_param);
        assertEquals(0, catalog_param.getIndex());
    }
    
    
    /**
     * testPopulateCurrentSolution
     */
//    @Test
//    public void testPopulateCurrentSolution() throws Exception {
//        this.partitioner.init(this.hints);
//        this.partitioner.populateCurrentSolution(catalog_db);
//        
//        Map<CatalogType, CatalogType> solution = this.partitioner.getCurrentSolution();
//        assertFalse("Current solution is empty??", solution.isEmpty());
//        for (Table catalog_tbl : catalog_db.getTables()) {
//            Column catalog_col = catalog_tbl.getPartitioncolumn();
//            assertNotNull(catalog_col);
//            assert(solution.containsKey(catalog_tbl));
//            Column current_catalog_col = (Column)solution.get(catalog_tbl);
//            assertNotNull(current_catalog_col);
//            assertEquals(catalog_col, current_catalog_col);
//        } // FOR
//    }
    
    /**
     * testGenerateMultiProcParameters
     */
    @Test
    public void testGenerateMultiProcParameters() throws Exception {
        hints.enable_multi_partitioning = true;
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        Collection<ProcParameter> orig_params = CollectionUtil.addAll(new ArrayList<ProcParameter>(), catalog_proc.getParameters());
        int orig_size = orig_params.size();
        Map<ProcParameter, Set<MultiProcParameter>> param_multip_map = PartitionerUtil.generateMultiProcParameters(info, hints, catalog_proc);
        assertNotNull(param_multip_map);
        assertEquals(orig_size, param_multip_map.size());

        for (ProcParameter catalog_param : orig_params) {
            assert(param_multip_map.containsKey(catalog_param)) : "Missing " + catalog_param;
            assertFalse(catalog_param.toString(), param_multip_map.get(catalog_param).isEmpty());
        } // FOR
    }
    
    /**
     * testGenerateMultiColumns
     */
    public void testGenerateMultiColumns() throws Exception {
        hints.enable_multi_partitioning = true;
        Procedure catalog_proc = this.getProcedure(GetAccessData.class);
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_ACCESS_INFO);
        Column expected[] = {
            this.getColumn(catalog_tbl, "S_ID"),
            this.getColumn(catalog_tbl, "AI_TYPE"),
        };
        
        Map<Table, Collection<MultiColumn>> multicolumns = PartitionerUtil.generateMultiColumns(info, hints, catalog_proc);
        assertNotNull(multicolumns);
        assertEquals(1, multicolumns.size());
        assert(multicolumns.containsKey(catalog_tbl));
        
        // XXX: What if there are multiple MultiColumns?
        MultiColumn mc = CollectionUtil.first(multicolumns.get(catalog_tbl));
        assertNotNull(mc);
        
        System.err.println("COLUMNS: " + multicolumns);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(mc.toString(), expected[i], mc.get(i));
        } // FOR
    }
    
    /**
     * testLocalSearchCostCheck
     */
    public void testLocalSearchCostCheck() throws Exception {
        // Make sure that the cost of the initial solution before the local search is the same
        // one that we get after (assuming the same PartitionPlan). We do this by setting the time limit
        // to zero so that won't actually traverse the search tree
        hints.limit_local_time = 0;
        hints.enable_procparameter_search = false;
        hints.max_memory_per_partition = Long.MAX_VALUE;
        this.partitioner.init(this.hints);
        this.partitioner.calculateInitialSolution(hints);
        assert(this.partitioner.initial_cost > 0);
        PartitionPlan orig_solution = new PartitionPlan(this.partitioner.initial_solution);
        this.partitioner.best_solution = orig_solution;
        this.partitioner.best_memory = this.partitioner.initial_memory;
        this.partitioner.best_cost = this.partitioner.initial_cost;

        // First check whether the cost is the same simply right after the first go
        assertEquals(orig_solution, this.partitioner.best_solution);
        double new_cost = info.getCostModel().estimateWorkloadCost(catalogContext, workload);
        assert(new_cost > 0);
        assertEquals(this.partitioner.initial_cost, new_cost);
        
        // Genarate table+procedure attribute lists
        List<Table> table_attributes = new ArrayList<Table>();
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        table_attributes.add(catalog_tbl);
        
        List<Procedure> proc_attributes = new ArrayList<Procedure>();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (PartitionerUtil.isPartitionable(catalog_proc)) proc_attributes.add(catalog_proc);
        } // FOR

        // Now throw everything at the local search procedure. This should stop right away because the 
        // time limits will immediately be exceeded
        this.partitioner.localSearch(hints, table_attributes, proc_attributes);
//        System.err.println(this.partitioner.best_solution);
        
        // Now check that the cost before and after are the same
        if (orig_solution.equals(this.partitioner.best_solution) == false) {
            System.err.println(orig_solution);
            System.err.println(StringUtil.repeat("*", 100));
            System.err.println(this.partitioner.best_solution);
        }
        
        for (Table tbl : catalog_db.getTables()) {
            assertEquals(tbl.toString(), orig_solution.getTableEntry(tbl), this.partitioner.best_solution.getTableEntry(tbl)); 
        }
// FIXME
//        info.getCostModel().clear();
//        new_cost = info.getCostModel().estimateWorkloadCost(catalog_db, workload);
//        assert(new_cost > 0);
//        assertEquals(this.partitioner.initial_cost, new_cost);
        
    }
}