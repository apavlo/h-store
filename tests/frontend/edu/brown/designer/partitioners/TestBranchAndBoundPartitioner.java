package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner.StateVertex;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner.TraverseThread;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestBranchAndBoundPartitioner extends BasePartitionerTestCase {

    private BranchAndBoundPartitioner partitioner;
    private AccessGraph agraph;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(BranchAndBoundPartitioner.class);
        assertNotNull(info.getStats());
        
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (BranchAndBoundPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, this.partitioner.generateAccessGraph());
        assertNotNull(this.agraph);
    }

    /**
     * testHaltReason
     */
    public void testHaltReason() throws Exception {
        List<Table> table_visit_order = PartitionerUtil.generateTableOrder(info, agraph, hints);
        assertFalse(table_visit_order.isEmpty());
        List<Procedure> proc_visit_order = (List<Procedure>)CollectionUtil.addAll(new ArrayList<Procedure>(), catalog_db.getProcedures());
        assertFalse(proc_visit_order.isEmpty());
        
        this.partitioner.setParameters(agraph, table_visit_order, proc_visit_order);
        // this.partitioner.init(this.hints);
    }
    
    /**
     * testMemoryExceeded
     */
    public void testMemoryExceeded() throws Exception {
        List<Procedure> proc_visit_order = new ArrayList<Procedure>();
        List<Table> table_visit_order = (List<Table>)CollectionUtil.addAll(new ArrayList<Table>(), catalog_db.getTables());
        
        // Set the tables to all be partitioned on the last column
        for (Table catalog_tbl : catalog_db.getTables()) {
            Column catalog_col = this.getColumn(catalog_tbl, -1);
            catalog_tbl.setPartitioncolumn(catalog_col);
        } // FOR
        
        // Set this to be the upperbounds
        PartitionPlan ub_pplan = PartitionPlan.createFromCatalog(catalog_db);
        assertNotNull(ub_pplan);
        this.partitioner.setUpperBounds(hints, ub_pplan, Double.MAX_VALUE, 1000l);
        
        hints.max_memory_per_partition = 1;
        hints.enable_procparameter_search = false;
        this.partitioner.setParameters(agraph, table_visit_order, proc_visit_order);
        this.partitioner.init(this.hints);
        
        StateVertex start_vertex = StateVertex.getStartVertex(Double.MAX_VALUE, Long.MAX_VALUE);
        TraverseThread thread = this.partitioner.new TraverseThread(info, hints, start_vertex, agraph, table_visit_order, proc_visit_order);
        assertNotNull(thread);
        thread.traverse(start_vertex, 0);
        this.partitioner.getHaltReason();
        
        // Make sure that the solution we pick has a memory and a cost
        StateVertex best_vertex = this.partitioner.getBestVertex();
        assertNotNull(best_vertex);
        assert(best_vertex.getCatalogKeyMap().isEmpty());
//        Map<CatalogType, CatalogType> m = best_vertex.getCatalogMap(catalog_db);
//        for (Table catalog_tbl : orig_partitioning.keySet()) {
//            assert(m.containsKey(catalog_tbl)) : "Missing " + catalog_tbl;
//            assertEquals(orig_partitioning.get(catalog_tbl).fullName(), m.get(catalog_tbl).fullName());
//        } // FOR
    }
    
    /**
     * testTraverse
     */
    public void testTraverse() throws Exception {
        List<Procedure> proc_visit_order = new ArrayList<Procedure>();
        List<Table> table_visit_order = new ArrayList<Table>();
        
        // We have to massage our attributes list so that our testing is deterministic. Set the only table
        // that we're going to visit is ACCESS_INFO and change its partitioning column to something that we know
        // we can beat if we partition on S_ID
        Map<Table, Column> expected = new HashMap<Table, Column>();
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY);
        Column catalog_col = this.getColumn(catalog_tbl, -1);
        catalog_tbl.setPartitioncolumn(catalog_col);
        table_visit_order.add(catalog_tbl);
        expected.put(catalog_tbl, this.getColumn(catalog_tbl, "S_ID"));
        
        catalog_tbl = this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING);
        catalog_col = this.getColumn(catalog_tbl, -1);
        catalog_tbl.setPartitioncolumn(catalog_col);
        table_visit_order.add(catalog_tbl);
        expected.put(catalog_tbl, this.getColumn(catalog_tbl, "S_ID"));
        
        // Set this to be the upperbounds
        PartitionPlan ub_pplan = PartitionPlan.createFromCatalog(catalog_db);
        assertNotNull(ub_pplan);
        this.partitioner.setUpperBounds(hints, ub_pplan, Double.MAX_VALUE, 1000l);
        
        hints.enable_multi_partitioning = false;
        hints.enable_replication_readmostly = false;
        hints.enable_replication_readonly = false;
        hints.enable_procparameter_search = false;
        hints.max_memory_per_partition = Long.MAX_VALUE;
        this.partitioner.setParameters(agraph, table_visit_order, proc_visit_order);
        this.partitioner.init(this.hints);
        
        StateVertex start_vertex = StateVertex.getStartVertex(Double.MAX_VALUE, Long.MAX_VALUE);
        TraverseThread thread = this.partitioner.new TraverseThread(info, hints, start_vertex, agraph, table_visit_order, proc_visit_order);
        assertNotNull(thread);
        thread.traverse(start_vertex, 0);
        
        // Make sure that the solution we pick has a memory and a cost
        StateVertex best_vertex = this.partitioner.getBestVertex();
        assertNotNull(best_vertex);
        Map<CatalogType, CatalogType> m = best_vertex.getCatalogMap(catalog_db);
        for (Table t : expected.keySet()) {
            assert(m.containsKey(t)) : "Missing " + t;
            assertEquals(expected.get(t), m.get(t));
        } // FOR
        assert(best_vertex.getCost() > 0) : best_vertex.getCost();
        assert(best_vertex.getMemory() > 0) : best_vertex.getMemory();
    }
}