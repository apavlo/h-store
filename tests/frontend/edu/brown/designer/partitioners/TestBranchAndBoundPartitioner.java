package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner.StateVertex;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner.TraverseThread;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestBranchAndBoundPartitioner extends BasePartitionerTestCase {

    private BranchAndBoundPartitioner partitioner;
    private AccessGraph agraph;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(BranchAndBoundPartitioner.class);
        assertNotNull(info.getStats());
        
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (BranchAndBoundPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, this.partitioner.generateAccessGraph());
        assertNotNull(this.agraph);
    }

    /**
     * testGenerateAccessGraph
     */
    public void testGenerateAccessGraph() throws Exception {
        // Make sure all of our tables are there
        for (Table catalog_tbl : catalog_db.getTables()) {
            Vertex v = agraph.getVertex(catalog_tbl);
            assertNotNull("Missing " + catalog_tbl, v);
        } // FOR
        
        // Make sure our edges have weights
        // Only the self-referencing edges for CALL_FORWARDING and SUBSCRIBER will be zero
        HashSet<Table> skip = new HashSet<Table>();
        skip.add(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        skip.add(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING));
        for (Edge e : agraph.getEdges()) {
            Vertex v0 = CollectionUtil.get(agraph.getIncidentVertices(e), 0);
            Vertex v1 = CollectionUtil.get(agraph.getIncidentVertices(e), 1);
            if (!(v0.getCatalogItem().equals(v1.getCatalogItem()) && skip.contains(v0.getCatalogItem()))) {
                assert(e.getTotalWeight() > 0) : "No edge weight for " + e;
            }
        } // FOR
    }

    /**
     * testGenerateTableOrder
     */
    public void testGenerateTableOrder() throws Exception {
        // Insert an artificial edge between SUBSCRIBER and ACCESS_INFO with a
        // high weight so that we can anticipate the ordering
        String expected[] = {
            CatalogKey.createKey(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER)),
            CatalogKey.createKey(this.getTable(TM1Constants.TABLENAME_ACCESS_INFO)),
            CatalogKey.createKey(this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY)),
            CatalogKey.createKey(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING)),
        };
        Vertex v0 = agraph.getVertex(expected[0]);
        assertNotNull("Missing vertex: " + expected[0], v0);
        Vertex v1 = agraph.getVertex(expected[1]);
        assertNotNull("Missing vertex: " + expected[1], v1);
        assert (agraph.addEdge(new Edge(agraph), v0, v1));
        for (Edge e : agraph.findEdgeSet(v0, v1)) {
            e.addToWeight(0, 10000000);
        } // FOR
//        for (Edge e : agraph.getEdges()) {
//            System.err.println(e + " [" + e.getTotalWeight() + "]");
//        }
        
        // Fire away!!
        List<String> ordered = BranchAndBoundPartitioner.generateTableOrder(this.info, agraph, this.hints);
        assertNotNull(ordered);
        assertFalse(ordered.isEmpty());
        assertEquals(expected.length, ordered.size());

//        System.err.println("Visit Order: " + ordered);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], ordered.get(i));
        } // FOR
    }

    /**
     * testGenerateColumnOrder
     */
    public void testGenerateColumnOrder() throws Exception {
        AccessGraph agraph = this.partitioner.generateAccessGraph();
        assertNotNull(agraph);

        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        String expected[] = {
            "S_ID",
            "SUB_NBR",
            "VLR_LOCATION",
            "BIT_1",
            ReplicatedColumn.COLUMN_NAME,
        };
        for (int i = 0; i < expected.length; i++)
            assertNotNull("Null column [" + i + "]", expected[i]);

        // Bombs away!!!
        List<String> ordered = BranchAndBoundPartitioner.generateColumnOrder(this.info, agraph, catalog_tbl, this.hints);
        assertNotNull(ordered);
        assertFalse(ordered.isEmpty());
        assertEquals(expected.length, ordered.size());

//        System.err.println("Visit Order: " + ordered);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], CatalogKey.getNameFromKey(ordered.get(i)));
        } // FOR
    }
    
    /**
     * testGenerateProcParameterOrder
     */
    public void testGenerateProcParameterOrder() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        List<String> param_order = BranchAndBoundPartitioner.generateProcParameterOrder(this.info, catalog_db, catalog_proc, hints);
        assertNotNull(param_order);
        assertFalse(param_order.isEmpty());
        
        // We should get back the first ProcParameter for each Procedure, since that maps to S_ID
        ProcParameter catalog_proc_param = CatalogKey.getFromKey(catalog_db, param_order.get(0), ProcParameter.class);
        assertNotNull(catalog_proc_param);
        assertEquals(0, catalog_proc_param.getIndex());
    }

    /**
     * testHaltReason
     */
    public void testHaltReason() throws Exception {
        List<Table> table_visit_order = (List<Table>)CatalogKey.getFromKeys(catalog_db, BranchAndBoundPartitioner.generateTableOrder(info, agraph, hints), Table.class);
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