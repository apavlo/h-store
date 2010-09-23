package edu.brown.designer.partitioners;

import java.util.HashSet;
import java.util.List;

import org.voltdb.catalog.*;

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
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestBranchAndBoundPartitioner extends BasePartitionerTestCase {

    private BranchAndBoundPartitioner partitioner;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(BranchAndBoundPartitioner.class);
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (BranchAndBoundPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
    }

    /**
     * testGenerateAccessGraph
     */
    public void testGenerateAccessGraph() throws Exception {
        AccessGraph agraph = this.partitioner.generateAccessGraph();
        assertNotNull(agraph);

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
        AccessGraph agraph = this.partitioner.generateAccessGraph();
        assertNotNull(agraph);

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
     * testTraverse
     */
//    @SuppressWarnings("unchecked")
//    public void testTraverse() throws Exception {
//        this.partitioner.init(this.hints);
//        
//        // We have to massage our attributes list so that our testing is deterministic
//        //  (1) Remove ACCESS_INFO
//        //  (2) Remove all attributes for SUBSCRIBER and SPECIAL_FACILITY except for S_ID
//        ListOrderedMap<String, List<Column>> attributes = this.partitioner.base_traversal_attributes;
//        attributes.remove(CatalogKey.createKey(this.getTable(TM1Constants.TABLENAME_ACCESS_INFO)));
//        for (String table_name : new String[] { TM1Constants.TABLENAME_SUBSCRIBER, TM1Constants.TABLENAME_SPECIAL_FACILITY }) {
//            Table catalog_tbl = this.getTable(table_name);
//            Column catalog_col = catalog_tbl.getColumns().get("S_ID");
//            assertNotNull(catalog_col);
//            attributes.get(CatalogKey.createKey(catalog_tbl)).retainAll(Arrays.asList(catalog_col));
//        } // FOR
//        
//        int next_table_idx = 0; 
//        String next_table_key = attributes.get(next_table_idx); 
//        assertNotNull(next_table_key);
//        Table next_table = CatalogKey.getFromKey(catalog_db, next_table_key, Table.class);
//        assertNotNull(next_table);
//        assertEquals(TM1Constants.TABLENAME_SUBSCRIBER, next_table.getName());
//        
//        Catalog clone = CatalogUtil.cloneBaseCatalog(catalog, Table.class);
//        assertNotNull(clone);
//        StateVertex start_vertex = new StateVertex(CatalogUtil.getDatabase(clone), SearchType.MIN);
//        
//        TraverseThread thread = this.partitioner.new TraverseThread(this.info, start_vertex, attributes);
//        assertNotNull(thread);
//        thread.traverse(start_vertex, 0);
//        
//        // Make sure that the solution we pick has a memory and a cost
//        StateVertex best_vertex = this.partitioner.getBestVertex();
//        assertNotNull(best_vertex);
//        assert(best_vertex.cost > 0);
//        assert(best_vertex.memory > 0);
//    }
}