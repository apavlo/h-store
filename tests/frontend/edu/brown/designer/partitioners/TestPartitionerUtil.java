package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.InsertCallForwarding;
import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.PartitionerUtil.VerticalPartitionCandidate;
import edu.brown.designer.partitioners.TestAbstractPartitioner.MockPartitioner;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestPartitionerUtil extends BasePartitionerTestCase {

    private MockPartitioner partitioner;
    private AccessGraph agraph;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(MockPartitioner.class);
        assertNotNull(info.getStats());
        
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (MockPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, this.partitioner.generateAccessGraph());
        assertNotNull(this.agraph);
    }
    
    /**
     * testGenerateVerticalPartitioningCandidates
     */
    public void testGenerateVerticalPartitioningCandidates() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        
        Collection<VerticalPartitionCandidate> candidates = PartitionerUtil.generateVerticalPartitioningCandidates(info, agraph, target_col, hints);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionCandidate vpc = CollectionUtil.getFirst(candidates);
        assertNotNull(vpc);

        Collection<Column> expected_cols = new HashSet<Column>();
        expected_cols.add(this.getColumn(catalog_tbl, "SUB_NBR"));
        assertEquals(expected_cols.size(), vpc.getColumns().size());
        assert(expected_cols.containsAll(vpc.getColumns()));
        
        Collection<Statement> expected_stmts = new HashSet<Statement>();
        expected_stmts.add(this.getStatement(this.getProcedure(DeleteCallForwarding.class), "query"));
        expected_stmts.add(this.getStatement(this.getProcedure(InsertCallForwarding.class), "query1"));
        assertEquals(expected_stmts.size(), vpc.getStatements().size());
        assert(expected_stmts.containsAll(vpc.getStatements()));
        
        System.err.println("-------------------------------");
        for (Column catalog_col : catalog_tbl.getColumns()) {
            PartitionerUtil.generateVerticalPartitioningCandidates(info, agraph, catalog_col, hints);
        }
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
        DesignerVertex v0 = agraph.getVertex(expected[0]);
        assertNotNull("Missing vertex: " + expected[0], v0);
        DesignerVertex v1 = agraph.getVertex(expected[1]);
        assertNotNull("Missing vertex: " + expected[1], v1);
        assert (agraph.addEdge(new DesignerEdge(agraph), v0, v1));
        for (DesignerEdge e : agraph.findEdgeSet(v0, v1)) {
            e.addToWeight(0, 10000000);
        } // FOR
//        for (Edge e : agraph.getEdges()) {
//            System.err.println(e + " [" + e.getTotalWeight() + "]");
//        }
        
        // Fire away!!
        List<String> ordered = PartitionerUtil.generateTableOrder(this.info, agraph, this.hints);
        assertNotNull(ordered);
        assertFalse(ordered.isEmpty());
        assertEquals(expected.length, ordered.size());

//        System.err.println("Visit Order: " + ordered);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], ordered.get(i));
        } // FOR
    }
    
    /**
     * testGenerateTableOrderMissingVertex
     */
    public void testGenerateTableOrderMissingVertex() throws Exception {
        // Remove one of the vertices from the graph and make sure that it's not included
        // in our search table order
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY);
        DesignerVertex v = agraph.getVertex(catalog_tbl);
        assertNotNull(v);
        boolean ret = agraph.removeVertex(v); 
        assert(ret);
        assertEquals(catalog_db.getTables().size()-1, agraph.getVertexCount());
        
//        System.err.println("GRAPH: " + FileUtil.writeStringToTempFile(GraphvizExport.export(agraph, "tm1"), "dot"));
        
        // Fire away!!
        List<String> ordered = PartitionerUtil.generateTableOrder(this.info, agraph, this.hints);
        assertNotNull(ordered);
        assertFalse(ordered.isEmpty());
        assertEquals(catalog_db.getTables().size()-1, ordered.size());
//      System.err.println("Visit Order: " + ordered);
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
        List<String> ordered = PartitionerUtil.generateColumnOrder(this.info, agraph, catalog_tbl, this.hints);
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
        Set<String> param_order = PartitionerUtil.generateProcParameterOrder(this.info, catalog_db, catalog_proc, hints);
        assertNotNull(param_order);
        assertFalse(param_order.isEmpty());
        
        // We should get back the first ProcParameter for each Procedure, since that maps to S_ID
        ProcParameter catalog_proc_param = CatalogKey.getFromKey(catalog_db, CollectionUtil.getFirst(param_order), ProcParameter.class);
        assertNotNull(catalog_proc_param);
        assertEquals(0, catalog_proc_param.getIndex());
    }
    
}
