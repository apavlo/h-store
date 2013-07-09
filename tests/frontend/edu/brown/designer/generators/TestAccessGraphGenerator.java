/**
 * 
 */
package edu.brown.designer.generators;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.AccessGraph.EdgeAttributes;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * @author pavlo
 *
 */
public class TestAccessGraphGenerator extends BaseTestCase {
    
    private static final long WORKLOAD_XACT_LIMIT = 100;
    private static final String TARGET_PROCEDURE = slev.class.getSimpleName();
    private static final String TARGET_STATEMENT = "GetStockCount";
    
    // Reading the workload takes a long time, so we only want to do it once
    private static Workload workload;
    
    private Procedure catalog_proc;
    private DesignerInfo info;
    
    private AccessGraph agraph;
    private AccessGraphGenerator generator;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            this.applyParameterMappings(ProjectType.TPCC);
            
            // Load up the workload!
            File workload_file = this.getWorkloadFile(ProjectType.TPCC); 
            workload = new Workload(catalog);
        
            // Workload Filter
            ProcedureNameFilter filter = new ProcedureNameFilter(false);
            filter.include(TARGET_PROCEDURE);
            filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            ((Workload)workload).load(workload_file, catalog_db, filter);
        }
        assertTrue(workload.getTransactionCount() > 0);
        
        // Setup everything else
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.info = new DesignerInfo(catalogContext, workload);
        
        this.agraph = new AccessGraph(catalog_db);
        this.generator = new AccessGraphGenerator(this.info, this.catalog_proc);
    }
    
//    private void display(final AccessGraph agraph) throws Exception {
//          javax.swing.SwingUtilities.invokeAndWait(new Runnable() {
//              public void run() {
//                  GraphVisualizationPanel.createFrame(agraph).setVisible(true);
//              }
//          });
//    }
    
    /**
     * testConvertToSingleColumnEdges
     */
    public void testConvertToSingleColumnEdges() throws Exception {
        System.err.println(CollectionUtil.first(workload.getTransactions()).debug(catalog_db));
        new AccessGraphGenerator(this.info, this.catalog_proc).generate(agraph);
        System.err.println(agraph.debug());
        System.err.println("==========================");
        agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, agraph);

        
        // Make sure that there is at least one edge between DISTRICT and all other tables
        Table target = this.getTable("DISTRICT");
        DesignerVertex v0 = agraph.getVertex(target);
        assertNotNull(v0);
        
        String expected[] = { "ORDER_LINE", "STOCK" };
        for (String tbl_name : expected) {
            Table catalog_tbl = this.getTable(tbl_name);
            DesignerVertex v1 = agraph.getVertex(catalog_tbl);
            assertNotNull(v1);
            
            Collection<DesignerEdge> edges = agraph.findEdgeSet(v0, v1);
            assertNotNull(edges);
            if (edges.isEmpty()) {
                System.err.println(agraph.debug());
            }
            assertFalse(v0 + "<->" + v1, edges.isEmpty());
            
            for (DesignerEdge e : edges) {
                PredicatePairs cset = e.getAttribute(EdgeAttributes.COLUMNSET);
                assertNotNull(cset);
                assertEquals(cset.toString(), 1, cset.size());
            }
        } // FOR
        
//        agraph.setVerbose(true);
//        System.err.println("Dumping AccessGraph to " + FileUtil.writeStringToTempFile(GraphvizExport.export(agraph, "tpcc"), "dot"));
    }
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        this.generator.initialize(agraph);
        for (Table catalog_tbl : CatalogUtil.getDataTables(catalog_db)) {
            if (catalog_tbl.getSystable()) continue;
            assertNotNull("Missing " + catalog_tbl, this.agraph.getVertex(catalog_tbl));
        } // FOR
    }
    
    /**
     * testFindEdgeSetUsingColumn
     */
    public void testFindEdgeSetUsingColumn() throws Exception {
        AccessGraph agraph = new AccessGraph(catalog_db);
        new AccessGraphGenerator(this.info, this.catalog_proc).generate(agraph);
        agraph.setVerbose(true);
        
        Table catalog_tbl = this.getTable("STOCK");
        Column catalog_col = this.getColumn(catalog_tbl, "S_W_ID");
        
        DesignerVertex v = agraph.getVertex(catalog_tbl);
        assertNotNull(v);
        Collection<DesignerEdge> edges = agraph.findEdgeSet(v, catalog_col);
        assertNotNull(edges);
        assert(agraph.getEdgeCount() != edges.size());
    }
    
    /**
     * testMultiPass
     */
    public void testMultiPass() throws Exception {
        AccessGraph agraph0 = new AccessGraph(catalog_db);
        new AccessGraphGenerator(this.info, this.catalog_proc).generate(agraph0);
        
        AccessGraph agraph1 = new AccessGraph(catalog_db);
        new AccessGraphGenerator(this.info, this.catalog_proc).generate(agraph1);
        
        // Make sure that all of the weights are the same
        for (DesignerEdge e0 : agraph0.getEdges()) {
            List<DesignerVertex> vertices0 = new ArrayList<DesignerVertex>(agraph0.getIncidentVertices(e0));
            DesignerVertex v1_0  = agraph1.getVertex(vertices0.get(0).getCatalogKey());
            assertNotNull("Missing vertex " + vertices0.get(0), v1_0);
            DesignerVertex v1_1  = agraph1.getVertex(vertices0.get(1).getCatalogKey());
            assertNotNull("Missing vertex " + vertices0.get(1), v1_1);
            
            DesignerEdge e1 = agraph1.findEdge(v1_0, v1_1);
            assertNotNull("Missing edge " + e0, e1);
            
            assertEquals(e0.getIntervalCount(), e1.getIntervalCount());
            for (int i = 0, cnt = e0.getIntervalCount(); i < cnt; i++) {
                assertEquals("Mismatched weights for " + e0 + " [" + i + "]", e0.getWeight(i), e1.getWeight(i));
            } // FOR
        } // FOR
    }
    
    /**
     * testCreateExplicitEdges
     */
    public void testCreateExplicitEdges() throws Exception {
        this.generator.initialize(agraph);
        
        Statement catalog_stmt = this.catalog_proc.getStatements().get(TARGET_STATEMENT);
        assertNotNull(catalog_stmt);
        this.generator.createExplicitEdges(agraph, catalog_stmt);
        
        assert(this.agraph.getVertexCount() > 0);
        assert(this.agraph.getEdgeCount() > 0);
        
        // Make sure that there is an edge between STOCK and ORDER_LINE
        Table tbl0 = this.getTable("STOCK"); 
        DesignerVertex v0 = this.agraph.getVertex(tbl0); 
        assertNotNull(v0);
        
        Table tbl1 = this.getTable("ORDER_LINE"); 
        DesignerVertex v1 = this.agraph.getVertex(tbl1);
        assertNotNull(v1);
        
        DesignerEdge edge = this.agraph.findEdge(v0, v1);
        assertNotNull("No edge exists between " + tbl0 + " and " + tbl1, edge);
    }
    
    /**
     * testCreateSharedParamEdges
     */
    public void testCreateSharedParamEdges() throws Exception {
        this.generator.initialize(agraph);
        
        Statement catalog_stmts[] = this.catalog_proc.getStatements().values();
        Statement catalog_stmt0 = catalog_stmts[0];
        assertNotNull(catalog_stmt0);
        this.generator.createExplicitEdges(agraph, catalog_stmt0);
        
        Statement catalog_stmt1 = catalog_stmts[1];
        assertNotNull(catalog_stmt1);
        this.generator.createExplicitEdges(agraph, catalog_stmt1);
        
        List<ProcParameter> catalog_stmt0_params = new ArrayList<ProcParameter>();
        for (StmtParameter param : catalog_stmt0.getParameters()) {
            if (param.getProcparameter() != null) catalog_stmt0_params.add(param.getProcparameter());
        } // FOR
        assertFalse("No ProcParameters mapped for " + catalog_stmt0, catalog_stmt0_params.isEmpty());
        
        // this.generator.setDebug(true);
        this.generator.createSharedParamEdges(agraph, catalog_stmt0, catalog_stmt1, catalog_stmt0_params);
        
        // We should now have a complete graph between the following tables
        String tables[] = { "ORDER_LINE", "DISTRICT", "STOCK" };
        for (int i = 0; i < tables.length; i++) {
            Table t0 = this.getTable(tables[i]);
            DesignerVertex v0 = agraph.getVertex(t0);
            assertNotNull(v0);            
            
            for (int ii = i + 1; ii < tables.length; ii++) {
                Table t1 = this.getTable(tables[ii]);
                assertNotSame(t0, t1);
                DesignerVertex v1 = agraph.getVertex(t1);
                assertNotNull(v1);
                assertNotNull(agraph.findEdge(v0, v1));
            } // FOR
        } // FOR
    }
    
    /**
     * testUpdateEdgeWeights
     */
    public void testUpdateEdgeWeights() throws Exception {
        this.generator.initialize(this.agraph);
        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            this.generator.createExplicitEdges(this.agraph, catalog_stmt);
        } // FOR
        
        Collection<TransactionTrace> traces = workload.getTransactions();
        Histogram<Integer> hist = new ObjectHistogram<Integer>();
//        TransactionTrace middle = traces.get(traces.size() / 2);
        for (TransactionTrace xact : traces) {
            // int time = (xact.getStartTimestamp() < middle.getStartTimestamp() ? 0 : 1);
            int time = workload.getTimeInterval(xact, info.getNumIntervals());
            hist.put(time);
            // System.err.println("[" + time + "] " + xact);
            this.generator.updateEdgeWeights(this.agraph, xact);
        } // FOR
        
//        System.err.println(hist);
//        for (Edge edge : agraph.getEdges()) {
//            System.err.println(edge + ": " + edge.getWeights());
//            // assertEquals(edge.getWeight(0), edge.getWeight(1));
//        } // FOR
    }
}
