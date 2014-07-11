/**
 * 
 */
package edu.brown.designer.generators;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogPair;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.AccessGraph.EdgeAttributes;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * @author pavlo
 *
 */
public class TestAccessGraphGenerator2 extends BaseTestCase {
    
    private static final long WORKLOAD_XACT_LIMIT = 5000;
    private static final String TARGET_PROCEDURE = "neworder";
    
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
            
            File workload_file = this.getWorkloadFile(ProjectType.TPCC); 
            workload = new Workload(catalog);
        
            // Workload Filter
            ProcedureNameFilter filter = new ProcedureNameFilter(false);
//            filter.include(TARGET_PROCEDURE);
            filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            ((Workload)workload).load(workload_file, catalog_db, filter);
        }
        
        // Setup everything else
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.info = new DesignerInfo(catalogContext, workload);
        
        this.agraph = new AccessGraph(catalog_db);
        this.generator = new AccessGraphGenerator(this.info, this.catalog_proc);
    }
    
    /**
     * testInsertHistory
     */
    public void testInsertHistory() throws Exception {
        agraph = new AccessGraph(catalog_db);
        catalog_proc = this.getProcedure(paymentByCustomerId.class);
        this.generator = new AccessGraphGenerator(this.info, catalog_proc);
        this.generator.generate(agraph);
        assert(agraph.getVertexCount() > 0);
        
        // Make sure it has a vertex for HISTORY
        Table catalog_tbl = this.getTable("HISTORY");
        DesignerVertex v = agraph.getVertex(catalog_tbl);
        assertNotNull(v);
        
        // And make sure that it has edges
        agraph.setVerbose(true);
        Collection<DesignerEdge> edges = agraph.getIncidentEdges(v); 
        assertFalse(edges.isEmpty());
        
        Set<Table> expected = new HashSet<Table>();
        CollectionUtil.addAll(expected, this.getTable("WAREHOUSE"),
                                        this.getTable("DISTRICT"),
                                        this.getTable("CUSTOMER"));
        Set<Table> actual = new HashSet<Table>();
        for (DesignerEdge e : edges) {
            DesignerVertex other_v = agraph.getOpposite(v, e);
            assertNotNull(other_v);
            Table other_tbl = other_v.getCatalogItem();
            assertNotNull(other_tbl);
            if (other_tbl.equals(catalog_tbl)) {
                assertEquals(v, other_v);
            } else {
                assert(expected.contains(other_tbl)) : other_tbl;
                actual.add(other_tbl);
            }
        } // FOR
        assertEquals(expected, actual);
        // System.err.println(StringUtil.join("\n", edges));
    }
    
    /**
     * testConvertToSingleColumnEdges
     */
    public void testConvertToSingleColumnEdges() throws Exception {
        agraph = AccessGraphGenerator.generateGlobal(this.info);
//        agraph.setVerbose(true);
//        System.err.println("Dumping AccessGraph to " + FileUtil.writeStringToFile("/tmp/global_tpcc.dot", GraphvizExport.export(agraph, "tpcc")));
        
        AccessGraph single_agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, agraph);
        assertNotNull(single_agraph);
//        single_agraph.setVerbose(true);
//        System.err.println("Dumping AccessGraph to " + FileUtil.writeStringToFile("/tmp/single_tpcc.dot", GraphvizExport.export(single_agraph, "tpcc")));

        // Make sure that it has all of our tables except HISTORY
        for (Table catalog_tbl : catalogContext.getDataTables()) {
            if (catalog_tbl.getName().equalsIgnoreCase("HISTORY")) continue;
            DesignerVertex v = single_agraph.getVertex(catalog_tbl);
            assertNotNull(catalog_tbl.getName(), v);
            System.err.println(catalog_tbl + ": " + v);
        } // FOR
        
        // Make a new ColumnSet that combines all the ColumnSets of all edges in the original AccessGraph
        DesignerVertex v0, v1;
        Collection<DesignerEdge> edges;
        boolean found = false;
        for (Table catalog_tbl0 : catalog_db.getTables()) {
            try {
                v0 = agraph.getVertex(catalog_tbl0);
            } catch (IllegalArgumentException ex) {
                continue;
            }
            for (Table catalog_tbl1 : catalog_db.getTables()) {
                try {
                    v1 = agraph.getVertex(catalog_tbl1);
                } catch (IllegalArgumentException ex) {
                    continue;
                }
                if (catalog_tbl0.equals(catalog_tbl1)) continue;
            
                PredicatePairs global_cset = new PredicatePairs();
                try {
                    edges = agraph.findEdgeSet(v0, v1);
                } catch (IllegalArgumentException ex) {
                    continue;
                }
                found = true;
                for (DesignerEdge e : edges) {
                    PredicatePairs e_cset = e.getAttribute(EdgeAttributes.COLUMNSET);
                    assertNotNull(e_cset);
                    global_cset.addAll(e_cset);
                } // FOR
//                System.err.println(String.format("%s <-> %s: %d", catalog_tbl0, catalog_tbl1, edges.size()));
                
                // Now check to make sure that there are no edges that have some funky ColumnSet entry that
                // wasn't in our original graph
                for (DesignerEdge e : single_agraph.findEdgeSet(v0, v1)) {
                    PredicatePairs e_cset = e.getAttribute(EdgeAttributes.COLUMNSET);
                    assertNotNull(e_cset);
                    assertEquals(e_cset.toString(), 1, e_cset.size());
                    CatalogPair entry = CollectionUtil.first(e_cset);
                    assertNotNull(entry);
                    assert(global_cset.contains(entry)) : "Missing " + entry;
                } // FOR
            } // FOR (V1)
        } // FOR (V0)
        assert(found);
    }
    
    /**
     * testGenerate
     */
    @SuppressWarnings("unchecked")
    public void testGenerate() throws Exception {
        // I'm getting weird results when I the AccessGraphGenerator run at Brown. Not sure if it's a 32-bit problem...
        // Just make sure that we get the following edges in the graph
        Pair<Table, Table> expected[] = new Pair[]{
                Pair.of(this.getTable("CUSTOMER"), this.getTable("ORDERS")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("CUSTOMER")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("DISTRICT")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("NEW_ORDER")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("NEW_ORDER")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("DISTRICT")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("DISTRICT")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("ORDERS")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ITEM"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ITEM"), this.getTable("ITEM")),
                Pair.of(this.getTable("ITEM"), this.getTable("STOCK")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("NEW_ORDER")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("ORDERS")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ORDERS"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ORDERS"), this.getTable("ORDERS")),
                Pair.of(this.getTable("ORDERS"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("ORDER_LINE"), this.getTable("STOCK")),
//                Pair.of(this.getTable("ORDER_LINE"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("ORDER_LINE"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("STOCK"), this.getTable("STOCK")),
                Pair.of(this.getTable("STOCK"), this.getTable("STOCK")),
                Pair.of(this.getTable("STOCK"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("WAREHOUSE"), this.getTable("WAREHOUSE")),
        };
        
        this.generator.generate(agraph);
        for (int i = 0, cnt = expected.length; i < cnt; i++) {
            Pair<Table, Table> search = expected[i];
            DesignerVertex v0 = agraph.getVertex(search.getFirst());
            assertNotNull("[" + i + "]: " + search.toString(), v0);
            DesignerVertex v1 = agraph.getVertex(search.getSecond());
            assertNotNull("[" + i + "]: " + search.toString(), v1);

            DesignerEdge found = agraph.findEdge(v0, v1);
            assertNotNull("[" + i + "]: " + search.toString() + "\n", found);
//            System.err.println("REMOVED: " + search + " [" + found + "]");
            agraph.removeEdge(found);
        } // FOR
    }
    
    public static void main(String[] args) throws Exception {
        TestAccessGraphGenerator2 test = new TestAccessGraphGenerator2();
        test.setUp();
        test.testGenerate();
    }
}
