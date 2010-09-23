package edu.brown.graphs;

import org.voltdb.catalog.Table;

import edu.brown.designer.DependencyGraph;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.utils.*;
import edu.brown.BaseTestCase;

public class TestGraphvizExport extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
    }
    
    /**
     * testExport
     */
    public void testExport() throws Exception {
        DependencyGraph dgraph = DependencyGraphGenerator.generate(catalog_db);
        assertNotNull(dgraph);
        assertTrue(dgraph.getVertexCount() > 0);
        assertTrue(dgraph.getEdgeCount() > 0);
        
        GraphvizExport<Vertex, Edge> graphviz = new GraphvizExport<Vertex, Edge>(dgraph);
        String output = graphviz.export("tpcc");
        for (Table catalog_tbl : catalog_db.getTables()) {
            assert(output.contains(catalog_tbl.getName()));
        } // FOR
    }
}
