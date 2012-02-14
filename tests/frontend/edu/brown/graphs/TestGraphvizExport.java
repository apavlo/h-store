package edu.brown.graphs;

import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.utils.*;
import edu.brown.BaseTestCase;

public class TestGraphvizExport extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        this.applyCatalogCorrelations(ProjectType.TPCC);
    }
    
    /**
     * testExport
     */
    public void testExport() throws Exception {
        DependencyGraph dgraph = DependencyGraphGenerator.generate(catalog_db);
        assertNotNull(dgraph);
        assertTrue(dgraph.getVertexCount() > 0);
        assertTrue(dgraph.getEdgeCount() > 0);
        
        GraphvizExport<DesignerVertex, DesignerEdge> graphviz = new GraphvizExport<DesignerVertex, DesignerEdge>(dgraph);
        String output = graphviz.export("tpcc");
        for (Table catalog_tbl : CatalogUtil.getDataTables(catalog_db)) {
            if (catalog_tbl.getSystable()) continue;
            assert(output.contains(catalog_tbl.getName()));
        } // FOR
    }
}
