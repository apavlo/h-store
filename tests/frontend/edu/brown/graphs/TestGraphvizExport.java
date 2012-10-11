package edu.brown.graphs;

import java.io.File;

import org.voltdb.catalog.Table;

import edu.brown.catalog.conflicts.ConflictGraph;
import edu.brown.catalog.conflicts.ConflictGraph.ConflictEdge;
import edu.brown.catalog.conflicts.ConflictGraph.ConflictVertex;
import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.utils.*;
import edu.brown.BaseTestCase;

public class TestGraphvizExport extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        // this.applyParameterMappings(ProjectType.TPCC);
    }
    
    /**
     * testExportDependencyGraph
     */
    public void testExportDependencyGraph() throws Exception {
        DependencyGraph graph = DependencyGraphGenerator.generate(catalogContext);
        assertNotNull(graph);
        assertTrue(graph.getVertexCount() > 0);
        assertTrue(graph.getEdgeCount() > 0);
        
        GraphvizExport<DesignerVertex, DesignerEdge> graphviz = new GraphvizExport<DesignerVertex, DesignerEdge>(graph);
        String output = graphviz.export("tm1");
        for (Table catalog_tbl : catalogContext.getDataTables()) {
            if (catalog_tbl.getSystable()) continue;
            assert(output.contains(catalog_tbl.getName()));
        } // FOR
    }
    
    /**
     * testExportConflictGraph
     */
    public void testExportConflictGraph() throws Exception {
        ConflictGraph graph = new ConflictGraph(catalogContext.database);
        assertNotNull(graph);
        assertTrue(graph.getVertexCount() > 0);
        assertTrue(graph.getEdgeCount() > 0);
        
        GraphvizExport<ConflictVertex, ConflictEdge> graphviz = new GraphvizExport<ConflictVertex, ConflictEdge>(graph);
        String output = graphviz.export("tm1");
//        File f = FileUtil.writeStringToFile(new File("/tmp/tm1-conflict.dot"), output);
//        System.err.println("CONFLICT GRAPH: " + f);
//        for (Table catalog_tbl : catalogContext.getDataTables()) {
//            if (catalog_tbl.getSystable()) continue;
//            assert(output.contains(catalog_tbl.getName()));
//        } // FOR
    }
}
