package edu.brown.graphs;

import java.io.File;
import java.util.Collection;

import edu.brown.BaseTestCase;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;
import edu.uci.ics.jung.graph.util.EdgeType;

public class TestGraphUtil extends BaseTestCase {
    
    protected static AbstractDirectedGraph<Vertex, Edge> graph;
    protected static Vertex root;
    protected File tempFile;
    protected static String tables[] = { "SUBSCRIBER", "ACCESS_INFO", "SPECIAL_FACILITY" };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        if (graph == null) {
            graph = new AbstractDirectedGraph<Vertex, Edge>(catalog_db) {
                private static final long serialVersionUID = 1L;
            };
            root = new Vertex(this.getTable("SUBSCRIBER"));
            graph.addVertex(root);
            
            for (int i = 1; i < tables.length; i++) {
                String table_name = tables[i];
                Vertex child = new Vertex(this.getTable(table_name));
                graph.addEdge(new Edge(graph), root, child);
            } // FOR
        }
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (this.tempFile != null && this.tempFile.exists()) {
            this.tempFile.delete();
        }
    }
    
    /**
     * 
     * @param edge_type
     * @param clone
     */
    private void checkGraph(EdgeType edge_type, IGraph<Vertex, Edge> clone) {
        for (Vertex v : graph.getVertices()) {
            Vertex clone_v = graph.getVertex(v.getCatalogItem());
            assertNotNull(clone_v);
        } // FOR
        for (Edge e : graph.getEdges()) {
            Collection<Vertex> vertices = graph.getIncidentVertices(e);
            Vertex v0 = CollectionUtil.get(vertices, 0);
            assertNotNull(v0);
            Vertex v1 = CollectionUtil.get(vertices, 1);
            assertNotNull(v1);
            
            Vertex clone_v0 = clone.getVertex(v0.getCatalogKey());
            assertNotNull(clone_v0);
            Vertex clone_v1 = clone.getVertex(v1.getCatalogKey());
            assertNotNull(clone_v1);
            Collection<Edge> clone_e = clone.findEdgeSet(clone_v0, clone_v1);
            assertFalse(clone_e.isEmpty());
            assertEquals(1, clone_e.size());
            assertEquals(edge_type, clone.getEdgeType(CollectionUtil.getFirst(clone_e)));
        } // FOR
    }
    
    /**
     * 
     * @throws Exception
     */
    private void writeFile() throws Exception {
        if (this.tempFile == null) {
            this.tempFile = File.createTempFile("graph-", null);
            GraphUtil.save(graph, tempFile.getAbsolutePath());
        }
    }
    
    /**
     * testSave
     */
    public void testSave() throws Exception {
        this.writeFile();
        
        String contents = FileUtil.readFile(tempFile);
        assertFalse(contents.isEmpty());
        
        for (String table_name : tables) {
            assertTrue(contents.contains(table_name));
        } // FOR
        // System.out.println(contents);
    }
    
    /**
     * testLoadDirected
     */
    public void testLoadDirected() throws Exception {
        this.writeFile();
        
        String contents = FileUtil.readFile(tempFile);
        assertFalse(contents.isEmpty());
        //System.out.println(contents);
        
        AbstractDirectedGraph<Vertex, Edge> clone = new AbstractDirectedGraph<Vertex, Edge>(catalog_db) {
            private static final long serialVersionUID = 1L;
        };
        GraphUtil.load(clone, catalog_db, this.tempFile.getAbsolutePath());
        this.checkGraph(EdgeType.DIRECTED, clone);
    }
    
    /**
     * testLoadUnDirected
     */
//    public void testLoadUnDirected() throws Exception {
//        this.writeFile();
//        
//        String contents = FileUtil.readFile(tempFile);
//        assertFalse(contents.isEmpty());
//        //System.out.println(contents);
//        
//        AbstractUndirectedGraph<Vertex, Edge> clone = new AbstractUndirectedGraph<Vertex, Edge>(catalog_db) {
//        };
//        GraphUtil.load(clone, catalog_db, this.tempFile.getAbsolutePath());
//        this.checkGraph(EdgeType.DIRECTED, clone);
//    }

}
