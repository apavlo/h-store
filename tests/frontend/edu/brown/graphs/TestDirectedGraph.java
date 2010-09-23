package edu.brown.graphs;

import java.util.*;

import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.designer.*;
import edu.brown.utils.ProjectType;


public class TestDirectedGraph extends BaseTestCase {
    
    protected static AbstractDirectedGraph<Vertex, Edge> graph;
    protected static Map<String, Vertex> vertex_xref = new HashMap<String, Vertex>();
    protected static String path[] = { "WAREHOUSE", "DISTRICT", "CUSTOMER", "ORDERS" };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        if (graph == null) {
            graph = new AbstractDirectedGraph<Vertex, Edge>(catalog_db) {
                private static final long serialVersionUID = 1L;
                // Nothing...
            };
            
            for (Table catalog_tbl : catalog_db.getTables()) {
                for (String table_name : path) {
                    if (table_name.equals(catalog_tbl.getName())) {
                        Vertex v = new Vertex(catalog_tbl);
                        graph.addVertex(v);
                        vertex_xref.put(catalog_tbl.getName(), v);
                        break;
                    }
                } // FOR
            } // FOR
            
            Vertex previous = null;
            for (String item : path) {
                Vertex v = vertex_xref.get(item);
                if (previous != null) {
                    graph.addEdge(new Edge(graph), previous, v);
                }
                previous = v;
            } // FOR
        }
    }
    
    public void testGetVertex() throws Exception {
        String table_name = "WAREHOUSE";
        Vertex v = graph.getVertex(catalog_db.getTables().get(table_name));
        assertNotNull(v);
        assertEquals(table_name, v.getCatalogItem().getName());
    }

    /**
     * testGetPath
     */
    public void testGetPath() throws Exception {
        Vertex v0 = vertex_xref.get(path[0]);
        Vertex v1 = vertex_xref.get(path[path.length - 1]);
        
        List<Edge> ret_path = graph.getPath(v0, v1);
        assertNotNull(ret_path);
        assertEquals(path.length - 1, ret_path.size());
        for (int i = 0, cnt = ret_path.size(); i < cnt; i++) {
            Edge e = ret_path.get(i);
            assertEquals(vertex_xref.get(path[i]), graph.getSource(e));
        } // FOR
    }
    
    /**
     * testGetAncestors
     */
    public void testGetAncestors() throws Exception {
        Vertex v = vertex_xref.get(path[path.length - 1]);
        List<Vertex> ancestors = graph.getAncestors(v);
        assertEquals(path.length - 1, ancestors.size());
//        System.out.println("Ancestors: " + ancestors);
        
        int j = 0;
        for (int i = ancestors.size() - 1; i >= 0; i--) {
            assertEquals(vertex_xref.get(path[j++]), ancestors.get(i));
        } // FOR
    }
    
    /**
     * testGetDescendants
     */
    public void testGetDescendants() throws Exception {
        Vertex v = vertex_xref.get(path[0]);
        Set<Vertex> descendants = graph.getDescendants(v);
//        System.out.println("Descedants: " + descendants);
        assertEquals(path.length, descendants.size());
        
        for (int i = 0; i < path.length; i++) {
            assertTrue(descendants.contains(vertex_xref.get(path[i])));
        }
    }
 
    /**
     * testGetRoots
     */
    public void testGetRoots() throws Exception {
        Set<Vertex> roots = graph.getRoots();
//        System.out.println("ROOTS: " + roots);
        assertEquals(1, roots.size());
        assertTrue(roots.contains(vertex_xref.get(path[0])));
    }
}
