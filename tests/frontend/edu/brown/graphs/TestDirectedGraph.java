package edu.brown.graphs;

import java.util.*;

import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.designer.*;
import edu.brown.utils.ProjectType;


public class TestDirectedGraph extends BaseTestCase {
    
    protected static AbstractDirectedGraph<DesignerVertex, DesignerEdge> graph;
    protected static Map<String, DesignerVertex> vertex_xref = new HashMap<String, DesignerVertex>();
    protected static String path[] = { "WAREHOUSE", "DISTRICT", "CUSTOMER", "ORDERS" };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        if (graph == null) {
            graph = new AbstractDirectedGraph<DesignerVertex, DesignerEdge>(catalog_db) {
                private static final long serialVersionUID = 1L;
                // Nothing...
            };
            
            for (Table catalog_tbl : catalog_db.getTables()) {
                for (String table_name : path) {
                    if (table_name.equals(catalog_tbl.getName())) {
                        DesignerVertex v = new DesignerVertex(catalog_tbl);
                        graph.addVertex(v);
                        vertex_xref.put(catalog_tbl.getName(), v);
                        break;
                    }
                } // FOR
            } // FOR
            
            DesignerVertex previous = null;
            for (String item : path) {
                DesignerVertex v = vertex_xref.get(item);
                if (previous != null) {
                    graph.addEdge(new DesignerEdge(graph), previous, v);
                }
                previous = v;
            } // FOR
        }
    }
    
    public void testGetVertex() throws Exception {
        String table_name = "WAREHOUSE";
        DesignerVertex v = graph.getVertex(catalog_db.getTables().get(table_name));
        assertNotNull(v);
        assertEquals(table_name, v.getCatalogItem().getName());
    }

    /**
     * testGetPath
     */
    public void testGetPath() throws Exception {
        DesignerVertex v0 = vertex_xref.get(path[0]);
        DesignerVertex v1 = vertex_xref.get(path[path.length - 1]);
        
        List<DesignerEdge> ret_path = graph.getPath(v0, v1);
        assertNotNull(ret_path);
        assertEquals(path.length - 1, ret_path.size());
        for (int i = 0, cnt = ret_path.size(); i < cnt; i++) {
            DesignerEdge e = ret_path.get(i);
            assertEquals(vertex_xref.get(path[i]), graph.getSource(e));
        } // FOR
    }
    
    /**
     * testGetAncestors
     */
    public void testGetAncestors() throws Exception {
        DesignerVertex v = vertex_xref.get(path[path.length - 1]);
        List<DesignerVertex> ancestors = graph.getAncestors(v);
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
        DesignerVertex v = vertex_xref.get(path[0]);
        Set<DesignerVertex> descendants = graph.getDescendants(v);
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
        Set<DesignerVertex> roots = graph.getRoots();
//        System.out.println("ROOTS: " + roots);
        assertEquals(1, roots.size());
        assertTrue(roots.contains(vertex_xref.get(path[0])));
    }
}
