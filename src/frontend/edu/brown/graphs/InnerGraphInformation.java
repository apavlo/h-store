package edu.brown.graphs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogKey;
import edu.uci.ics.jung.algorithms.shortestpath.ShortestPathUtils;
import edu.uci.ics.jung.algorithms.shortestpath.UnweightedShortestPath;
import edu.uci.ics.jung.graph.util.EdgeType;

public class InnerGraphInformation<V extends AbstractVertex, E extends AbstractEdge> {

    private final Database catalog_db;
    private final IGraph<V, E> graph;
    private final Map<String, V> catalog_vertex_xref = new HashMap<String, V>();
    private final Map<Long, V> element_id_xref = new HashMap<Long, V>();
    private String name;
    
    /**
     * Constructor
     * @param graph
     * @param catalog_db
     */
    protected InnerGraphInformation(IGraph<V, E> graph, Database catalog_db) {
        this.graph = graph;
        this.catalog_db = catalog_db;
    }
    
    public Database getDatabase() {
        return catalog_db;
    }
    
    public void addVertx(V v) {
        this.catalog_vertex_xref.put(CatalogKey.createKey(v.getCatalogItem()), v);
        this.element_id_xref.put(v.getElementId(), v);
    }
    public V getVertex(String catalog_key) {
        return (this.catalog_vertex_xref.get(catalog_key));
    }
    public V getVertex(CatalogType catalog_item) {
        return (this.catalog_vertex_xref.get(CatalogKey.createKey(catalog_item)));
    }
    public V getVertex(Long element_id) {
        return (this.element_id_xref.get(element_id));
    }
    public List<E> getPath(V source, V target) {
        return (ShortestPathUtils.getPath(this.graph, new UnweightedShortestPath<V, E>(this.graph), source, target));
    }
    /**
     * For the given list of vertices, return the list of edges that connect consecutive vertices in the path 
     * @param path
     * @return
     */
    public List<E> getPath(List<V> path) {
        final List<E> ret = new ArrayList<E>(); 
        V last = null;
        for (V v : path) {
            if (last != null) {
                E e = this.graph.findEdge(last, v);
                assert(e != null) : "No edge exists between " + v + " and " + last;
                ret.add(e);
            }
            last = v;
        } // FOR
        return (ret);
    }
    public void pruneIsolatedVertices() {
        List<V> vertices = new ArrayList<V>(this.graph.getVertices());
        for (V V : vertices) {
            if (this.graph.getIncidentEdges(V).isEmpty()) {
                this.graph.removeVertex(V);
            }
        } // FOR
    }
    
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    
    public String toString(E e, boolean verbose) {
        String edge_marker = null;
        V source = null;
        V dest = null;
        
        // Directed
        if (this.graph.getEdgeType(e) == EdgeType.DIRECTED) {
            edge_marker = "->";
            source = this.graph.getSource(e);
            dest = this.graph.getDest(e);
        // Undirected
        } else {
            edge_marker = "--";
            for (V v : this.graph.getIncidentVertices(e)) {
                if (source == null) source = v;
                else if (dest == null) dest = v;
                else assert(false);
            } // FOR
        }
        assert(source != null);
        assert(dest != null);
        
        String source_lbl = source.toString();
        String dest_lbl = dest.toString();

        // If the edge doesn't have verbose output enabled, truncate the vertex labels
        if (verbose == false) {
            source_lbl = source_lbl.substring(0, 2);
            dest_lbl = dest_lbl.substring(0, 2);
        }
        return (String.format("%s %s %s", source_lbl, edge_marker, dest_lbl));
    }
    
    
}
