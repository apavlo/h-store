package edu.brown.graphs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogKey;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.algorithms.shortestpath.ShortestPathUtils;
import edu.uci.ics.jung.algorithms.shortestpath.UnweightedShortestPath;
import edu.uci.ics.jung.graph.util.EdgeType;

public class InnerGraphInformation<V extends AbstractVertex, E extends AbstractEdge> {
//    private static final Logger LOG = Logger.getLogger(InnerGraphInformation.class);
    
    private static final AtomicInteger NEXT_GRAPH_ID = new AtomicInteger(10000);
    
    private enum DirtyIndex {
        DESCENDANTS,
        ANCESTORS,
        ROOTS,
    };
    
    private Integer graph_id;
    private final Database catalog_db;
    private final IGraph<V, E> graph;
    private final Map<String, V> catalog_vertex_xref = new HashMap<String, V>();
    private final Map<Long, V> element_id_xref = new HashMap<Long, V>();
    private String name;
    private boolean enable_dirty_checks = false;
    private final boolean dirty[];

    private ListOrderedSet<V> descendants;
    private List<V> ancestors;
    private ListOrderedSet<V> roots;
    
//    private transient final Map<V, Map<V, E>> cache_findEdge = new ConcurrentHashMap<V, Map<V, E>>();
    
    /**
     * Constructor
     * @param graph
     * @param catalog_db
     */
    protected InnerGraphInformation(IGraph<V, E> graph, Database catalog_db) {
        this.graph = graph;
        this.catalog_db = catalog_db;
        
        this.dirty = new boolean[DirtyIndex.values().length];
        this.markAllDirty(false);
    }
    
    public Database getDatabase() {
        return catalog_db;
    }
    
    public int getGraphId() {
        if (this.graph_id == null) {
            synchronized (this) {
                if (this.graph_id == null) this.graph_id = NEXT_GRAPH_ID.getAndIncrement();
            }
        }
        return (this.graph_id.intValue());
    }
    public synchronized void setGraphId(int id) {
        this.graph_id = id;
        NEXT_GRAPH_ID.set(this.graph_id);
    }
    
    /**
     * Sets verbose output for all elements
     * @param verbose
     */
    public void setVerbose(boolean verbose) {
        this.setEdgeVerbose(verbose);
        this.setVertexVerbose(verbose);
    }
    public void setEdgeVerbose(boolean verbose) {
        for (E e : this.graph.getEdges()) {
            e.setVerbose(verbose);
        } // FOR
    }
    public void setVertexVerbose(boolean verbose) {
        for (V v : this.graph.getVertices()) {
            v.setVerbose(verbose);
        } // FOR
    }
    
    /**
     * Enable internal book keeping for when the graph has been modified
     */
    public void enableDirtyChecks() {
        this.enable_dirty_checks = true;
        this.markAllDirty(true);
    }
    
    /**
     * Returns true if this has been modified 
     * @return
     */
    protected boolean isDirty(DirtyIndex idx) {
        return (this.enable_dirty_checks == true && this.dirty[idx.ordinal()]);
    }
    /**
     * Reset the internal dirty bit for this graph to false
     */
    protected void resetDirty(DirtyIndex idx) {
        this.dirty[idx.ordinal()] = false;
    }
    /**
     * Mark all of the internal dirty flags as the given value
     */
    private void markAllDirty(boolean value) {
        for (int i = 0, cnt = this.dirty.length; i < cnt; i++) {
            this.dirty[i] = value;
        } // FOR
    }
    
    public synchronized void addVertx(V v) {
        this.catalog_vertex_xref.put(CatalogKey.createKey(v.getCatalogItem()), v);
        this.element_id_xref.put(v.getElementId(), v);
        if (this.enable_dirty_checks) this.markAllDirty(true);
    }
    
    public synchronized void addEdge(E e) {
        if (this.enable_dirty_checks) this.markAllDirty(true);
    }
    
//    public E findEdge(V v1, V v2) {
//        this.graph.
//        return (this.graph.findEdge(v1, v2));
////        E e = null;
////        synchronized (v1) {
////            Map<V, E> cache = this.cache_findEdge.get(v1);
////            if (cache == null) {
////                cache = new HashMap<V, E>();
////                this.cache_findEdge.put(v1, cache);
////            }
////            e = cache.get(v2);
////            if (v2 == null) {
////                e = this.graph.findEdge(v1, v2);
////                cache.put(v2, e);
////            }
////            System.err.println(v2 + ": " + cache);
////        } // SYNCH
////        return (e);
//    }
    
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
//                if (e == null) return (ret);
                assert(e != null) : "No edge exists between " + v + " and " + last;
                ret.add(e);
            }
            last = v;
        } // FOR
        return (ret);
    }
    
    /**
     * Return the set of vertices that are descedants of the given vertices
     * @param vertex
     * @return
     */
    public synchronized Set<V> getDescendants(V vertex) {
        boolean recompute = false;
        
        // First time
        if (this.descendants == null) {
            this.descendants = new ListOrderedSet<V>();
            recompute = true;
        // Check whether it's been marked as dirty. If it hasn't, then
        // we know that our cache is still valid
        } else if (this.isDirty(DirtyIndex.DESCENDANTS)) {
            recompute = true;
        }
        
        if (recompute) {
            this.descendants.clear();
            new VertexTreeWalker<V, E>(this.graph) {
                @Override
                protected void callback(V element) {
                    descendants.add(element);
                }
            }.traverse(vertex);
            // Probably a race condition...
            this.resetDirty(DirtyIndex.DESCENDANTS);
        }
        return (Collections.unmodifiableSet(this.descendants));
    }
    
    /**
     * Get the path of ancestor vertices up to a root
     * @param vertex
     * @return
     */
    public synchronized List<V> getAncestors(final V vertex) {
        boolean recompute = false;
        
        // First time
        if (this.ancestors == null) {
            this.ancestors = new ArrayList<V>();
            recompute = true;
        // Check whether it's been marked as dirty. If it hasn't, then
        // we know that our cache is still valid
        } else if (this.isDirty(DirtyIndex.ANCESTORS)) {
            recompute = true;
        }
        
        if (recompute) {
            this.ancestors.clear();
            this.buildAncestorsList(vertex);
            // Probably a race condition...
            this.resetDirty(DirtyIndex.DESCENDANTS);
        }
        return (Collections.unmodifiableList(this.ancestors));
    }
    
    private void buildAncestorsList(final V v) {
        for (V parent : this.graph.getPredecessors(v)) {
            if (!this.ancestors.contains(parent)) {
                this.ancestors.add(parent);
                this.buildAncestorsList(parent);
            }
        } // FOR
        return;
    }
    
    /**
     * Get the lists of roots for this graph. 
     * This probably won't work if the graph is undirected
     * @return
     */
    public synchronized Set<V> getRoots() {
        boolean recompute = false;
        
        // First time
        if (this.roots == null) {
            this.roots = new ListOrderedSet<V>();
            recompute = true;
        // Check whether it's been marked as dirty. If it hasn't, then
        // we know that our cache is still valid
        } else if (this.isDirty(DirtyIndex.ROOTS)) {
            recompute = true;
        }

        if (recompute) {
            this.roots.clear();
            for (V v : this.graph.getVertices()) {
                if (this.graph.getPredecessorCount(v) == 0) {
                    this.roots.add(v);
                }
            } // FOR
            // Probably a race condition
            this.resetDirty(DirtyIndex.ROOTS);
        }
        return (Collections.unmodifiableSet(this.roots));
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
    
    @Override
    public String toString() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("Vertices", StringUtil.join("\n", this.graph.getVertices()));
        m.put("Edges", StringUtil.join("\n", this.graph.getEdges()));
        return StringUtil.formatMaps(m);
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
        return (source_lbl + edge_marker + dest_lbl);
    }
}