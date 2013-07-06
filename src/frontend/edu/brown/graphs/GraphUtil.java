package edu.brown.graphs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.uci.ics.jung.graph.util.Pair;

public abstract class GraphUtil {
    private static final Logger LOG = Logger.getLogger(GraphUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public enum Members {
        ID,
        VERTEX_CLASS,
        VERTICES,
        EDGE_CLASS,
        EDGES,
    }

    /**
     * Remove all of the edges that are not incident to the given vertex
     * @param graph
     * @param v
     * @return Returns the set of edges that were removed
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> Collection<E> removeEdgesWithoutVertex(IGraph<V, E> graph, V...vertices) {
        if (debug.val) LOG.debug("Removing edges that are not incident to " + Arrays.toString(vertices));
        Set<E> toRemove = new HashSet<E>();
        for (E e : graph.getEdges()) {
            boolean found = false;
            for (V v : vertices) {
                if (graph.isIncident(v, e)) {
                    found = true;
                    break;
                }
            } // FOR
            if (found == false) toRemove.add(e);
        } // FOR
        for (E e : toRemove) {
            graph.removeEdge(e);
        } // FOR
        return (toRemove);
    }
    
    /**
     * Remove any edges where the source and destination are the same vertex
     * @param graph
     * @return Returns the set of edges that were removed
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> Collection<E> removeLoopEdges(IGraph<V, E> graph) {
        Set<E> toRemove = new HashSet<E>();
        for (E e : graph.getEdges()) {
            Collection<V> vertices = graph.getIncidentVertices(e);
            if (vertices == null) continue;
            if (vertices.size() == 1) {
                toRemove.add(e);
            }
            else if (CollectionUtil.first(vertices) == CollectionUtil.get(vertices, 1)) {
                toRemove.add(e);
            }
        } // FOR
        for (E e : toRemove) {
            graph.removeEdge(e);
        } // FOR
        return (toRemove);
    }
    
    /**
     * 
     * @param graph
     * @return Returns the set of vertices that were removed
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> Collection<V> removeDisconnectedVertices(IGraph<V, E> graph) {
        Set<V> toRemove = new HashSet<V>();
        for (V v : graph.getVertices()) {
            if (graph.getIncidentEdges(v).isEmpty()) {
                toRemove.add(v);
            }
        } // FOR
        for (V v : toRemove) {
            graph.removeVertex(v);
        }
        return (toRemove);
    }
    
    /**
     * Remove duplicate edges between every unique pair of vertices
     * @param graph
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> void removeDuplicateEdges(IGraph<V, E> graph) {
        Set<E> toRemove = new HashSet<E>();
        for (V v0 : graph.getVertices()) {
            for (V v1 : graph.getVertices()) {
                Collection<E> edges = graph.findEdgeSet(v0, v1);
                if (edges == null || edges.size() <= 1) continue;
                
                toRemove.clear();
                boolean first = true;
                for (E e : edges) {
                    if (first) {
                        first = false;
                    } else {
                        toRemove.add(e);
                    }
                } // FOR
                for (E e : toRemove) {
                    graph.removeEdge(e);
                } // FOR
            } // FOR
        } // FOR
    }
    
    /**
     * 
     * @param <V>
     * @param <E>
     * @param graph
     * @param path0
     * @param path1
     * @return
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> String comparePathsDebug(IGraph<V, E> graph, List<V> path0, List<V> path1) {
        StringBuilder sb = new StringBuilder();

        final String match = "\u2713";
        final String conflict = "X";
        
        int ctr0 = 0;
        int cnt0 = path0.size();
        
        int ctr1 = 0;
        int cnt1 = path1.size();
        
        while (ctr0 < cnt0 || ctr1 < cnt1) {
            V v0 = null;
            if (ctr0 < cnt0) v0 = path0.get(ctr0++);
            
            V v1 = null;
            if (ctr1 < cnt1) v1 = path1.get(ctr1++);
            
            String status;
            if (v0 != null && v1 != null) {
                status = (v0.equals(v1) ? match : conflict);
            } else {
                status = conflict;
            }
            sb.append(String.format("| %s | %-50s | %-50s |\n", status, v0, v1));
            sb.append(StringUtil.repeat("-", 110)).append("\n");
        } // FOR
        
        return (sb.toString());
    }
    
    
    /**
     * For a given graph, write out a serialized form of it to the target output_path
     * @param <V>
     * @param <E>
     * @param graph
     * @param output_path
     * @throws Exception
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> void save(IGraph<V, E> graph, File output_path) throws IOException {
        if (debug.val) LOG.debug("Writing out graph to '" + output_path + "'");
        
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            GraphUtil.serialize(graph, stringer);
            stringer.endObject();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        
        String json = stringer.toString();
        try {
            FileUtil.writeStringToFile(output_path, JSONUtil.format(json));
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        if (debug.val) LOG.debug("Graph was written out to '" + output_path + "'");
    }
    
    /**
     * For a given graph, write out its contents in serialized form into the provided Stringer
     * @param <V>
     * @param <E>
     * @param graph
     * @param stringer
     * @throws JSONException
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> void serialize(IGraph<V, E> graph, JSONStringer stringer) throws JSONException {
        GraphUtil.serialize(graph, null, null, stringer);
    }
    
    /**
     * For a given graph, write out its contents in serialized form into the provided Stringer
     * Can provide sets of vertices or edges to not be serialized out
     * @param <V>
     * @param <E>
     * @param graph
     * @param ignore_v
     * @param ignore_e
     * @param stringer
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    public static <V extends AbstractVertex, E extends AbstractEdge> void serialize(IGraph<V, E> graph, Collection<V> ignore_v, Collection<E> ignore_e, JSONStringer stringer) throws JSONException {
        int e_cnt = 0;
        int v_cnt = 0;
        int e_skipped = 0;
        
        // Graph ID
        stringer.key(Members.ID.name()).value(graph.getGraphId());
        
        // Vertices
        assert(graph.getVertexCount() > 0) : "Graph has no vertices";
        stringer.key(Members.VERTICES.name()).array();
        Class<V> v_class = null;
        Set<Long> all_vertices = new HashSet<Long>();
        for (V v : graph.getVertices()) {
            if (ignore_v != null && ignore_v.contains(v)) continue;
            if (v_class == null) {
                v_class = (Class<V>)v.getClass();
                if (debug.val) LOG.debug("Discovered vertex class: " + v_class.getName());
            }
            stringer.object();
            v.toJSON(stringer);
            stringer.endObject();
            all_vertices.add(v.getElementId());
            v_cnt++;
            if (trace.val) LOG.trace("V [" + v.getElementId() + "]");
        } // FOR
        stringer.endArray();
        stringer.key(Members.VERTEX_CLASS.name()).value(v_class.getName());
        if (debug.val) LOG.debug("# of Vertices: " + v_cnt);
        
        // Edges
        if (graph.getEdgeCount() > 0) {
            stringer.key(Members.EDGES.name()).array();
            Class<E> e_class = null;
            for (E e : graph.getEdges()) {
                if (ignore_e != null && ignore_e.contains(e)) continue;
                if (e_class == null) {
                    e_class = (Class<E>)e.getClass();
                    if (debug.val) LOG.debug("Discovered edge class: " + e_class.getName());
                }
                // Thread synchronization issue
                // This is an attempt to prevent us from writing out edges that have vertices
                // that were added in between the time that we originally wrote out the list of vertices
                V v0 = graph.getSource(e);
                V v1 = graph.getDest(e);
                if (v0 == null) {
                    Pair<V> pair = graph.getEndpoints(e);
                    v0 = pair.getFirst();
                    v1 = pair.getSecond();
                }
                assert(v0 != null) : e; 
                assert(v1 != null) : e; 
                
                if (ignore_v != null && (ignore_v.contains(v0) || ignore_v.contains(v1))) continue;
                if (all_vertices.contains(v0.getElementId()) && all_vertices.contains(v1.getElementId())) {
                    if (trace.val) LOG.trace(String.format("E [%d] %d => %d", e.getElementId(), v0.getElementId(), v1.getElementId()));
                    stringer.object();
                    e.toJSON(stringer);
                    stringer.endObject();
                    e_cnt++;
                } else {
                    e_skipped++;
                }
            } // FOR
            stringer.endArray();
            stringer.key(Members.EDGE_CLASS.name()).value(e_class.getName());
        }
        if (e_skipped > 0) LOG.warn(String.format("Skipped %d out of %d edges", e_skipped, graph.getEdgeCount()));
        if (debug.val) LOG.debug("# of Edges: " + e_cnt);
        return;
    }
    
    /**
     * Given a graph and a path to a serialized file, load in all of the vertices+edges into the graph 
     * @param <V>
     * @param <E>
     * @param graph
     * @param catalog_db
     * @param path
     * @throws Exception
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> void load(IGraph<V, E> graph, Database catalog_db, File path) throws IOException {
        if (debug.val) LOG.debug("Loading in serialized graph from '" + path + "'");
        String contents = FileUtil.readFile(path);
        if (contents.isEmpty()) {
            throw new IOException("The workload statistics file '" + path + "' is empty");
        }
        try {
            GraphUtil.deserialize(graph, catalog_db, new JSONObject(contents));
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        if (debug.val) LOG.debug("Graph loading is complete");
        return;
    }
    
    /**
     * Deserialize the provided JSONObject into the Graph object
     * @param <V>
     * @param <E>
     * @param graph
     * @param catalog_db
     * @param jsonObject
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static <V extends AbstractVertex, E extends AbstractEdge> int deserialize(IGraph<V, E> graph, Database catalog_db, JSONObject jsonObject) throws JSONException {
        // Graph Id
        int id = jsonObject.getInt(Members.ID.name());
        
        // Vertices
        String v_className = jsonObject.getString(Members.VERTEX_CLASS.name());
        
        // 2011-07-13: Fix for MarkovVertex
        v_className = v_className.replace("markov.Vertex", "markov.MarkovVertex");
        Class<V> v_class = (Class<V>)ClassUtil.getClass(v_className);
        if (debug.val) LOG.debug("Vertex class is '" + v_class.getName() + "'");
        
        JSONArray jsonArray = jsonObject.getJSONArray(Members.VERTICES.name());
        for (int i = 0, cnt = jsonArray.length(); i < cnt; i++) {
            V vertex = null;
            try {
                vertex = v_class.newInstance();
            } catch (Exception ex) {
                LOG.fatal("Failed to create new instance of " + v_class.getName());
                throw new JSONException(ex);
            }
            JSONObject jsonVertex = jsonArray.getJSONObject(i);
            vertex.fromJSON(jsonVertex, catalog_db);
            graph.addVertex(vertex);
        } // FOR
        
        // Edges
        // If the EDGE_CLASS is missing, don't bother loading any edges
        if (jsonObject.has(Members.EDGE_CLASS.name())) {
            String e_className = jsonObject.getString(Members.EDGE_CLASS.name());
            
            // 2011-07-13: Fix for MarkovVertex
            e_className = e_className.replace("markov.Edge", "markov.MarkovEdge");
            Class<E> e_class = (Class<E>)ClassUtil.getClass(e_className);
            if (debug.val) LOG.debug("Edge class is '" + v_class.getName() + "'");
            
            jsonArray = jsonObject.getJSONArray(Members.EDGES.name());
            for (int i = 0, cnt = jsonArray.length(); i < cnt; i++) {
                E edge = ClassUtil.newInstance(e_class, new Object[] { graph }, new Class<?>[]{ IGraph.class });
                JSONObject jsonEdge = jsonArray.getJSONObject(i);
                edge.fromJSON(jsonEdge, catalog_db);
            } // FOR
        }
        
        return (id);
    }
    
    /**
     * 
     * @param <V>
     * @param <E>
     * @param graph
     */
    @SuppressWarnings("unchecked")
    public static <V extends AbstractVertex, E extends AbstractEdge> void visualizeGraph(IGraph<V, E> graph) {
        GraphVisualizationPanel.createFrame(graph).setVisible(true);
        ThreadUtil.sleep(10000);
    }
    
}