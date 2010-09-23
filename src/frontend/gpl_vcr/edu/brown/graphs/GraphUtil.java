package edu.brown.graphs;

import java.io.*;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.*;
import org.voltdb.catalog.Database;

import edu.brown.utils.ClassUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public abstract class GraphUtil {
    protected static final Logger LOG = Logger.getLogger(GraphUtil.class.getName());

    public enum Members {
        VERTEX_CLASS,
        VERTICES,
        EDGE_CLASS,
        EDGES,
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
    public static <V extends AbstractVertex, E extends AbstractEdge> String comparePaths(IGraph<V, E> graph, List<V> path0, List<V> path1) {
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
    public static <V extends AbstractVertex, E extends AbstractEdge> void save(IGraph<V, E> graph, String output_path) throws IOException {
        LOG.debug("Writing out graph to '" + output_path + "'");
        
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
            FileUtil.writeStringToFile(new File(output_path), JSONUtil.format(json));
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        LOG.debug("Graph was written out to '" + output_path + "'");
    }
    
    /**
     * For a given graph, write out its contents in serialized form into the provided Stringer
     * @param <V>
     * @param <E>
     * @param graph
     * @param stringer
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    public static <V extends AbstractVertex, E extends AbstractEdge> void serialize(IGraph<V, E> graph, JSONStringer stringer) throws JSONException {
        // Vertices
        assert(graph.getVertexCount() > 0) : "Graph has no vertices";
        stringer.key(Members.VERTICES.name()).array();
        Class<V> v_class = null;
        for (V vertex : graph.getVertices()) {
            if (v_class == null) {
                v_class = (Class<V>)vertex.getClass();
                LOG.debug("Discovered vertex class: " + v_class.getName());
            }
            stringer.object();
            vertex.toJSON(stringer);
            stringer.endObject();
        } // FOR
        stringer.endArray();
        stringer.key(Members.VERTEX_CLASS.name()).value(v_class.getName());
        
        // Edges
        if (graph.getEdgeCount() > 0) {
            stringer.key(Members.EDGES.name()).array();
            Class<E> e_class = null;
            for (E edge : graph.getEdges()) {
                if (e_class == null) {
                    e_class = (Class<E>)edge.getClass();
                    LOG.debug("Discovered edge class: " + e_class.getName());
                }
                stringer.object();
                edge.toJSON(stringer);
                stringer.endObject();
            } // FOR
            stringer.endArray();
            stringer.key(Members.EDGE_CLASS.name()).value(e_class.getName());
        }
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
    public static <V extends AbstractVertex, E extends AbstractEdge> void load(IGraph<V, E> graph, Database catalog_db, String path) throws IOException {
        LOG.debug("Loading in serialized graph from '" + path + "'");
        String contents = FileUtil.readFile(path);
        if (contents.isEmpty()) {
            throw new IOException("The workload statistics file '" + path + "' is empty");
        }
        try {
            GraphUtil.deserialize(graph, catalog_db, new JSONObject(contents));
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        LOG.debug("Graph loading is complete");
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
    public static <V extends AbstractVertex, E extends AbstractEdge> void deserialize(IGraph<V, E> graph, Database catalog_db, JSONObject jsonObject) throws JSONException {
        // Vertices
        String v_className = jsonObject.getString(Members.VERTEX_CLASS.name());
        Class<V> v_class = (Class<V>)ClassUtil.getClass(v_className);
        LOG.debug("Vertex class is '" + v_class.getName() + "'");
        
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
            Class<E> e_class = (Class<E>)ClassUtil.getClass(e_className);
            LOG.debug("Edge class is '" + v_class.getName() + "'");
            
            jsonArray = jsonObject.getJSONArray(Members.EDGES.name());
            for (int i = 0, cnt = jsonArray.length(); i < cnt; i++) {
                E edge = ClassUtil.newInstance(e_class, new Object[] { graph }, new Class<?>[]{ IGraph.class });
                JSONObject jsonEdge = jsonArray.getJSONObject(i);
                edge.fromJSON(jsonEdge, catalog_db);
            } // FOR
        }
    }
    
}