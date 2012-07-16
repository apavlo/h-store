package edu.brown.graphs;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.utils.JSONUtil;
import edu.uci.ics.jung.graph.UndirectedSparseMultigraph;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public abstract class AbstractUndirectedGraph<V extends AbstractVertex, E extends AbstractEdge> extends UndirectedSparseMultigraph<V, E> implements Cloneable, IGraph<V, E> {
    protected static final Logger LOG = Logger.getLogger(AbstractUndirectedGraph.class.getName());
    private static final long serialVersionUID = 5267919528011628037L;
    
    /**
     * Inner state information about the graph
     */
    private final InnerGraphInformation<V, E> inner;

    /**
     * Constructor
     * @param catalog_db
     */
    public AbstractUndirectedGraph(Database catalog_db) {
        super();
        this.inner = new InnerGraphInformation<V, E>(this, catalog_db);
    }
    
    public Database getDatabase() {
        return this.inner.getDatabase();
    }
    
    // ----------------------------------------------------------------------------
    // INNER DELEGATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public int getGraphId() {
        return this.inner.getGraphId();
    }
    @Override
    public void setVerbose(boolean verbose) {
        this.inner.setVerbose(verbose);
    }
    @Override
    public void setEdgeVerbose(boolean verbose) {
        this.inner.setEdgeVerbose(verbose);
    }
    @Override
    public void setVertexVerbose(boolean verbose) {
        this.inner.setVertexVerbose(verbose);
    }
    @Override
    public void enableDirtyChecks() {
        this.inner.enableDirtyChecks();
    }
    @Override
    public List<E> getPath(V source, V target) {
        return (this.inner.getPath(source, target));
    }
    @Override
    public List<E> getPath(List<V> path) {
        return (this.inner.getPath(path));
    }
    @Override
    public String getName() {
        return (this.inner.getName());
    }
    @Override
    public void setName(String name) {
        this.inner.setName(name);
    }
    @Override
    public boolean addVertex(V v) {
        this.inner.addVertx(v);
        return super.addVertex(v);
    }
    @Override
    public V getVertex(String catalog_key) {
        return (this.inner.getVertex(catalog_key));
    }
    @Override
    public V getVertex(CatalogType catalog_item) {
        return (this.inner.getVertex(catalog_item));
    }
    @Override
    public V getVertex(Long element_id) {
        return (this.inner.getVertex(element_id));
    }
    @Override
    public void pruneIsolatedVertices() {
        this.inner.pruneIsolatedVertices();
    }
//    @Override
//    public E findEdge(V v1, V v2) {
//        return (this.inner.findEdge(v1, v2));
//    }
    
//    @Override
//    public Object clone() throws CloneNotSupportedException {
//        //
//        // Clone everything from the base graph
//        //
//        AbstractGraph clone = null; //new AbstractGraph(this.catalog_db);
//        //this.clone(clone);
//        return (clone);
//    }

//    public void clone(Graph clone) throws CloneNotSupportedException {
//        assert(clone != null);
//        //
//        // Copy Vertices
//        //
//        for (CatalogType catalog_table : this.getVertices().keySet()) {
//            clone.vertices.put(catalog_table, new V(catalog_table));
//        } // FOR
//        //
//        // Copy Edges
//        //
//        for (CatalogType catalog_table : this.getVertices().keySet()) {
//            V clone_vertex = clone.vertices.get(catalog_table);
//            V base_vertex = this.vertices.get(catalog_table);
//            
//            //
//            // Copy the child list
//            //
//            for (V base_child : base_vertex.getChildren()) {
//                clone_vertex.getChildren().add(clone.vertices.get(base_child.catalog_table));
//            } // FOR
//            
//            //
//            // Clone Edges
//            //
//            for (E base_edge : base_vertex.getEdges()) {
//                V source = clone_vertex;
//                Column source_col = base_edge.source_col;
//                V dest = clone.vertices.get(base_edge.dest.catalog_table);
//                Column dest_col = base_edge.dest_col;
//                clone_vertex.getEdges().add(new E(source, source_col, dest, dest_col, base_edge.constraint));
//            } // FOR
//        } // FOR
//        return;
//    }

    @Override
    public String toString(E e, boolean verbose) {
        return (this.inner.toString(e, verbose));
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "@" + this.hashCode());
    }
    
    public String debug() {
        return this.inner.toString();
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        GraphUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(File output_path) throws IOException {
        GraphUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        GraphUtil.serialize(this, stringer);
    }
    
    @Override
    public void fromJSON(JSONObject jsonObject, Database catalog_db) throws JSONException {
        int id = GraphUtil.deserialize(this, catalog_db, jsonObject);
        this.inner.setGraphId(id);
    }
}
