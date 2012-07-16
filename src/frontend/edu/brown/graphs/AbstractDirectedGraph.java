package edu.brown.graphs;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.utils.ClassUtil;
import edu.brown.utils.JSONUtil;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.graph.util.Pair;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public abstract class AbstractDirectedGraph<V extends AbstractVertex, E extends AbstractEdge> extends DirectedSparseMultigraph<V, E> implements IGraph<V, E> {
    protected static final Logger LOG = Logger.getLogger(AbstractDirectedGraph.class);
    private static final long serialVersionUID = 5267919528011628037L;

    /**
     * Inner state information about the graph
     */
    private final InnerGraphInformation<V, E> inner;

    /**
     * Constructor
     * @param catalog_db
     */
    public AbstractDirectedGraph(Database catalog_db) {
        super();
        this.inner = new InnerGraphInformation<V, E>(this, catalog_db);
    }


    // ----------------------------------------------------------------------------
    // INNER DELEGATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public int getGraphId() {
        return this.inner.getGraphId();
    }
    public Set<V> getDescendants(V vertex) {
        return (this.inner.getDescendants(vertex));
    }
    public List<V> getAncestors(V vertex) {
        return (this.inner.getAncestors(vertex));
    }
    public Set<V> getRoots() {
        return (this.inner.getRoots());
    }
    public Database getDatabase() {
        return (this.inner.getDatabase());
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
    @Override
    public boolean addEdge(E edge, Pair<? extends V> endpoints, EdgeType edgeType) {
        boolean ret = super.addEdge(edge, endpoints, edgeType);
        this.inner.addEdge(edge);
        return (ret);
    }
//    @Override
//    public E findEdge(V v1, V v2) {
//        return (this.inner.findEdge(v1, v2));
//    }

    /**
     * Makes unique copies of the vertices and edges into the cloned graph
     * @param copy_source
     * @throws CloneNotSupportedException
     */
    @SuppressWarnings("unchecked")
    public void clone(IGraph<V, E> copy_source) throws CloneNotSupportedException {
        //
        // Copy Vertices
        //
        for (V v : copy_source.getVertices()) {
            this.addVertex(v);
        } // FOR
        //
        // Copy Edges
        //
        Constructor<E> constructor = null;
        try {
            for (E edge : copy_source.getEdges()) {
                if (constructor == null) {
                    Class<?> params[] = new Class<?>[] { IGraph.class, AbstractEdge.class };  
                    constructor = (Constructor<E>)ClassUtil.getConstructor(edge.getClass(), params);
                }
                Pair<V> endpoints = copy_source.getEndpoints(edge);
                E new_edge = constructor.newInstance(new Object[]{ this, edge });
                this.addEdge(new_edge, endpoints, copy_source.getEdgeType(edge));
            } // FOR
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return;
    }
    
    @Override
    public String toString(E e, boolean verbose) {
        return (this.inner.toString(e, verbose));
    }

    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "@" + this.hashCode());
    }
    
    public String debug() {
        return super.toString();
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
