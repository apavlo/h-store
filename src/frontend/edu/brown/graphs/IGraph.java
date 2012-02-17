package edu.brown.graphs;

import java.util.List;

import org.voltdb.catalog.CatalogType;

import edu.brown.utils.JSONSerializable;
import edu.uci.ics.jung.graph.Graph;

/**
 * 
 * @author pavlo
 *
 */
public interface IGraph<V extends AbstractVertex, E extends AbstractEdge> extends Graph<V, E>, Cloneable, JSONSerializable {
    public int getGraphId();
    public V getVertex(CatalogType catalog_item);
    public V getVertex(String catalog_key);
    public V getVertex(Long element_id);
    public void pruneIsolatedVertices();
    public String getName();
    public void setName(String name);
    public List<E> getPath(V source, V target);
    public List<E> getPath(List<V> path);
    
    /**
     * Enable the checks on whether the graph is dirty
     */
    public void enableDirtyChecks();
    
    /**
     * Enable verbose output for all elements of this graph
     * @param verbose
     */
    public void setVerbose(boolean verbose);
    
    public void setEdgeVerbose(boolean verbose);
    public void setVertexVerbose(boolean verbose);
    
    public String toString(E e, boolean verbose);
    public String debug();
}
