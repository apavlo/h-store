package edu.brown.designer;

import org.voltdb.catalog.CatalogType;

import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.IGraph;

public class Vertex extends AbstractVertex {

    public Vertex() {
        super();
    }
    
    public Vertex(CatalogType catalog_item) {
        super(catalog_item);
    }
    
    /**
     * Copy constructor
     * @param graph
     * @param copy
     */
    public Vertex(IGraph<Vertex, Edge> graph, AbstractVertex copy) {
        super(graph, copy);
    }
}
