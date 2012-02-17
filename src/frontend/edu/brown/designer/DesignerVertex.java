package edu.brown.designer;

import org.voltdb.catalog.CatalogType;

import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.IGraph;

public class DesignerVertex extends AbstractVertex {

    public DesignerVertex() {
        super();
    }

    public DesignerVertex(CatalogType catalog_item) {
        super(catalog_item);
    }

    /**
     * Copy constructor
     * 
     * @param graph
     * @param copy
     */
    public DesignerVertex(IGraph<DesignerVertex, DesignerEdge> graph, AbstractVertex copy) {
        super(graph, copy);
    }
}
