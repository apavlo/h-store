package edu.brown.designer;

import org.voltdb.catalog.Database;

import edu.brown.graphs.AbstractDirectedGraph;

/**
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 */
public class DependencyGraph extends AbstractDirectedGraph<DesignerVertex, DesignerEdge> {
    private static final long serialVersionUID = 1L;

    public enum EdgeAttributes {
        COLUMNSET, CONSTRAINT,
    }

    public DependencyGraph(Database catalog_db) {
        super(catalog_db);
    }
}
