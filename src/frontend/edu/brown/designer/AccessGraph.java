package edu.brown.designer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;

import edu.brown.graphs.AbstractUndirectedGraph;
import edu.brown.utils.PredicatePairs;

/**
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 */
@SuppressWarnings("serial")
public class AccessGraph extends AbstractUndirectedGraph<DesignerVertex, DesignerEdge> {

    public enum EdgeAttributes {
        ACCESSTYPE, COLUMNSET, FOREIGNKEY,
    };

    public enum AccessType {
        SCAN, SQL_JOIN, PARAM_JOIN, IMPLICIT_JOIN, INSERT;

        public static List<AccessType> JOINS = new ArrayList<AccessType>();
        static {
            JOINS.add(SQL_JOIN);
            JOINS.add(PARAM_JOIN);
            JOINS.add(IMPLICIT_JOIN);
        }
    };

    //
    // Edge Weight Modifiers
    //
    public static final Double WEIGHT_FOREIGN_KEY = 5.0;
    public static final Double WEIGHT_JOIN = 2.5;
    public static final Double WEIGHT_IMPLICIT = 4.0;

    /**
     * @param catalog_db
     */
    public AccessGraph(Database catalog_db) {
        super(catalog_db);
    }

    /**
     * Return all the edges for the given vertex whose ColumnSets contain the
     * given Column
     * 
     * @param v
     * @param catalog_col
     * @return
     */
    public Collection<DesignerEdge> findEdgeSet(DesignerVertex v, Column catalog_col) {
        assert (v != null);
        Set<DesignerEdge> edges = new HashSet<DesignerEdge>();
        for (DesignerEdge e : this.getIncidentEdges(v)) {
            PredicatePairs cset = e.getAttribute(EdgeAttributes.COLUMNSET);
            assert (cset != null);
            if (cset.findAll(catalog_col).isEmpty() == false) {
                edges.add(e);
            }
        } // FOR
        return (edges);
    }

    public <T> Collection<DesignerEdge> getIncidentEdges(DesignerVertex vertex, String name, T value) {
        Set<DesignerEdge> edges = new HashSet<DesignerEdge>();
        for (DesignerEdge edge : super.getIncidentEdges(vertex)) {
            if (edge.getAttribute(name).equals(value))
                edges.add(edge);
        } // WHILE
        return (edges);
    }

    @Override
    public String toString(DesignerEdge e, boolean verbose) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString(e, verbose));

        if (verbose) {
            // for (Entry<String, Object> entry :
            // e.getAttributeValues(this).entrySet()) {
            // sb.append(String.format("\n => %-15s%s", entry.getKey()+":",
            // entry.getValue().toString()));
            // }
            PredicatePairs cset = e.getAttribute(EdgeAttributes.COLUMNSET.name());
            assert (cset != null);
            sb.append("\n").append(cset.debug());
        }
        return (sb.toString());
    }
}