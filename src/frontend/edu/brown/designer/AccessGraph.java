package edu.brown.designer;

import java.util.*;

import org.voltdb.catalog.*;

import edu.brown.graphs.AbstractUndirectedGraph;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
@SuppressWarnings("serial")
public class AccessGraph extends AbstractUndirectedGraph<Vertex, Edge>  {
    
    public enum EdgeAttributes {
        ACCESSTYPE,
        COLUMNSET,
        FOREIGNKEY,
    };
    public enum AccessType {
        SCAN,
        SQL_JOIN,
        PARAM_JOIN,
        IMPLICIT_JOIN,
        INSERT;
        
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
     * 
     * @param catalog_db
     */
    public AccessGraph(Database catalog_db) {
        super(catalog_db);
    }
    
    /**
     * Return all the edges for the given vertex whose ColumnSets contain the given Column
     * @param v
     * @param catalog_col
     * @return
     */
    public Collection<Edge> findEdgeSet(Vertex v, Column catalog_col) {
        assert(v != null);
        Set<Edge> edges = new HashSet<Edge>();
        for (Edge e : this.getIncidentEdges(v)) {
            ColumnSet cset = e.getAttribute(EdgeAttributes.COLUMNSET);
            assert(cset != null);
            if (cset.findAll(catalog_col).isEmpty() == false) {
                edges.add(e);
            }
        } // FOR
        return (edges);
    }
    
    public <T> Collection<Edge> getIncidentEdges(Vertex vertex, String name, T value) {
        Set<Edge> edges = new HashSet<Edge>();
        for (Edge edge : super.getIncidentEdges(vertex)) {
            if (edge.getAttribute(name).equals(value)) edges.add(edge);
        } // WHILE
        return (edges);
    }
    
    @Override
    public String toString(Edge e, boolean verbose) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString(e, verbose));
        
        if (verbose) {
//            for (Entry<String, Object> entry : e.getAttributeValues(this).entrySet()) {
//                sb.append(String.format("\n => %-15s%s", entry.getKey()+":", entry.getValue().toString()));
//            }
            ColumnSet cset = e.getAttribute(EdgeAttributes.COLUMNSET.name());
            assert(cset != null);
            sb.append("\n").append(cset.debug());
        }
        return (sb.toString());
    }
}