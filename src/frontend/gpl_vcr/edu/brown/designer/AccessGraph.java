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
        WEIGHT,         // DOUBLE
        ACCESSTYPE,
        COLUMNSET,
        FOREIGNKEY,
    };
    public enum AccessType {
        SCAN,
        SQL_JOIN,
        PARAM_JOIN,
        IMPLICIT_JOIN;
        
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
    
    public <T> Collection<Edge> getIncidentEdges(Vertex vertex, String name, T value) {
        Set<Edge> edges = new HashSet<Edge>();
        for (Edge edge : super.getIncidentEdges(vertex)) {
            if (edge.getAttribute(name).equals(value)) edges.add(edge);
        } // WHILE
        return (edges);
    }
}