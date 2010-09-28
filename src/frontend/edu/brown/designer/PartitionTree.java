package edu.brown.designer;

import org.voltdb.catalog.*;
import org.voltdb.types.PartitionMethodType;

import edu.brown.graphs.AbstractDirectedTree;

import java.util.*;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class PartitionTree extends AbstractDirectedTree<Vertex, Edge> {
    private static final long serialVersionUID = -7005176921187161150L;

    public enum EdgeAttributes {
        CONSTRAINT,
        WEIGHT,
    }
    public enum VertexAttributes {
        REMOVED,
        ATTRIBUTE,
        PARENT_ATTRIBUTE,
        METHOD,
    }
    
    private Double weight = 0.0;
    private final Set<Procedure> procedures = new HashSet<Procedure>();
    
    public PartitionTree(Database catalog_db) {
        super(catalog_db);
    }
    
    public Edge createEdge(Vertex parent, Vertex child, Edge orig_edge) {
        Edge new_edge = new Edge(this, orig_edge);
        this.addEdge(new_edge, parent, child);
        this.addVertex(child);
        return (new_edge);
    }
    
    public Set<Procedure> getProcedures() {
        return this.procedures;
    }
    
    public Double getWeight() {
        return this.weight;
    }
    
    public void setWeight(Double weight) {
        this.weight = weight;
    }
    
    public boolean isReplicated(Vertex vertex) {
        assert(vertex != null);
        if (vertex.hasAttribute(this, VertexAttributes.METHOD.name())) {
            return (vertex.getAttribute(this, VertexAttributes.METHOD.name()).equals(PartitionMethodType.REPLICATION));
        }
        return (false);
    }
}
