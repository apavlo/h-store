package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.graphs.VertexTreeWalker;

public class ConstraintPropagator {
    
    private final DesignerInfo info;
    private final AccessGraph agraph;
    private final Map<String, List<String>> orig_attributes;
    
    private final transient Set<Edge> marked_edges = new HashSet<Edge>();  
    
    public ConstraintPropagator(DesignerInfo info, AccessGraph agraph, Map<String, List<String>> attributes) {
        this.info = info;
        this.agraph = agraph;
        this.orig_attributes = attributes;
    }
    
    public void update(Table catalog_tbl) {
        // If this Table's partitioning column is set, then we can look at the AccessGraph
        // and remove any edges that don't use that column
        if (catalog_tbl.getIsreplicated() == false && catalog_tbl.getPartitioncolumn() != null) {
            Column catalog_col = catalog_tbl.getPartitioncolumn();
            assert(catalog_col != null);
            
            Vertex a_vertex = this.agraph.getVertex(catalog_tbl);
            assert(a_vertex != null);
            Collection<Edge> edges = this.agraph.findEdgeSet(a_vertex, catalog_col);
            assert(edges != null);
            
            for (Edge e : this.agraph.getIncidentEdges(a_vertex)) {
                if (edges.contains(e) == false) this.marked_edges.add(e);
            } // FOR
            
            for (Vertex other : this.agraph.getNeighbors(a_vertex)) {
                assert(other.equals(a_vertex) == false);
                
            } // FOR
        }
        
    }
    
    public void reset(Table catalog_tbl) {
        
    }
    
    public Collection<String> getDomain(Table catalog_tbl) {
        
        return (null);
    }

}
