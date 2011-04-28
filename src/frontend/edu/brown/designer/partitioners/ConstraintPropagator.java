package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.designer.AccessGraph;
import edu.brown.designer.ColumnSet;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class ConstraintPropagator {
    private static final Logger LOG = Logger.getLogger(ConstraintPropagator.class);
    private static final boolean d = LOG.isDebugEnabled();
    private static final boolean t = LOG.isTraceEnabled();
    
    private final AccessGraph agraph;
    private final Map<CatalogType, Set<? extends CatalogType>> attributes = new HashMap<CatalogType, Set<? extends CatalogType>>();
    private final Map<Vertex, Map<Edge, Column>> vertex_edge_col = new HashMap<Vertex, Map<Edge, Column>>();
    
    private final transient Set<CatalogType> isset = new HashSet<CatalogType>(); 
    private final transient Set<Edge> marked_edges = new HashSet<Edge>();
    
    public ConstraintPropagator(AccessGraph agraph) {
        this.agraph = agraph;
        
        // Pre-populate which column we can get back for a given vertex+edge
        for (Vertex v : this.agraph.getVertices()) {
            Table catalog_tbl = v.getCatalogItem();
            if (this.attributes.containsKey(catalog_tbl) == false) {
                this.attributes.put(catalog_tbl, new HashSet<Column>());
            }
            
            Map<Edge, Column> m = new HashMap<Edge, Column>();
            for (Edge e : this.agraph.getIncidentEdges(v)) {
                ColumnSet cset = e.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET);
                assert(cset != null);
                Column catalog_col = CollectionUtil.getFirst(cset.findAllForParent(Column.class, catalog_tbl));
                if (catalog_col == null) LOG.fatal("Failed to find column for " + catalog_tbl + " in ColumnSet:\n" + cset);
                assert(catalog_col != null);
                m.put(e, catalog_col);
            } // FOR
            this.vertex_edge_col.put(v, m);
        } // FOR
    }
    
    protected Map<Edge, Column> getEdgeColumns(Vertex v) {
        return (this.vertex_edge_col.get(v));
    }
    
    protected boolean isMarked(Edge e) {
        return (this.marked_edges.contains(e));
    }

    /**
     * The partitioning column for this Table has changed, so we need to update
     * our internal data structures about which edges are still eligible for selection
     * @param catalog_tbl
     */
    public void update(Table catalog_tbl) {
        if (d) LOG.debug("Propagating constraints for " + catalog_tbl);
        assert(this.isset.contains(catalog_tbl) == false) : "Trying to update " + catalog_tbl + " more than once!";
        
        // If this Table's partitioning column is set, then we can look at the AccessGraph
        // and remove any edges that don't use that column
        if (catalog_tbl.getIsreplicated() == false && catalog_tbl.getPartitioncolumn() != null) {
            Column catalog_col = catalog_tbl.getPartitioncolumn();
            assert(catalog_col != null);
            
            Vertex v0 = this.agraph.getVertex(catalog_tbl);
            if (v0 == null) throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
            Collection<Edge> edges = this.agraph.findEdgeSet(v0, catalog_col);
            assert(edges != null);
            Collection<Edge> all_edges = this.agraph.getIncidentEdges(v0);
            assert(all_edges != null);
            
            if (d) LOG.debug(String.format("%s Edges:\nMATCHING EDGES\n%s\n===========\nALL EDGES\n%s",
                             catalog_tbl.getName(), StringUtil.join("\n", edges), StringUtil.join("\n", all_edges)));
            
            // Look at v0's edges and mark any that are not in our list 
            for (Edge e : all_edges) {
                if (t) LOG.trace(String.format("Checking whether we can mark edge [%s]", StringUtil.join("--", this.agraph.getIncidentVertices(e))));
                if (edges.contains(e) == false && this.marked_edges.contains(e) == false) {
                    Vertex v1 = this.agraph.getOpposite(v0, e);
                    assert(v1 != null);
                    this.marked_edges.add(e);
                    if (t) LOG.trace("Marked edge " + e.toString(true) + " as ineligible for " + v1.getCatalogItem());
                }
            } // FOR
        }
        this.isset.add(catalog_tbl);
    }
    
    /**
     * 
     * @param catalog_tbl
     */
    public void reset(Table catalog_tbl) {
        assert(this.isset.contains(catalog_tbl)) : "Trying to reset " + catalog_tbl + " before it's been marked as set!";
        if (d) LOG.debug("Reseting marked edges for " + catalog_tbl);
        Vertex v = this.agraph.getVertex(catalog_tbl);
        if (v == null) throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
        Collection<Edge> edges = this.agraph.getIncidentEdges(v);
        this.marked_edges.removeAll(edges);
        this.isset.remove(catalog_tbl);
    }
    
    /**
     * 
     * @param catalog_proc
     * @return
     */
    public Collection<ProcParameter> getPossibleValues(Procedure catalog_proc) {
        
        return (null);
    }
    
    /**
     * Get the possible values for the given Table
     * @param catalog_tbl
     * @return
     */
    @SuppressWarnings("unchecked")
    public Collection<Column> getPossibleValues(Table catalog_tbl) {
        Vertex v = this.agraph.getVertex(catalog_tbl);
        if (v == null) throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
        if (d) LOG.debug("Calculating possible values for " + catalog_tbl);
        
        Map<Edge, Column> edge_cols = this.vertex_edge_col.get(v);
        assert(edge_cols != null);
        Set<Column> cols = (Set<Column>)this.attributes.get(catalog_tbl);
        cols.clear();
        for (Edge e : this.agraph.getIncidentEdges(v)) {
            if (this.marked_edges.contains(e)) continue;
            Column catalog_col = edge_cols.get(e);
            assert(catalog_col != null);
            cols.add(catalog_col);
        } // FOR
        return (cols);
    }

}
