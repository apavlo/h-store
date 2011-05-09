package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.ColumnSet;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class ConstraintPropagator {
    private static final Logger LOG = Logger.getLogger(ConstraintPropagator.class);
    private static final boolean d = LOG.isDebugEnabled();
    private static final boolean t = LOG.isTraceEnabled();
    
    private final Database catalog_db;
    private final DesignerInfo info;
    private final DesignerHints hints;
    private final AccessGraph agraph;
    
    private final int num_tables;
    private final Map<CatalogType, Set<? extends CatalogType>> attributes = new HashMap<CatalogType, Set<? extends CatalogType>>();
    private final Map<Vertex, Map<Edge, Column>> vertex_edge_col = new HashMap<Vertex, Map<Edge, Column>>();
    
    private final Map<Table, Set<Column>> multicolumns = new HashMap<Table, Set<Column>>();
    private final Map<Procedure, Set<ProcParameter>> multiparams = new HashMap<Procedure, Set<ProcParameter>>();
    
    
    private final transient Set<CatalogType> isset = new HashSet<CatalogType>();
    private transient int isset_tables = 0;
    private transient int isset_procs = 0;
    private final transient Set<Edge> marked_edges = new HashSet<Edge>();
    
    public ConstraintPropagator(DesignerInfo info, DesignerHints hints, AccessGraph agraph) {
        this.catalog_db = info.catalog_db;
        this.info = info;
        this.hints = hints;
        this.agraph = agraph;
        
        // Pre-populate which column we can get back for a given vertex+edge
        Collection<Vertex> vertices = this.agraph.getVertices();
        for (Vertex v : vertices) {
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
        
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (AbstractPartitioner.shouldIgnoreProcedure(this.hints, catalog_proc) == false) {
                this.attributes.put(catalog_proc, new HashSet<ProcParameter>());
            }
        } // FOR
        
        // Generate the multi-attribute partitioning candidates
        if (hints.enable_multi_partitioning) {
            for (Procedure catalog_proc : info.catalog_db.getProcedures()) {
                if (AbstractPartitioner.shouldIgnoreProcedure(hints, catalog_proc)) continue;
                
                // MultiProcParameters
                multiparams.put(catalog_proc, new HashSet<ProcParameter>());
                for (Set<MultiProcParameter> mpps : AbstractPartitioner.generateMultiProcParameters(info, hints, catalog_proc).values()) {
                    multiparams.get(catalog_proc).addAll(mpps);
                } // FOR 
                
                // MultiColumns
                for (Entry<Table, Set<MultiColumn>> e : AbstractPartitioner.generateMultiColumns(info, hints, catalog_proc).entrySet()) {
                    Table catalog_tbl = e.getKey();
                    Set<Column> s = this.multicolumns.get(catalog_tbl);
                    if (s == null) {
                        s = new HashSet<Column>();
                        multicolumns.put(catalog_tbl, s);
                    }
                    s.addAll(e.getValue());
                } // FOR (entry)
            } // FOR (procedure)
        }
        
        
        this.num_tables = this.catalog_db.getTables().size();
    }
    
    public AccessGraph getAccessGraph() {
        return (this.agraph);
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
    public void update(CatalogType catalog_obj) {
        if (d) LOG.debug("Propagating constraints for " + catalog_obj);
        assert(this.isset.contains(catalog_obj) == false) : "Trying to update " + catalog_obj + " more than once!";
        
        // ----------------------------------------------
        // Table
        // ----------------------------------------------
        if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table)catalog_obj;
            
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
                
                if (t) LOG.trace(String.format("%s Edges:\nMATCHING EDGES\n%s\n===========\nALL EDGES\n%s",
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
            this.isset_tables++;
            
        // ----------------------------------------------
        // Procedure
        // ----------------------------------------------
        } else if (catalog_obj instanceof Procedure) {
            this.isset_procs++;
            
        // ----------------------------------------------
        // Busted
        // ----------------------------------------------
        } else {
            assert(false) : "Unexpected " + catalog_obj;
        }
        this.isset.add(catalog_obj);
        assert(this.isset.size() == (this.isset_tables + this.isset_procs));
    }
    
    /**
     * 
     * @param catalog_tbl
     */
    public void reset(CatalogType catalog_obj) {
        assert(this.isset.contains(catalog_obj)) : "Trying to reset " + catalog_obj + " before it's been marked as set!";
        if (d) LOG.debug("Reseting marked edges for " + catalog_obj);
        
        // ----------------------------------------------
        // Table
        // ----------------------------------------------
        if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table)catalog_obj;
            Vertex v = this.agraph.getVertex(catalog_tbl);
            if (v == null) throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
            Collection<Edge> edges = this.agraph.getIncidentEdges(v);
            this.marked_edges.removeAll(edges);
            this.isset_tables--;
            
        // ----------------------------------------------
        // Procedure
        // ----------------------------------------------
        } else if (catalog_obj instanceof Procedure) {
            this.isset_procs--;
            
        // ----------------------------------------------
        // Busted
        // ----------------------------------------------
        } else {
            assert(false) : "Unexpected " + catalog_obj;
        }
        this.isset.remove(catalog_obj);
        assert(this.isset.size() == (this.isset_tables + this.isset_procs));
    }
    
    /**
     * Get the possible values for the given Table
     * @param catalog_tbl
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends CatalogType> Collection<T> getPossibleValues(CatalogType catalog_obj, Class<T> return_class) {
        Collection<T> ret = null;
        
        // ----------------------------------------------
        // Table
        // ----------------------------------------------
        if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table)catalog_obj;
            Vertex v = this.agraph.getVertex(catalog_tbl);
            if (v == null) {
                throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
            }
            if (d) LOG.debug("Calculating possible values for " + catalog_tbl);
            
            Map<Edge, Column> edge_cols = this.vertex_edge_col.get(v);
            assert(edge_cols != null);
            ret = (Set<T>)this.attributes.get(catalog_tbl);
            ret.clear();
            for (Edge e : this.agraph.getIncidentEdges(v)) {
                if (this.marked_edges.contains(e)) continue;
                Column catalog_col = edge_cols.get(e);
                assert(catalog_col != null);
                ret.add((T)catalog_col);
            } // FOR
            
            // TODO: Prune out any MultiColumn where one of the elements isn't at least in our list
            if (hints.enable_multi_partitioning && this.multicolumns.containsKey(catalog_tbl)) {
                ret.addAll((Set<T>)this.multicolumns.get(catalog_tbl));
            }
            
        // ----------------------------------------------
        // Procedure
        // ----------------------------------------------
        } else if (catalog_obj instanceof Procedure) {
            Procedure catalog_proc = (Procedure)catalog_obj;
            List<String> attributes = null;
            try {
                attributes = AbstractPartitioner.generateProcParameterOrder(this.info, this.catalog_db, catalog_proc, this.hints);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            assert(attributes != null);
            ret = (Set<T>)this.attributes.get(catalog_proc);
            CatalogKey.getFromKeys(this.catalog_db, attributes, ProcParameter.class, (Set<ProcParameter>)ret);
            
            if (hints.enable_multi_partitioning && this.multiparams.containsKey(catalog_proc)) {
                ret.addAll((Set<T>)this.multiparams.get(catalog_proc));
            }
        // ----------------------------------------------
        // Busted (like your mom!)
        // ----------------------------------------------
        } else {
            assert(false) : "Unexpected " + catalog_obj;
        }
        assert(ret != null);
        if (d) LOG.debug(String.format("%s Possible Values [%d]: %s", catalog_obj, ret.size(), ret));
        return (ret);
    }

}
