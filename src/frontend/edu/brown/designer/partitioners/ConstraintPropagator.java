package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
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
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.StringUtil;

public class ConstraintPropagator {
    private static final Logger LOG = Logger.getLogger(ConstraintPropagator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static class Counter {
        private final int orig;
        private int ctr = 0;

        public Counter(int i) {
            this.ctr = i;
            this.orig = i;
        }

        public int incrementAndGet() {
            return (++ctr);
        }

        public int decrementAndGet() {
            return (--ctr);
        }

        public int get() {
            return (ctr);
        }

        public int getInitialValue() {
            return (orig);
        }
    }

    // --------------------------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // --------------------------------------------------------------------------------------------

    private final Database catalog_db;
    private final DesignerInfo info;
    private final DesignerHints hints;
    private final AccessGraph agraph;

    /**
     * These are the candidate sets that we will return through
     * getCandidateValues()
     */
    private final Map<CatalogType, Set<? extends CatalogType>> retvals = new HashMap<CatalogType, Set<? extends CatalogType>>();

    /**
     * For each DesignerEdge, maintain a mapping to the collection of Columns
     * that are referenced by that edge. As long as that edge is not marked,
     * then those Columns can be included in the candidate set for their table
     */
    private final Map<DesignerEdge, Collection<Column>> edge_cols_xref = new HashMap<DesignerEdge, Collection<Column>>();

    /**
     * Reverse index from Columns to the DesignerEdges where they are referenced
     */
    private final Map<Column, Collection<DesignerEdge>> column_edge_xref = new HashMap<Column, Collection<DesignerEdge>>();

    /**
     * For each Column, keep track of number of the edges where they are
     * referenced are marked When a Column's counter reaches zero, then that
     * Column is excluded in its Table's candidate set
     */
    private final Map<Column, Counter> column_edge_ctrs = new HashMap<Column, Counter>();

    private final Map<Table, ReplicatedColumn> repcolumns = new HashMap<Table, ReplicatedColumn>();
    private final Map<Column, Collection<MultiColumn>> multicolumns = new HashMap<Column, Collection<MultiColumn>>();
    private final Map<Procedure, Collection<ProcParameter>> multiparams = new HashMap<Procedure, Collection<ProcParameter>>();

    private final transient Set<CatalogType> isset = new HashSet<CatalogType>();
    private transient int isset_tables = 0;
    private transient int isset_procs = 0;
    private final transient Set<DesignerEdge> marked_edges = new HashSet<DesignerEdge>();

    // --------------------------------------------------------------------------------------------
    // INITIALIZATION
    // --------------------------------------------------------------------------------------------

    /**
     * Constructor
     * 
     * @param info
     * @param hints
     * @param agraph
     */
    public ConstraintPropagator(DesignerInfo info, DesignerHints hints, AccessGraph agraph) {
        this.catalog_db = info.catalogContext.database;
        this.info = info;
        this.hints = hints;
        this.agraph = agraph;

        this.init();
    }

    /**
     * Initialize internal data structures
     */
    private void init() {
        Random rng = new Random();

        // PROCEDURES
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc))
                continue;

            // CACHED RETURN VALUE SETS
            this.retvals.put(catalog_proc, new HashSet<ProcParameter>());

            // Generate the multi-attribute partitioning candidates
            if (hints.enable_multi_partitioning) {
                // MultiProcParameters
                this.multiparams.put(catalog_proc, new HashSet<ProcParameter>());
                for (Set<MultiProcParameter> mpps : PartitionerUtil.generateMultiProcParameters(info, hints, catalog_proc).values()) {
                    this.multiparams.get(catalog_proc).addAll(mpps);
                } // FOR

                // MultiColumns
                for (Entry<Table, Collection<MultiColumn>> e : PartitionerUtil.generateMultiColumns(info, hints, catalog_proc).entrySet()) {
                    for (MultiColumn mc : e.getValue()) {
                        for (Column catalog_col : mc.getAttributes()) {
                            Collection<MultiColumn> s = this.multicolumns.get(catalog_col);
                            if (s == null) {
                                this.multicolumns.put(catalog_col, CollectionUtil.addAll(new HashSet<MultiColumn>(), mc));
                            } else {
                                s.add(mc);
                            }
                        } // FOR
                    } // FOR
                } // FOR (entry)
            }
        } // FOR (procedure)

        // Pre-populate which column we can get back for a given vertex+edge
        Collection<DesignerVertex> vertices = this.agraph.getVertices();
        Map<Column, Collection<VerticalPartitionColumn>> col_vps = new HashMap<Column, Collection<VerticalPartitionColumn>>();
        for (DesignerVertex v : vertices) {
            Table catalog_tbl = v.getCatalogItem();
            if (this.retvals.containsKey(catalog_tbl) == false) {
                // CACHE RETURN VALUE SETS
                this.retvals.put(catalog_tbl, new HashSet<Column>());

                // REPLICATION
                if (this.hints.enable_replication_readmostly || this.hints.enable_replication_readonly) {
                    this.repcolumns.put(catalog_tbl, ReplicatedColumn.get(catalog_tbl));
                }
            }

            for (DesignerEdge e : this.agraph.getIncidentEdges(v)) {
                PredicatePairs cset = e.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET);
                assert (cset != null);
                Column catalog_col = CollectionUtil.first(cset.findAllForParent(Column.class, catalog_tbl));
                Collection<Column> candidates = new HashSet<Column>();

                if (catalog_col == null)
                    LOG.fatal("Failed to find column for " + catalog_tbl + " in ColumnSet:\n" + cset);

                if (catalog_col.getNullable()) {
                    if (debug.val)
                        LOG.warn("Ignoring nullable horizontal partition column candidate " + catalog_col.fullName());
                } else {
                    // Always add the base column without any vertical
                    // partitioning
                    candidates.add(catalog_col);

                    // Maintain a reverse index from Columns to DesignerEdges
                    Collection<DesignerEdge> col_edges = this.column_edge_xref.get(catalog_col);
                    if (col_edges == null) {
                        col_edges = new HashSet<DesignerEdge>();
                        this.column_edge_xref.put(catalog_col, col_edges);
                    }
                    col_edges.add(e);

                    // Pre-generate all of the vertical partitions that we will
                    // need during the search
                    if (hints.enable_vertical_partitioning) {
                        Collection<VerticalPartitionColumn> vp_candidates = col_vps.get(catalog_col);
                        if (vp_candidates == null) {
                            try {
                                vp_candidates = VerticalPartitionerUtil.generateCandidates(catalog_col, info.stats);
                                col_vps.put(catalog_col, vp_candidates);
                            } catch (Throwable ex) {
                                LOG.warn("Failed to generate vertical partition candidates for " + catalog_col.fullName(), ex);
                            }
                        }
                        if (vp_candidates != null)
                            candidates.addAll(vp_candidates);
                    }
                    // Add in the MultiColumns
                    if (hints.enable_multi_partitioning && this.multicolumns.containsKey(catalog_col)) {
                        candidates.addAll(this.multicolumns.get(catalog_col));
                    }

                }

                assert (catalog_col != null);
                List<Column> shuffled = new ArrayList<Column>(candidates);
                Collections.shuffle(shuffled, rng);
                this.edge_cols_xref.put(e, shuffled);
            } // FOR

        } // FOR

        // Initialized marked edge counters
        for (Collection<Column> cols : this.edge_cols_xref.values()) {
            for (Column catalog_col : cols) {
                Counter ctr = this.column_edge_ctrs.get(catalog_col);
                if (ctr == null) {
                    this.column_edge_ctrs.put(catalog_col, new Counter(1));
                } else {
                    ctr.incrementAndGet();
                }
            } // FOR
        } // FOR
    }

    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------

    protected Map<DesignerEdge, Collection<Column>> getEdgeColumns(DesignerVertex v) {
        Map<DesignerEdge, Collection<Column>> ret = new HashMap<DesignerEdge, Collection<Column>>();
        for (DesignerEdge e : this.agraph.getIncidentEdges(v)) {
            ret.put(e, this.edge_cols_xref.get(e));
        } // FOR
        return (ret);
    }

    protected Collection<VerticalPartitionColumn> getVerticalPartitionColumns(Table catalog_tbl) {
        Collection<VerticalPartitionColumn> vp_cols = new ListOrderedSet<VerticalPartitionColumn>();
        for (Collection<Column> cols : this.edge_cols_xref.values()) {
            for (Column catalog_col : cols) {
                if (catalog_col.getParent().equals(catalog_tbl) && catalog_col instanceof VerticalPartitionColumn) {
                    vp_cols.add((VerticalPartitionColumn) catalog_col);
                }
            } // FOR
        } // FOR
        return (vp_cols);
    }

    protected boolean isMarked(DesignerEdge e) {
        return (this.marked_edges.contains(e));
    }

    private void markEdge(DesignerEdge e) {
        assert (this.marked_edges.contains(e) == false) : "Trying to mark " + e + " more than once";
        this.marked_edges.add(e);

        // Keep track of the number of edges that have been marked per column
        // When the counter reaches zero, we know that we can exclude these
        // columns in
        // their tables' candidate values
        for (Column catalog_col : this.edge_cols_xref.get(e)) {
            int val = this.column_edge_ctrs.get(catalog_col).decrementAndGet();
            assert (val >= 0) : "Invalid counter for " + catalog_col.fullName() + ": " + val;
        } // FOR
        if (debug.val)
            LOG.debug(String.format("Marked edge %s and updated %d Column counters.", e.toStringPath(agraph), this.edge_cols_xref.get(e).size()));
    }

    private void unmarkEdge(DesignerEdge e) {
        assert (this.marked_edges.contains(e) == true) : "Trying to unmark " + e + " before it was marked";
        this.marked_edges.remove(e);

        // Keep track of the number of edges that have been marked per column
        // When the counter reaches zero, we know that we can exclude these
        // columns in
        // their tables' candidate values
        for (Column catalog_col : this.edge_cols_xref.get(e)) {
            this.column_edge_ctrs.get(catalog_col).incrementAndGet();
        } // FOR
        if (debug.val)
            LOG.debug(String.format("Unmarked edge %s and updated %d Column counters.", e.toStringPath(agraph), this.edge_cols_xref.get(e).size()));
    }

    // --------------------------------------------------------------------------------------------
    // SEARCH METHODS
    // --------------------------------------------------------------------------------------------

    /**
     * The partitioning column for this Table has changed, so we need to update
     * our internal data structures about which edges are still eligible for
     * selection
     * 
     * @param catalog_tbl
     */
    public void update(CatalogType catalog_obj) {
        if (debug.val)
            LOG.debug("Propagating constraints for " + catalog_obj);
        assert (this.isset.contains(catalog_obj) == false) : "Trying to update " + catalog_obj + " more than once!";

        // ----------------------------------------------
        // Table
        // ----------------------------------------------
        if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table) catalog_obj;

            // If this Table's partitioning column is set, then we can look at
            // the AccessGraph
            // and remove any edges that don't use that column
            if (catalog_tbl.getSystable() == false && catalog_tbl.getIsreplicated() == false && catalog_tbl.getPartitioncolumn() != null) {
                Column catalog_col = catalog_tbl.getPartitioncolumn();
                assert (catalog_col != null);
                if (catalog_col instanceof VerticalPartitionColumn) {
                    VerticalPartitionColumn vp_col = (VerticalPartitionColumn) catalog_col;
                    catalog_col = vp_col.getHorizontalColumn();
                    assert (catalog_col != null);
                }

                DesignerVertex v0 = this.agraph.getVertex(catalog_tbl);
                if (v0 == null)
                    throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
                Collection<DesignerEdge> edges = this.agraph.findEdgeSet(v0, catalog_col);
                assert (edges != null);
                Collection<DesignerEdge> all_edges = this.agraph.getIncidentEdges(v0);
                assert (all_edges != null);

                if (trace.val)
                    LOG.trace(String.format("%s Edges:\nMATCHING EDGES\n%s" + StringUtil.SINGLE_LINE + "ALL EDGES\n%s", catalog_tbl.fullName(), StringUtil.prefix(StringUtil.join("\n", edges), "   "),
                            StringUtil.prefix(StringUtil.join("\n", all_edges), "   ")));

                // Look at v0's edges and mark any that are not in our list
                int e_ctr = 0;
                for (DesignerEdge e : all_edges) {
                    if (trace.val)
                        LOG.trace(String.format("Checking whether we can mark edge %s", e.toStringPath(agraph)));
                    if (edges.contains(e) == false && this.marked_edges.contains(e) == false) {
                        this.markEdge(e);
                        e_ctr++;
                    }
                } // FOR
                if (debug.val)
                    LOG.debug(String.format("Marked %d out of %d edges for update of %s", e_ctr, all_edges.size(), catalog_tbl));
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
            assert (false) : "Unexpected " + catalog_obj;
        }
        this.isset.add(catalog_obj);
        if (debug.val)
            LOG.debug("Current IsSet Items: " + this.isset);
        assert (this.isset.size() == (this.isset_tables + this.isset_procs));
    }

    /**
     * @param catalog_tbl
     */
    public void reset(CatalogType catalog_obj) {
        assert (this.isset.contains(catalog_obj)) : "Trying to reset " + catalog_obj + " before it's been marked as set!";
        if (debug.val)
            LOG.debug("Reseting marked edges for " + catalog_obj);

        // ----------------------------------------------
        // Table
        // ----------------------------------------------
        if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table) catalog_obj;
            DesignerVertex v = this.agraph.getVertex(catalog_tbl);
            if (v == null)
                throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
            for (DesignerEdge e : this.agraph.getIncidentEdges(v)) {
                if (this.isMarked(e))
                    this.unmarkEdge(e);
            } // FOR
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
            assert (false) : "Unexpected " + catalog_obj;
        }
        this.isset.remove(catalog_obj);
        if (debug.val)
            LOG.debug("Current IsSet Items: " + this.isset);
        assert (this.isset.size() == (this.isset_tables + this.isset_procs));
    }

    /**
     * Get the possible values for the given Table
     * 
     * @param catalog_tbl
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends CatalogType> Collection<T> getCandidateValues(CatalogType catalog_obj, Class<T> return_class) {
        Collection<T> ret = (Set<T>) this.retvals.get(catalog_obj);
        if (ret == null) {
            throw new IllegalArgumentException("Unexpected item " + catalog_obj);
        }
        ret.clear();
        if (debug.val)
            LOG.debug("Retrieving candidate values for " + catalog_obj);

        // ----------------------------------------------
        // Table
        // ----------------------------------------------
        if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table) catalog_obj;
            int total_cols = 0;
            DesignerVertex v = this.agraph.getVertex(catalog_tbl);
            if (v == null) {
                throw new IllegalArgumentException("Missing vertex for " + catalog_tbl);
            }

            Column repcol = this.repcolumns.get(catalog_tbl);
            if (repcol != null) {
                ret.add((T) repcol);
                total_cols++;
            }

            Collection<DesignerEdge> edges = this.agraph.getIncidentEdges(v);
            for (DesignerEdge e : edges) {
                if (this.marked_edges.contains(e))
                    continue;
                Collection<Column> columns = this.edge_cols_xref.get(e);
                if (columns == null)
                    continue;

                if (trace.val && columns.isEmpty() == false) {
                    LOG.trace(String.format("Examining %d candidate Columns for %s", columns.size(), e.toStringPath(agraph)));
                }
                for (Column catalog_col : columns) {
                    // Make sure that we only add the Columns for our table
                    if (catalog_col.getParent().equals(catalog_tbl) && this.column_edge_ctrs.get(catalog_col).get() > 0) {
                        ret.add((T) catalog_col);
                        if (trace.val)
                            LOG.trace(String.format("Adding candidate Column %s because of Edge %s", catalog_col.fullName(), e.toStringPath(agraph)));
                    }
                } // FOR (columns)
                total_cols += columns.size();
            } // FOR (edges)
            if (debug.val) {
                LOG.debug(String.format("Selected %d out of %d candidate Columns using %d edges for %s", ret.size(), total_cols, edges.size(), catalog_tbl));
            }

            // ----------------------------------------------
            // Procedure
            // ----------------------------------------------
        } else if (catalog_obj instanceof Procedure) {
            Procedure catalog_proc = (Procedure) catalog_obj;
            Set<String> proc_attributes = null;
            try {
                proc_attributes = PartitionerUtil.generateProcParameterOrder(this.info, this.catalog_db, catalog_proc, this.hints);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            assert (proc_attributes != null);
            CatalogKey.getFromKeys(this.catalog_db, proc_attributes, ProcParameter.class, (Set<ProcParameter>) ret);
            if (hints.enable_multi_partitioning && this.multiparams.containsKey(catalog_proc)) {
                ret.addAll((Set<T>) this.multiparams.get(catalog_proc));
            }
            // ----------------------------------------------
            // Busted (like your mom!)
            // ----------------------------------------------
        } else {
            assert (false) : "Unexpected " + catalog_obj;
        }
        if (debug.val)
            LOG.debug(String.format("%s Possible Values [%d]: %s", catalog_obj, ret.size(), CatalogUtil.debug(ret)));
        return (ret);
    }

    // --------------------------------------------------------------------------------------------
    // DEBUG
    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        List<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
        Map<String, Object> m = null;

        m = new ListOrderedMap<String, Object>();
        m.put("Base Objects", "");
        for (CatalogType catalog_item : this.retvals.keySet()) {
            Map<String, Object> inner = new ListOrderedMap<String, Object>();
            inner.put("Isset", this.isset.contains(catalog_item));
            inner.put("RetVals", StringUtil.join("\n", this.retvals.get(catalog_item)));
            m.put(catalog_item.fullName(), inner);
        } // FOR
        maps.add(m);

        m = new ListOrderedMap<String, Object>();
        m.put("Columns", "");
        for (Column catalog_col : this.column_edge_xref.keySet()) {
            Counter ctr = this.column_edge_ctrs.get(catalog_col);
            Collection<MultiColumn> mcs = this.multicolumns.get(catalog_col);
            Collection<DesignerEdge> edges = this.column_edge_xref.get(catalog_col);

            Map<String, Object> inner = new ListOrderedMap<String, Object>();
            inner.put("Counter", String.format("Current=%d / Initial=%d", ctr.get(), ctr.getInitialValue()));
            if (mcs != null)
                inner.put(String.format("MultiColumns [%d]", mcs.size()), StringUtil.join("\n", mcs));
            inner.put(String.format("DesignerEdges [%d]", edges.size()), StringUtil.join("\n", edges));
            m.put(catalog_col.fullName(), inner);
        } // FOR
        maps.add(m);

        m = new ListOrderedMap<String, Object>();
        m.put("Edges", "");
        for (DesignerEdge e : this.edge_cols_xref.keySet()) {
            Collection<Column> cols = this.edge_cols_xref.get(e);

            Map<String, Object> inner = new ListOrderedMap<String, Object>();
            inner.put(String.format("Columns [%d]", cols.size()), StringUtil.join("\n", cols));
            m.put(e.toStringPath(this.agraph), inner);
        } // FOR
        maps.add(m);

        return StringUtil.formatMaps(maps.toArray(new Map<?, ?>[0]));
    }
}
