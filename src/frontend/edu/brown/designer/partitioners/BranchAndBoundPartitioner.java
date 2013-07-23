/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.costmodel.AbstractCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.MemoryEstimator;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.StringUtil;
import edu.brown.workload.filters.Filter;

/**
 * @author pavlo
 */
public class BranchAndBoundPartitioner extends AbstractPartitioner {
    public static final Logger LOG = Logger.getLogger(BranchAndBoundPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // --------------------------------------------------------------------------------------------
    // STATIC DATA MEMBERS
    // --------------------------------------------------------------------------------------------

    /**
     * Debug Output Spacers
     */
    private static final Map<Integer, String> TRAVERSAL_SPACERS = new HashMap<Integer, String>();
    static {
        // Debug Spacer
        String spacer = "";
        for (int i = 0; i < 100; i++) {
            spacer += "  "; // StringUtil.SPACER;
            TRAVERSAL_SPACERS.put(i, spacer);
        } // FOR
    } // STATIC

    // --------------------------------------------------------------------------------------------
    // INTERNAL STATE
    // --------------------------------------------------------------------------------------------

    /**
     * 
     */
    protected static class StateVertex {
        private static final String START_VERTEX_NAME = "*START*";
        private static final String UPPERBOUND_NAME = "*UPPERBOUND*";

        private final int depth;
        private final StateVertex parent;
        private final boolean is_table;
        private final String catalog_key;
        private final String partition_key;
        private final Double cost;
        private final Double singlep_txns;
        private final Long memory;

        private String debug;

        /**
         * Full Constructor
         * 
         * @param parent
         * @param catalog_key
         * @param partition_key
         * @param cost
         * @param memory
         */
        private StateVertex(StateVertex parent, int depth, String catalog_key, String partition_key, boolean is_table, Double cost, Double singlep_txns, Long memory) {
            this.parent = parent;
            this.depth = depth;
            this.catalog_key = catalog_key;
            this.partition_key = partition_key;
            this.is_table = is_table;
            this.cost = cost;
            this.singlep_txns = singlep_txns;
            this.memory = memory;
        }

        public StateVertex(StateVertex parent, String catalog_key, String partition_key, boolean is_table, Double cost, Double singlep_txns, Long memory) {
            this(parent, parent.depth + 1, catalog_key, partition_key, is_table, cost, singlep_txns, memory);
        }

        /**
         * Generate a new starting placeholder vertex
         * 
         * @return
         */
        public static StateVertex getStartVertex(Double cost, Long memory) {
            return (new StateVertex(null, 0, START_VERTEX_NAME, null, false, cost, null, memory));
        }

        /**
         * Generate a new upper-bound placeholder vertex
         * 
         * @param cost
         * @param memory
         * @return
         */
        public static StateVertex getUpperBoundVertex(Double cost, Long memory) {
            return (new StateVertex(null, 0, UPPERBOUND_NAME, null, false, cost, null, memory));
        }

        public int getDepth() {
            return (this.depth);
        }

        public Double getCost() {
            return cost;
        }

        public Long getMemory() {
            return memory;
        }

        public boolean isStartVertex() {
            return (this.catalog_key.equals(START_VERTEX_NAME));
        }

        public boolean isUpperBoundVertex() {
            return (this.catalog_key.equals(UPPERBOUND_NAME));
        }

        public String getCatalogKey() {
            return (this.catalog_key);
        }

        public String getPartitionKey() {
            return (this.partition_key);
        }

        /**
         * Construct the mapping from CatalogKey -> PartitionKey We do this so
         * don't have to copy the map at every single level in the tree
         * 
         * @return
         */
        public Map<String, String> getCatalogKeyMap() {
            return (this.buildCatalogKeyMap(new LinkedHashMap<String, String>()));
        }

        private Map<String, String> buildCatalogKeyMap(Map<String, String> map) {
            if (this.parent != null)
                this.parent.buildCatalogKeyMap(map);
            if (this.depth > 0)
                map.put(this.catalog_key, this.partition_key);
            return (map);
        }

        public Map<CatalogType, CatalogType> getCatalogMap(Database catalog_db) {
            Map<CatalogType, CatalogType> m = new LinkedHashMap<CatalogType, CatalogType>();

            // // First insert all of the entries from the catalog
            // for (Table catalog_tbl : catalog_db.getTables()) {
            // m.put(catalog_tbl, catalog_tbl.getPartitioncolumn());
            // } // FOR
            // for (Procedure catalog_proc : catalog_db.getProcedures()) {
            // if (catalog_proc.getSystemproc()) continue;
            // m.put(catalog_proc, CatalogUtil.getProcParameter(catalog_proc));
            // } // FOR

            // Then overwrite those entries with what's in our vertex tree
            return (this.buildCatalogMap(catalog_db, m));
        }

        private Map<CatalogType, CatalogType> buildCatalogMap(Database catalog_db, Map<CatalogType, CatalogType> map) {
            if (this.parent != null)
                this.parent.buildCatalogMap(catalog_db, map);
            if (this.depth > 0) {
                CatalogType key = null;
                CatalogType val = null;

                if (this.is_table) {
                    key = CatalogKey.getFromKey(catalog_db, this.catalog_key, Table.class);
                    val = CatalogKey.getFromKey(catalog_db, this.partition_key, Column.class);
                } else {
                    key = CatalogKey.getFromKey(catalog_db, this.catalog_key, Procedure.class);
                    val = CatalogKey.getFromKey(catalog_db, this.partition_key, ProcParameter.class);
                }
                map.put(key, val);
            }
            return (map);
        }

        @Override
        public String toString() {
            Map<String, Object> m0 = new LinkedHashMap<String, Object>();
            m0.put(String.format("StateVertex[%s]", this.catalog_key), null);
            m0.put("Cost", this.cost);
            m0.put("Memory", this.memory);

            Map<String, Object> m1 = new LinkedHashMap<String, Object>();
            for (Entry<String, String> e : this.getCatalogKeyMap().entrySet()) {
                m1.put(CatalogKey.getNameFromKey(e.getKey()), CatalogKey.getNameFromKey(e.getValue()));
            }

            return (StringUtil.formatMaps(m0, m1));
        }
    } // END CLASS

    // --------------------------------------------------------------------------------------------
    // DATA MEMBERS
    // --------------------------------------------------------------------------------------------

    protected StateVertex best_vertex = null;
    protected StateVertex upper_bounds_vertex = null;
    protected PartitionPlan upper_bounds_pplan = null;
    protected final Map<CatalogType, WorkloadFilter> traversal_filters = new HashMap<CatalogType, WorkloadFilter>();
    protected TraverseThread thread = null;

    protected List<Table> table_visit_order = new ArrayList<Table>();
    protected List<Procedure> proc_visit_order = new ArrayList<Procedure>();
    protected AccessGraph agraph = null;

    // --------------------------------------------------------------------------------------------
    // CONSTRUCTORS
    // --------------------------------------------------------------------------------------------

    public BranchAndBoundPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    /**
     * @param stats_catalog_db
     * @param workload
     */
    public BranchAndBoundPartitioner(Designer designer, DesignerInfo info, AccessGraph agraph, List<Table> table_visit_order, List<Procedure> proc_visit_order) {
        super(designer, info);
        this.setParameters(agraph, table_visit_order, proc_visit_order);
    }

    // --------------------------------------------------------------------------------------------
    // MEMBER UTILITY METHODS
    // --------------------------------------------------------------------------------------------

    public AccessGraph getAcessGraph() {
        return this.agraph;
    }

    public void setParameters(AccessGraph agraph, List<Table> table_visit_order, List<Procedure> proc_visit_order) {
        this.agraph = agraph;
        this.table_visit_order.addAll(table_visit_order);
        this.proc_visit_order.addAll(proc_visit_order);
    }

    public void setUpperBounds(final DesignerHints hints, PartitionPlan pplan, double cost, long memory) throws Exception {
        this.upper_bounds_pplan = pplan;
        this.upper_bounds_pplan.apply(this.info.catalogContext.database);
        this.upper_bounds_vertex = StateVertex.getUpperBoundVertex(cost, memory);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("UpperBounds Cost Estimate:   %.02f", this.upper_bounds_vertex.cost));
            LOG.debug(String.format("UpperBounds Memory Estimate: %.02f", (this.upper_bounds_vertex.cost / (double) hints.max_memory_per_partition)));
        }
    }

    public StateVertex getUpperBounds() {
        return (this.upper_bounds_vertex);
    }

    // public void setTraversalAttributes(Map<String, List<String>> attributes,
    // int num_tables) {
    // this.base_traversal_attributes.clear();
    // this.base_traversal_attributes.putAll(attributes);
    // this.num_tables = num_tables;
    // }

    /**
     * Returns true if the last search halt because of a limit Returns false if
     * the last search completed because it ran out of attributes to investigate
     * If the search hasn't been run yet, returns null
     * 
     * @return
     */
    public Boolean wasHalted() {
        if (this.thread != null) {
            return (this.thread.halt_search);
        }
        return (null);
    }

    /**
     * Returns the last reason for why the TraverseThread halted its search
     * 
     * @return
     */
    public HaltReason getLastHaltReason() {
        if (this.thread != null)
            return (this.thread.halt_reason);
        return (null);
    }

    public Long getLastBackTrackCount() {
        if (this.thread != null)
            return (this.thread.backtrack_ctr);
        return (null);
    }

    /**
     * Return the best StateVertex selected during the search process
     * 
     * @return
     */
    protected StateVertex getBestVertex() {
        return (this.best_vertex);
    }

    protected PartitionPlan createPartitionPlan(DesignerHints hints, StateVertex vertex, boolean validate, boolean useCatalog) {
        Map<String, String> catalogkey_map = vertex.getCatalogKeyMap();
        if (validate) {
            assert (catalogkey_map.size() == (this.table_visit_order.size() + this.proc_visit_order.size())) : String.format("Solution has %d vertices. Should have %d tables and %d procedures!",
                    catalogkey_map.size(), this.table_visit_order.size(), this.proc_visit_order.size());
        }
        // System.out.println("MEMORY: " + this.best_vertex.memory);

        Map<CatalogType, CatalogType> pplan_map = new HashMap<CatalogType, CatalogType>();

        // Apply the changes in the best catalog to the original
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            String table_key = CatalogKey.createKey(catalog_tbl);
            String column_key = catalogkey_map.get(table_key);
            if (column_key != null) {
                Column catalog_col = CatalogKey.getFromKey(info.catalogContext.database, column_key, Column.class);
                pplan_map.put(catalog_tbl, catalog_col);
            } else if (useCatalog) {
                pplan_map.put(catalog_tbl, catalog_tbl.getPartitioncolumn());
            }
        } // FOR

        if (hints.enable_procparameter_search) {
            for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
                if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc))
                    continue;
                String proc_key = CatalogKey.createKey(catalog_proc);
                String param_key = catalogkey_map.get(proc_key);
                if (param_key != null) {
                    ProcParameter catalog_proc_param = CatalogKey.getFromKey(info.catalogContext.database, param_key, ProcParameter.class);
                    pplan_map.put(catalog_proc, catalog_proc_param);
                } else if (useCatalog) {
                    pplan_map.put(catalog_proc, CatalogUtil.getPartitioningProcParameter(catalog_proc));
                }
            } // FOR
        }
        PartitionPlan pplan = PartitionPlan.createFromMap(pplan_map);
        return (pplan);
    }

    // --------------------------------------------------------------------------------------------
    // INITIALIZATION
    // --------------------------------------------------------------------------------------------

    /**
     * @param hints
     * @throws Exception
     */
    protected void init(final DesignerHints hints) throws Exception {
        if (this.agraph == null) {
            this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(info.catalogContext.database, this.generateAccessGraph());
            this.table_visit_order.addAll(PartitionerUtil.generateTableOrder(info, agraph, hints));
            for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
                if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc) == false) {
                    this.proc_visit_order.add(catalog_proc);
                }
            } // FOR
        }
        assert (this.agraph != null) : "Missing global AccessGraph";
        assert (this.table_visit_order.isEmpty() == false) : "Missing Table Visit Order";
        if (hints.enable_procparameter_search)
            assert (this.proc_visit_order.isEmpty() == false) : "Missing Procedure Visit Order";

        if (debug.val)
            LOG.debug("Initializing partitioner for new generate run");

        // (1) We first need to construct an AccessGraph that represents the
        // entire workload for all procedures
        // final AccessGraph agraph =
        // AccessGraphGenerator.convertToSingleColumnEdges(info.catalogContext.database,
        // this.generateAccessGraph());

        // List<String> table_visit_order = null;
        // List<String> proc_visit_order = null;
        // boolean need_attributes = this.base_traversal_attributes.isEmpty();
        // if (need_attributes) {
        // table_visit_order = AbstractPartitioner.generateTableOrder(this.info,
        // agraph, hints);
        // //GraphVisualizationPanel.createFrame(agraph).setVisible(true);
        // proc_visit_order = this.generateProcedureOrder(info.catalogContext.database,
        // hints);
        // } else {
        // assert(this.num_tables < this.base_traversal_attributes.size());
        // table_visit_order = new ArrayList<String>();
        // proc_visit_order = new ArrayList<String>();
        // for (int i = 0, cnt = this.base_traversal_attributes.size(); i < cnt;
        // i++) {
        // if (i < this.num_tables) {
        // table_visit_order.add(this.base_traversal_attributes.get(i));
        // } else {
        // proc_visit_order.add(this.base_traversal_attributes.get(i));
        // }
        // } // FOR
        // }

        // (2) Then get the ordered list of tables that we will visit during the
        // search
        // if (debug.val) LOG.debug("Table Visit Order: " +
        // table_visit_order);

        // (3) Now for each table we're going to visit, come up with a ordered
        // list of how
        // we will visit the columns of each table. We also will generate
        // filters so that
        // we can quickly walk through the workload and only evaluate queries
        // that access the
        // tables for our level in the search tree.
        if (debug.val)
            LOG.debug("Creating table specific data structures for the " + table_visit_order.size() + " traversal levels");
        List<Table> filter_tables = new ArrayList<Table>();

        // IMPORTANT: Add in any table that is not in the attributes list
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            if (!table_visit_order.contains(catalog_tbl)) {
                filter_tables.add(catalog_tbl);
            }
        } // FOR
        if (debug.val)
            LOG.debug("Tables to never filter: " + CatalogUtil.debug(filter_tables));
        // Now construct all of the workload filters for this level of the
        // traversal
        for (Table catalog_tbl : table_visit_order) {
            filter_tables.add(catalog_tbl);
            this.traversal_filters.put(catalog_tbl, new WorkloadFilter(info.catalogContext.database, filter_tables));
        } // FOR

        // (4) Lastly, we need to add the list Procedures that we are going to
        // need to select
        // ProcParameters for. We don't actually want to populate the entries at
        // this point
        // because we will be able to throw out of a lot of the candidates once
        // we know
        // what the tables are partitioned on using our ParameterCorrelations
        // object
        // assert(proc_visit_order.size() ==
        // info.workload.getProcedureHistogram().getValueCount()) :
        // "Missing Procedures from visit order list:\n" +
        // "Expected[" + info.workload.getProcedureHistogram().getValueCount() +
        // "]: " + info.workload.getProcedureHistogram().values() + "\n" +
        // "Got Back[" + proc_visit_order.size() + "]: " + proc_visit_order;
        // if (debug.val) LOG.debug("Procedure Visit Order: " +
        // proc_visit_order);
        // if (hints.enable_procparameter_search) {
        // for (String proc_key : proc_visit_order) {
        // assert(proc_key != null);
        // assert(proc_key.isEmpty() == false);
        // Procedure catalog_proc = CatalogKey.getFromKey(info.catalogContext.database,
        // proc_key, Procedure.class);
        //
        // // Just put in an empty list for now -- WHY???
        // if (need_attributes) this.base_traversal_attributes.put(proc_key, new
        // LinkedList<String>());
        //
        // // Important: Don't put in any workload filters here because we want
        // to estimate the cost of
        // // all procedures (even though we may not have selected a
        // ProcParameter yet) This is necessary
        // // to ensure that our cost estimates never decrease as we traverse
        // the search tree
        // this.traversal_filters.put(catalog_proc, null);
        // } // FOR
        // }

        // Pure genius! Then use the PrimaryKeyPartitioner to calculate an
        // upper-bounds
        if (this.upper_bounds_pplan == null) {
            if (debug.val)
                LOG.debug("Calculating upper bounds...");
            PartitionPlan pplan = new MostPopularPartitioner(this.designer, this.info).generate(hints);

            // Cost Estimate
            AbstractCostModel cost_model = info.getCostModel();
            assert (cost_model != null);
            cost_model.applyDesignerHints(hints);
            cost_model.clear();
            double cost = cost_model.estimateWorkloadCost(this.info.catalogContext, this.info.workload);

            // Memory Estimate
            long memory = this.info.getMemoryEstimator().estimate(this.info.catalogContext.database, info.getNumPartitions());
            assert (memory <= hints.max_memory_per_partition) : memory + " <= " + hints.max_memory_per_partition + "\n" + pplan;
            this.setUpperBounds(hints, pplan, cost, memory);
        }

        // We first need a starting node as the root of the graph
        this.best_vertex = StateVertex.getStartVertex(this.upper_bounds_vertex.getCost(), this.upper_bounds_vertex.getMemory());
    }

    // --------------------------------------------------------------------------------------------
    // SEARCH
    // --------------------------------------------------------------------------------------------

    /**
     * 
     */
    @Override
    public PartitionPlan generate(final DesignerHints hints) throws Exception {
        hints.startGlobalSearchTimer();

        // Initialize our various data structures
        assert (this.info.getCostModel() != null);
        this.info.getCostModel().applyDesignerHints(hints);
        this.init(hints);

        if (debug.val) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Exhaustive Search", hints.exhaustive_search);
            m.put("Greedy Search", hints.greedy_search);
            m.put("Local Search Time", hints.limit_local_time);
            m.put("Back Track Limit", hints.limit_back_tracks);
            m.put("Upper Bounds Cost", this.upper_bounds_vertex.getCost());
            m.put("Upper Bounds Memory", this.upper_bounds_vertex.getMemory());
            m.put("Number of Partitions", CatalogUtil.getNumberOfPartitions(info.catalogContext.database));
            m.put("Memory per Partition", hints.max_memory_per_partition);
            m.put("Cost Model Weights", "[execution=" + info.getCostModel().getExecutionWeight() + ", entropy=" + info.getCostModel().getEntropyWeight() + "]");
            if (trace.val)
                m.put("Procedure Histogram:", info.workload.getProcedureHistogram());
            LOG.debug("Branch-and-Bound Status:\n" + StringUtil.formatMaps(m));
        }

        this.thread = new TraverseThread(info, hints, this.best_vertex, this.agraph, this.table_visit_order, this.proc_visit_order);
        thread.run(); // BLOCK
        this.halt_reason = this.thread.halt_reason;

        PartitionPlan pplan = null;

        // If the best solution is still the start vertex, then we know that it
        // was the upper bound
        // limit was the best solution, so we can just use that. What a waste of
        // a search!
        if (this.best_vertex.isStartVertex()) {
            if (debug.val)
                LOG.debug("No new solution was found. Using upper bounds");
            assert (this.upper_bounds_vertex.isUpperBoundVertex());
            assert (this.upper_bounds_pplan != null);
            assert (this.best_vertex.getCost().equals(this.upper_bounds_vertex.getCost())) : this.best_vertex.getCost() + " == " + this.upper_bounds_vertex.getCost();
            pplan = this.upper_bounds_pplan;

            // Otherwise reconstruct the PartitionPlan from the StateVertex
        } else {
            pplan = this.createPartitionPlan(hints, this.best_vertex, true, true);
            this.setProcedureSinglePartitionFlags(pplan, hints);
        }
        // Make sure that we actually completed the search and didn't just abort
        if (!thread.completed_search) {
            LOG.error("Failed to complete search successfully:\n" + pplan);
            assert (false);
        }
        pplan.apply(info.catalogContext.database); // Why?
        return (pplan);
    }

    /**
     * 
     */
    protected class TraverseThread extends Thread {

        private final AccessGraph agraph;
        private final ConstraintPropagator cp;
        private final DesignerInfo info;
        private final DesignerHints hints;

        private final List<Table> search_tables;
        private final List<Procedure> search_procs;
        private final List<CatalogType> all_search_elements = new ArrayList<CatalogType>();
        private final List<String> all_search_keys = new ArrayList<String>();
        private final Set<Table> previous_tables[];

        private final Set<Table> current_vertical_partitions = new HashSet<Table>();

        /**
         * Total number of catalog elements that we're going to search against
         * (tables+procedures)
         */
        private final int num_elements;
        /** Total number of search tables */
        private final int num_tables;

        private final String start_name;
        private StateVertex start;
        private final MemoryEstimator memory_estimator;
        private AbstractCostModel cost_model;
        private long traverse_ctr = 0;
        private long backtrack_ctr = 0;

        private Long halt_time;
        private boolean halt_time_local;
        private boolean halt_search = false;
        private HaltReason halt_reason = null;
        private boolean completed_search = false;

        /**
         * Constructor
         * 
         * @param info
         * @param start
         * @param traversal_attributes
         * @param children
         */
        @SuppressWarnings("unchecked")
        public TraverseThread(DesignerInfo info, DesignerHints hints, StateVertex start, AccessGraph agraph, List<Table> search_tables, List<Procedure> search_procs) {
            this.info = info;
            this.hints = hints;
            this.start = start;
            this.start_name = CatalogKey.getNameFromKey(start.getCatalogKey());
            this.search_tables = search_tables;
            this.search_procs = search_procs;

            this.all_search_elements.addAll(this.search_tables);
            this.all_search_elements.addAll(this.search_procs);
            this.all_search_keys.addAll(CatalogKey.createKeys(this.all_search_elements));

            this.num_tables = this.search_tables.size();
            this.num_elements = this.all_search_elements.size();
            this.previous_tables = (Set<Table>[]) (new Set<?>[this.num_elements]);

            // Create a ConstraintPropagator that will keep track of which
            // attributes we can use per database object
            this.agraph = agraph;
            this.cp = new ConstraintPropagator(this.info, this.hints, this.agraph);

            // Memory Estimator
            this.memory_estimator = info.getMemoryEstimator();

            // Cost Model
            this.cost_model = info.getCostModel();
            assert (this.cost_model != null);
            this.cost_model.applyDesignerHints(hints);

            try {
                this.initializeTables();
                this.initializeProcedures();
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to initialize BranchAndBound traverser!", ex);
            }
        }

        /**
         * Initialize internal data structures for tables
         */
        private void initializeTables() {
            // These tables aren't in our attributes but we still need to use
            // them for estimating size
            final Set<Table> remaining_tables = new HashSet<Table>();

            Map<Table, MaterializedViewInfo> vps = CatalogUtil.getVerticallyPartitionedTables(info.catalogContext.database);

            // Mark all of our tables that we're not going to search against as
            // updated in our ConstraintPropagator
            if (trace.val)
                LOG.trace("All Tables: " + CatalogUtil.getDisplayNames(info.catalogContext.database.getTables()));
            for (Table catalog_tbl : info.catalogContext.getDataTables()) {
                assert (catalog_tbl.getSystable() == false);
                if (this.search_tables.contains(catalog_tbl) == false) {
                    if (trace.val)
                        LOG.trace("Updating ConstraintPropagator for fixed " + catalog_tbl);
                    try {
                        this.cp.update(catalog_tbl);
                    } catch (IllegalArgumentException ex) {
                        // IGNORE
                    }
                    remaining_tables.add(catalog_tbl);
                }
                // Include the vertical partition for any table that is not in
                // our search neighborhood
                else if (hints.enable_vertical_partitioning && vps.containsKey(catalog_tbl) && catalog_tbl.getPartitioncolumn() instanceof VerticalPartitionColumn) {
                    MaterializedViewInfo catalog_view = vps.get(catalog_tbl);
                    assert (catalog_view != null);
                    assert (catalog_view.getDest() != null);
                    remaining_tables.add(catalog_view.getDest());
                }
            } // FOR
            if (debug.val)
                LOG.debug(String.format("Tables: %d Searchable / %d Fixed", this.search_tables.size(), remaining_tables.size()));

            // HACK: If we are searching against all possible tables, then just
            // completely clear out the cost model
            if ((remaining_tables.isEmpty()) && hints.enable_costmodel_caching) {
                if (debug.val)
                    LOG.debug("Searching against all tables. Completely resetting cost model.");
                this.cost_model.clear(true);
            }

            // For each level in the search tree, compute the sets of tables
            // that have been bound
            // This is needed for when we calculate the total size of a solution
            for (int i = 0; i < this.num_elements; i++) {
                this.previous_tables[i] = new HashSet<Table>(remaining_tables);
                if (i < this.num_tables)
                    remaining_tables.add(this.search_tables.get(i));
            } // FOR
        }

        /**
         * Initialize internal data structures for procedures
         */
        private void initializeProcedures() {
            int proc_ctr = 0;
            for (Procedure catalog_proc : this.info.catalogContext.database.getProcedures()) {
                if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc))
                    continue;
                proc_ctr++;

                // Unset all Procedure partitioning parameters
                if (this.search_procs.contains(catalog_proc)) {
                    catalog_proc.setPartitionparameter(NullProcParameter.PARAM_IDX);
                } else {
                    if (trace.val)
                        LOG.trace("Updating ConstraintPropagator for fixed " + catalog_proc);
                    this.cp.update(catalog_proc);
                }
            } // FOR
            if (debug.val)
                LOG.debug(String.format("Procedures: %d Searchable / %d Fixed", this.search_procs.size(), (proc_ctr - this.search_procs.size())));

        }

        @Override
        public String toString() {
            return this.start_name + "-" + super.toString();
        }

        public void halt() {
            this.halt_search = true;
        }

        @Override
        public void run() {
            Pair<TimestampType, Boolean> p = hints.getNextStopTime();
            if (p != null) {
                this.halt_time = p.getFirst().getMSTime();
                this.halt_time_local = p.getSecond();
            }
            if (debug.val) {
                if (this.halt_time != null)
                    LOG.debug("Remaining Search Time: " + (this.halt_time - System.currentTimeMillis()) / 1000d);
                LOG.debug(String.format("Starting Search for %d Elements: %s", this.all_search_elements.size(), this.all_search_elements));
                LOG.debug("Current Best Solution: " + best_vertex.cost);
            }
            if (hints.target_plan != null) {
                LOG.info("Searching for target PartitionPlan '" + hints.target_plan_path + "'");
            }

            ProfileMeasurement timer = new ProfileMeasurement("timer").start();
            try {
                this.traverse(start, 0);
            } catch (Exception ex) {
                LOG.error("Failed to execute search", ex);
                throw new RuntimeException(ex);
            } finally {
                timer.stop();
                if (this.halt_reason == null)
                    this.halt_reason = HaltReason.EXHAUSTED_SEARCH;
                this.completed_search = true;
            }
            LOG.info(String.format("Search Halted - %s [%.2f sec]", this.halt_reason, timer.getTotalThinkTimeSeconds()));
        }

        /**
         * @param parent
         * @param idx
         * @throws Exception
         */
        protected void traverse(final StateVertex parent, final int idx) throws Exception {
            assert (idx < this.num_elements && idx >= 0);
            final CatalogType current = this.all_search_elements.get(idx);
            assert (current != null);
            final String current_key = this.all_search_keys.get(idx);

            this.traverse_ctr++;
            final boolean is_table = (idx < this.num_tables);
            // Are we the last element in the list, thereby creating a complete
            // solution
            final boolean complete_solution = (idx + 1 == this.num_elements);
            if (is_table)
                assert (current instanceof Table);
            final String spacer = BranchAndBoundPartitioner.TRAVERSAL_SPACERS.get(idx);

            if (this.halt_search == false) {
                assert (this.halt_reason == null);
                if (hints.limit_back_tracks != null && hints.limit_back_tracks >= 0 && this.backtrack_ctr > hints.limit_back_tracks) {
                    LOG.info("Hit back track limit. Halting search [" + this.backtrack_ctr + "]");
                    this.halt_search = true;
                    this.halt_reason = HaltReason.BACKTRACK_LIMIT;
                    return;
                } else if (this.halt_time != null && System.currentTimeMillis() >= this.halt_time) {
                    LOG.info("Hit time limit. Halting search [" + this.backtrack_ctr + "]");
                    this.halt_search = true;
                    this.halt_reason = (this.halt_time_local ? HaltReason.LOCAL_TIME_LIMIT : HaltReason.GLOBAL_TIME_LIMIT);
                    return;
                }
            } else
                return;

            // Get the list of possible attributes that we could use for this
            // current element
            Collection<CatalogType> current_attributes = null;
            try {
                current_attributes = this.cp.getCandidateValues(current, CatalogType.class);
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException("Failed to get candidate attributes for " + current, ex);
            }
            final int num_attributes = current_attributes.size();
            assert (current_attributes != null);
            if (trace.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                m.put("Traversal Level", traverse_ctr);
                m.put("Item Index", idx);
                m.put("Current", current);
                m.put("Attributes", String.format("COUNT=%d\n%s", num_attributes, StringUtil.join("\n", CatalogUtil.getDisplayNames(current_attributes))));
                LOG.trace(StringUtil.formatMaps(m));
            }

            // Keep track of whether we have a VerticalPartitionColumn that
            // needs to be
            // reset after each round
            VerticalPartitionColumn vp_col = null;

            // Get our workload filter for this level of the traversal
            Filter filter = BranchAndBoundPartitioner.this.traversal_filters.get(current);

            // Descendant tables used for memory calculations
            // It's ok for it to be empty. That means we're searching against
            // all of tables
            Set<Table> current_previousTables = this.previous_tables[idx];
            assert (current_previousTables != null) : String.format("No previous tables at index %d? [current=%s, num_tables=%d]", idx, current, num_tables);

            // The local best vertex is the best vertex for this level in the
            // traversal
            StateVertex local_best_vertex = null;

            // Just make sure that if we have a VerticalPartitionColumn in our
            // list of
            // current attributes that it's optimizations are not applied
            // If they are, then we can disable them
            for (CatalogType attribute : current_attributes) {
                if (attribute instanceof VerticalPartitionColumn && ((VerticalPartitionColumn) attribute).isUpdateApplied()) {
                    ((VerticalPartitionColumn) attribute).revertUpdate();
                }
            } // FOR

            // Iterate through the columns and find the one with the best cost
            int attribute_ctr = 0;
            for (CatalogType attribute : current_attributes) {
                assert (attribute != null) : "Null attribute key for " + current + ": " + current_attributes;
                String attribute_key = CatalogKey.createKey(attribute);
                if (trace.val)
                    LOG.trace("Evaluating " + attribute.fullName());
                boolean memory_exceeded = false;
                Long memory = null;
                CatalogType current_attribute = null;

                // Is this the last element we have to look at
                boolean last_attribute = (++attribute_ctr == num_attributes);

                // Dynamic Debugging
                this.cost_model.setDebuggingEnabled(this.hints.isDebuggingEnabled(attribute_key));

                // IMPORTANT: We have to invalidate the cache for the current
                // element *AND* all those levels
                // below us in the search tree!
                if (this.cost_model.isCachingEnabled()) {
                    for (int i = idx; i < this.num_elements; i++) {
                        if (trace.val)
                            LOG.trace("Invalidating " + this.all_search_keys.get(i));
                        this.cost_model.invalidateCache(this.all_search_keys.get(i));
                    } // FOR
                    // If we're not using caching, then just clear out the cost
                    // model completely
                } else {
                    this.cost_model.clear();
                }

                // ----------------------------------------------
                // TABLE PARTITIONING KEY
                // ----------------------------------------------
                if (is_table) {
                    Table current_tbl = (Table) current;
                    Column search_col = (Column) attribute;
                    Column current_col = null;

                    // Check whether this is our replication marker column
                    if (search_col instanceof ReplicatedColumn) {
                        current_tbl.setIsreplicated(true);
                        current_col = ReplicatedColumn.get(current_tbl);
                    }
                    // VerticalPartitionColumn
                    else if (search_col instanceof VerticalPartitionColumn) {
                        // We need to update the statements that can use them
                        // using the pre-compiled query plans
                        current_tbl.setIsreplicated(false);
                        current_col = search_col;
                        vp_col = (VerticalPartitionColumn) search_col;
                        assert (CatalogUtil.getDatabase(vp_col).equals(info.catalogContext.database)) : String.format("VP_COL[%d] != INFO[%d]", CatalogUtil.getDatabase(vp_col).hashCode(),
                                info.catalogContext.database.hashCode());

                        MaterializedViewInfo catalog_view = vp_col.applyUpdate();
                        assert (catalog_view != null) : "Unexpected null MaterializedViewInfo for " + current_tbl + " vertical partition:\n" + vp_col;
                        if (this.cost_model.isCachingEnabled()) {
                            if (trace.val)
                                LOG.trace("Invalidating VerticalPartition Statements in cost model: " + vp_col.getOptimizedQueries());
                            this.cost_model.invalidateCache(vp_col.getOptimizedQueries());
                        }
                        TableStatistics tstats = VerticalPartitionerUtil.computeTableStatistics(vp_col, info.stats);
                        assert (tstats != null);
                        // Add the vp's sys table to the list of tables that we
                        // need to estimate the memory
                        assert (catalog_view.getDest() != null) : "Missing parent table for " + catalog_view.fullName();
                        assert (this.current_vertical_partitions.contains(catalog_view.getDest()) == false) : vp_col;
                        this.current_vertical_partitions.add(catalog_view.getDest());
                    }
                    // MultiColumn
                    else if (search_col instanceof MultiColumn) {
                        // Nothing special?
                        current_tbl.setIsreplicated(false);
                        current_col = search_col;
                    }
                    // Otherwise partition on this particular column
                    else {
                        current_tbl.setIsreplicated(false);
                        current_col = current_tbl.getColumns().get(search_col.getName());
                    }

                    // We should always have a horizontal partition column
                    assert (current_col != null);
                    current_tbl.setPartitioncolumn(current_col);
                    assert (current_col.getName().equals(current_tbl.getPartitioncolumn().getName())) : "Unexpected " + current_col.fullName() + " != " + current_tbl.getPartitioncolumn().fullName();

                    // Estimate memory size
                    Collection<Table> tablesToEstimate = null;
                    if (hints.enable_vertical_partitioning && this.current_vertical_partitions.isEmpty() == false) {
                        tablesToEstimate = CollectionUtils.union(current_previousTables, this.current_vertical_partitions);
                    } else {
                        tablesToEstimate = current_previousTables;
                    }
                    if (trace.val)
                        LOG.trace(String.format("Calculating memory size of current solution [%s]:\n%s", current_col.fullName(), StringUtil.join("\n", current_previousTables)));
                    try {
                        memory = this.memory_estimator.estimate(info.catalogContext.database, info.getNumPartitions(), tablesToEstimate);
                    } catch (Throwable ex) {
                        throw new RuntimeException("Failed to estimate memory using new attribute " + current_col.fullName(), ex);
                    }
                    memory_exceeded = (memory > this.hints.max_memory_per_partition);
                    if (trace.val)
                        LOG.trace(String.format("%s Memory: %s [ratio=%.2f, exceeded=%s]", current_col.fullName(), StringUtil.formatSize(memory), memory / (double) hints.max_memory_per_partition,
                                memory_exceeded));
                    current_attribute = current_col;

                    // ----------------------------------------------
                    // PROCEDURE PARTITIONING PARAMETER
                    // ----------------------------------------------
                } else {
                    Procedure current_proc = (Procedure) current;
                    ProcParameter current_proc_param = (ProcParameter) attribute;
                    assert (current_proc_param != null);
                    current_proc.setPartitionparameter(current_proc_param.getIndex());
                    memory = parent.memory;
                    current_attribute = current_proc_param;
                }

                // ----------------------------------------------
                // COST ESTIMATION
                // ----------------------------------------------
                Double cost = null;
                Double singlep_txns = null;
                // Don't estimate the cost if it doesn't fit
                if (!memory_exceeded) {
                    cost = this.cost_model.estimateWorkloadCost(info.catalogContext, info.workload, filter, best_vertex.cost);
                    singlep_txns = this.cost_model.getSinglePartitionProcedureHistogram().getSampleCount() / (double) this.cost_model.getProcedureHistogram().getSampleCount();
                } else {
                    cost = Double.MAX_VALUE;
                }

                StateVertex state = new StateVertex(parent, current_key, attribute_key, is_table, cost, singlep_txns, memory);
                if (debug.val)
                    state.debug = cost_model.debugHistograms(info.catalogContext);
                // if (!this.graph.containsVertex(parent))
                // this.graph.addVertex(parent);
                // this.graph.addEdge(new StateEdge(), parent, state,
                // EdgeType.DIRECTED);

                if (local_best_vertex == null || local_best_vertex.cost > state.cost)
                    local_best_vertex = state;
                assert (local_best_vertex != null);

                // ----------------------------------------------
                // DEBUG OUTPUT
                // ----------------------------------------------
                // state.debug = this.cost_model.getLastDebugMessage();
                LOG.info(this.createLevelOutput(state, CatalogUtil.getDisplayName(current_attribute, false), spacer, memory_exceeded));

                // ----------------------------------------------
                // ANALYSIS
                // ----------------------------------------------
                if (memory_exceeded) {
                    if (debug.val)
                        LOG.debug("Memory exceeeded! Skipping solution!");
                    if (vp_col != null) {
                        this.revertVerticalPartitionColumn(vp_col);
                        vp_col = null;
                    }
                    continue;
                }

                // The cost of our parent can never be greater than our cost
                if (!parent.isStartVertex()) {
                    boolean is_valid = MathUtil.greaterThanEquals(cost.floatValue(), parent.cost.floatValue(), 0.001f);

                    if (!is_valid) { // && debug.get()) {
                        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
                        m0.put("Parent State", parent.toString());
                        m0.put("Parent CostModel", parent.debug);
                        m0.put("Parent Catalog", createPartitionPlan(hints, parent, false, false));

                        Map<String, Object> m1 = new LinkedHashMap<String, Object>();
                        m1.put("Current Attribute", current_attribute.fullName());
                        m1.put("Current State", state.toString());
                        m1.put("Current CostModel", cost_model.debugHistograms(info.catalogContext));
                        m1.put("Current Catalog", createPartitionPlan(hints, state, false, false));

                        LOG.error("CURRENT COST IS GREATER THAN CURRENT COST! THIS CANNOT HAPPEN!\n" + StringUtil.formatMaps(m0, m1));

                        cost_model.clear();
                        double cost2 = this.cost_model.estimateWorkloadCost(info.catalogContext, info.workload, filter, best_vertex.cost);
                        LOG.error("Second Time: " + cost2);

                    }
                    assert (is_valid) : attribute_key + ": Parent[" + parent.cost + "] <= Current[" + cost + "]" + "\n" + "Rounded: Parent[" + MathUtil.roundToDecimals(parent.cost, 2)
                            + "] <= Current[" + MathUtil.roundToDecimals(cost, 2) + "]";
                }

                // If this is a complete solution, then check whether if it is
                // the best one that we have seen thus far
                // Allow the current solution to be the best solution if the
                // following criteria are met:
                // (1) It's a complete solution
                // (2) It has not exhausted our per-partition memory limit
                // (4) It is less than the upper bounds limit
                // (3) And one of the following is true:
                // (a) The current best solution is the start vertex
                // (b) Or the current solution has a cost less than the current
                // best solution
                if (complete_solution && memory_exceeded == false && cost < BranchAndBoundPartitioner.this.upper_bounds_vertex.cost
                        && (BranchAndBoundPartitioner.this.best_vertex.isStartVertex() || cost < BranchAndBoundPartitioner.this.best_vertex.cost)) {
                    assert (best_vertex.cost > state.cost) : "Best=" + best_vertex.cost + ", Current=" + state.cost;
                    assert (upper_bounds_vertex.cost > state.cost) : "Upper=" + upper_bounds_vertex.cost + ", Current=" + state.cost;

                    if (debug.val) {
                        LOG.debug("Old Solution:\n" + StringBoxUtil.box(best_vertex.toString()));
                    }
                    BranchAndBoundPartitioner.this.best_vertex = state;
                    if (debug.val) {
                        LOG.debug("New Best Solution:\n" + StringBoxUtil.box(best_vertex.toString()));
                        if (this.cost_model.hasDebugMessages())
                            LOG.debug("Last Cost Model Info:\n " + this.cost_model.getLastDebugMessage());
                    }

                    // Log new solution cost
                    if (hints.shouldLogSolutionCosts())
                        hints.logSolutionCost(state.cost, state.singlep_txns);

                    // Check whether we found our target solution and need to
                    // stop
                    // Note that we only need to compare Tables, because the
                    // Procedure's could have
                    // different parameters for those ones where the parameter
                    // actually doesn't make a difference
                    if (hints.target_plan != null) {
                        if (debug.val)
                            LOG.info("Comparing new best solution with target PartitionPlan");
                        PartitionPlan new_plan = createPartitionPlan(hints, best_vertex, false, false);
                        if (hints.target_plan.getTableEntries().equals(new_plan.getTableEntries())) {
                            this.halt_search = true;
                            this.halt_reason = HaltReason.FOUND_TARGET;
                        }
                    }

                    // for (int i = 0; i <
                    // ((TimeIntervalCostModel)this.cost_model).getIntevalCount();
                    // i++) {
                    // System.err.println("Interval #" + i);
                    // System.err.println(((TimeIntervalCostModel)this.cost_model).getCostModel(i).getTxnPartitionAccessHistogram());
                    // System.err.println("================================================");
                    // }
                    //
                    // System.exit(1);
                }

                // ----------------------------------------------
                // CONTINUE SEARCH TRAVERSAL
                // ----------------------------------------------

                // We now need to look to see whether we should continue down
                // this path. Much like
                // selecting a new best solution, the following criteria must be
                // met in order for
                // the partitioner to be allowed to continue down a search path:
                // (1) If this is last attribute at this level in the tree
                // AND we're looking at a TABLE
                // AND we're doing a greedy search
                // Then we'll always want to keep traversing here.
                // Otherwise, the following the criteria must be met:
                // (1) It must not be a complete solution
                // (2) The current catalog item must be a table (no procedures!)
                // (3) The cost must be less than the current best solution cost
                // (4) The cost must be less than the upper bounds limit
                // Or we can just say screw all that and keep going if the
                // exhaustive flag is enabled
                if (this.halt_search == false
                        && ((last_attribute && is_table && this.hints.greedy_search) || (this.hints.exhaustive_search == true) || (complete_solution == false && is_table
                                && cost < BranchAndBoundPartitioner.this.best_vertex.cost && cost < BranchAndBoundPartitioner.this.upper_bounds_vertex.cost))) {

                    // IMPORTANT: If this is the last table in our traversal,
                    // then we need to switch over
                    // to working Procedures->ProcParameters. We have to now
                    // calculate the list of attributes
                    // that we want to consider
                    // if (is_last_table &&
                    // this.hints.enable_procparameter_search) {
                    // for (int i = idx + 1; i < this.num_elements; i++) {
                    // Procedure catalog_proc =
                    // (Procedure)this.all_search_elements.get(i);
                    // LinkedList<String> attributes =
                    // AbstractPartitioner.generateProcParameterOrder(info,
                    // this.info.catalogContext.database, catalog_proc, hints);
                    //
                    // // We should have ProcParameter candidates!
                    // assert(attributes.isEmpty() == false) :
                    // "No ProcParameter candidates: " + catalog_proc + "\n" +
                    // state;
                    // this.traversal_attributes.get(proc_key).clear();
                    // this.traversal_attributes.get(proc_key).addAll(attributes);
                    // } // FOR
                    // }

                    // We only traverse if this is a table. The ProcParameter
                    // selection is a simple greedy algorithm
                    if (this.hints.greedy_search == false || (this.hints.greedy_search == true && last_attribute)) {
                        if (debug.val && this.hints.greedy_search)
                            LOG.debug(this.createLevelOutput(local_best_vertex, "GREEDY->" + local_best_vertex.getPartitionKey(), spacer, false));
                        this.cp.update(current);
                        this.traverse((this.hints.greedy_search ? local_best_vertex : state), idx + 1);
                        this.cp.reset(current);
                    }
                }
                if (vp_col != null) {
                    this.revertVerticalPartitionColumn(vp_col);
                    vp_col = null;
                }
                if (this.halt_search)
                    break;
            } // FOR

            // ProcParameter Traversal
            if (!is_table && !this.halt_search) {
                assert (local_best_vertex != null) : "Missing local best vertex for " + current_key;

                // Set the partitioning ProcParameter in this Procedure to be
                // the one that
                // had the lowest cost in our search up above.
                Procedure current_proc = (Procedure) current;
                String best_key = local_best_vertex.getPartitionKey();
                ProcParameter current_param = CatalogKey.getFromKey(info.catalogContext.database, best_key, ProcParameter.class);
                assert (current_param != null);
                current_proc.setPartitionparameter(current_param.getIndex());

                // Is this necessary?
                this.cost_model.invalidateCache(current_proc);

                // if (debug.val)
                // LOG.debug(this.createLevelOutput(local_best_vertex,
                // CatalogUtil.getDisplayName(current_param), spacer, false));

                // If there are more Procedures to partition and we have gone
                // past our best cost
                // our upper bounds, then keep going...
                if (complete_solution == false && hints.enable_procparameter_search && (this.hints.greedy_search == true)
                        || (local_best_vertex.cost < best_vertex.cost && local_best_vertex.cost < upper_bounds_vertex.cost)) {
                    this.cp.update(current_proc);
                    this.traverse(local_best_vertex, idx + 1);
                    this.cp.reset(current_proc);
                }

                // Important: We will have to unset the Procedure partitioning
                // parameter
                // if this was the last table and we are going back up the tree.
                // This is because
                // unlike Tables, where we exclude queries using
                // WorkloadFilters, we can't do
                // the same thing for ProcParameters
                // manually once we finish up with this table because the next
                // passes will
                // overwrite the catalog that we saved off
                for (int i = idx; i < this.num_elements; i++) {
                    Procedure catalog_proc = (Procedure) this.all_search_elements.get(i);
                    catalog_proc.setPartitionparameter(-1);
                } // FOR
            }
            if (this.halt_search == false)
                this.backtrack_ctr++;
            return;
        }

        private void revertVerticalPartitionColumn(VerticalPartitionColumn vp_col) {
            // Reset the catalog and optimized queries in the cost model
            vp_col.revertUpdate();
            if (cost_model.isCachingEnabled()) {
                if (debug.val)
                    LOG.debug("Invalidating VerticalPartition Statements in cost model: " + vp_col.getOptimizedQueries());
                this.cost_model.invalidateCache(vp_col.getOptimizedQueries());
            }

            // Make sure we remove the sys table from the list of tables
            // that we need to estimate the memory from
            MaterializedViewInfo catalog_view = vp_col.getViewCatalog();
            assert (catalog_view != null) : vp_col;
            assert (catalog_view.getDest() != null) : vp_col;
            assert (this.current_vertical_partitions.contains(catalog_view.getDest())) : vp_col;
            this.current_vertical_partitions.remove(catalog_view.getDest());
        }

        private StringBuilder createLevelOutput(StateVertex state, String label, String spacer, boolean memory_exceeded) {
            final StringBuilder debug = new StringBuilder();
            if (!BranchAndBoundPartitioner.this.best_vertex.isStartVertex()) {
                debug.append(String.format("[%.02f] ", BranchAndBoundPartitioner.this.best_vertex.cost));
            } else {
                debug.append("[----] ");
            }
            int spacing = Math.max(0, 50 - spacer.length());
            final String f = "%s %s%-" + spacing + "s %-7s (memory=%.2f, traverse=%d, backTracks=%d, vpTables=%d)";
            debug.append(String.format(f, (memory_exceeded ? "X" : " "), spacer, label, (memory_exceeded ? "XXXXXX" : String.format("%.05f", state.cost)),
                    (state.memory / (double) hints.max_memory_per_partition), this.traverse_ctr, this.backtrack_ctr, this.current_vertical_partitions.size()));
            return (debug);
        }
    } // THREAD

    // public static void main(String[] vargs) throws Exception {
    // // ArgumentsParser args = ArgumentsParser.load(vargs);
    // // DesignerInfo info = new DesignerInfo(args.catalog_db, args.workload,
    // args.stats);
    // // BranchAndBoundPartitioner partitioner = new
    // BranchAndBoundPartitioner(info);
    // // long start = System.currentTimeMillis();
    // // PartitionPlan design = partitioner.generate();
    // // LOG.debug("STOP: " + (System.currentTimeMillis() - start) / 1000d);
    // }

}