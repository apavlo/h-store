/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.costmodel.*;
import edu.brown.designer.*;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.utils.*;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.*;

/**
 * @author pavlo
 *
 */
public class BranchAndBoundPartitioner extends AbstractPartitioner {
    public static final Logger LOG = Logger.getLogger(BranchAndBoundPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

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
    
    /**
     * 
     */
    protected static class StateVertex {
        private static final long serialVersionUID = 1L;
        
        private static final String START_VERTEX_NAME = "*START*";
        private static final String UPPERBOUND_NAME = "*UPPERBOUND*";
        
        private final int depth;
        private final StateVertex parent;
        private final boolean is_table;
        private final String catalog_key;
        private final String partition_key;
        private final Double cost;
        private final Long memory;
        
        private String debug;
        
        /**
         * Full Constructor
         * @param parent
         * @param catalog_key
         * @param partition_key
         * @param cost
         * @param memory
         */
        private StateVertex(StateVertex parent, int depth, String catalog_key, String partition_key, boolean is_table, Double cost, Long memory) {
            this.parent = parent;
            this.depth = depth;
            this.catalog_key = catalog_key;
            this.partition_key = partition_key;
            this.is_table = is_table;
            this.cost = new Double(cost);
            this.memory = memory;
        }
        
        public StateVertex(StateVertex parent, String catalog_key, String partition_key, boolean is_table, Double cost, Long memory) {
            this(parent, parent.depth + 1, catalog_key, partition_key, is_table, cost, memory);
        }

        /**
         * Generate a new starting placeholder vertex
         * @return
         */
        public static StateVertex getStartVertex(Double cost, Long memory) {
            return (new StateVertex(null, 0, START_VERTEX_NAME, null, false, cost, memory));
        }
        
        /**
         * Generate a new upper-bound placeholder vertex
         * @param cost
         * @param memory
         * @return
         */
        public static StateVertex getUpperBoundVertex(Double cost, Long memory) {
            return (new StateVertex(null, 0, UPPERBOUND_NAME, null, false, cost, memory));
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
         * Construct the mapping from CatalogKey -> PartitionKey
         * We do this so don't have to copy the map at every single level in the tree
         * @return
         */
        public Map<String, String> getCatalogKeyMap() {
            return (this.buildCatalogKeyMap(new ListOrderedMap<String, String>()));
        }
        private Map<String, String> buildCatalogKeyMap(Map<String, String> map) {
            if (this.parent != null) this.parent.buildCatalogKeyMap(map);
            if (this.depth > 0) map.put(this.catalog_key, this.partition_key);
            return (map);
        }
        
        public Map<CatalogType, CatalogType> getCatalogMap(Database catalog_db) {
            Map<CatalogType, CatalogType> m = new ListOrderedMap<CatalogType, CatalogType>();
            
//            // First insert all of the entries from the catalog
//            for (Table catalog_tbl : catalog_db.getTables()) {
//                m.put(catalog_tbl, catalog_tbl.getPartitioncolumn());
//            } // FOR
//            for (Procedure catalog_proc : catalog_db.getProcedures()) {
//                if (catalog_proc.getSystemproc()) continue;
//                m.put(catalog_proc, CatalogUtil.getProcParameter(catalog_proc));
//            } // FOR
            
            // Then overwrite those entries with what's in our vertex tree
            return (this.buildCatalogMap(catalog_db, m));
        }
        private Map<CatalogType, CatalogType> buildCatalogMap(Database catalog_db, Map<CatalogType, CatalogType> map) {
            if (this.parent != null) this.parent.buildCatalogMap(catalog_db, map);
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
            StringBuilder sb = new StringBuilder();
            sb.append("StateVertex[").append(this.catalog_key).append("]\n")
              .append("Cost=").append(this.cost).append("\n")
              .append("Memory=").append(this.memory).append("\n");
            for (Entry<String, String> e : this.getCatalogKeyMap().entrySet()) {
                sb.append(String.format("%-25s%s\n", CatalogKey.getNameFromKey(e.getKey()), CatalogKey.getNameFromKey(e.getValue())));
            }
            return (sb.toString());
        }
    } // END CLASS
    
    protected StateVertex best_vertex = null;
    protected StateVertex upper_bounds_vertex = null;
    protected PartitionPlan upper_bounds_pplan = null;
    protected final Map<CatalogType, WorkloadFilter> traversal_filters = new HashMap<CatalogType, WorkloadFilter>();
    protected TraverseThread thread = null;
    
    protected List<Table> table_visit_order = new ArrayList<Table>();
    protected List<Procedure> proc_visit_order = new ArrayList<Procedure>();
    protected AccessGraph agraph = null;

    /**
     * The key of this map is the CatalogKey of the element that we are trying to plan
     * The value is the list of partitioning attributes that can be set for the key 
     */
    // protected final ListOrderedMap<String, List<String>> base_traversal_attributes = new ListOrderedMap<String, List<String>>();
//    protected int num_tables;

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
    
    public void setParameters(AccessGraph agraph, List<Table> table_visit_order, List<Procedure> proc_visit_order) {
        this.agraph = agraph;
        this.table_visit_order.addAll(table_visit_order);
        this.proc_visit_order.addAll(proc_visit_order);
    }

    public void setUpperBounds(final DesignerHints hints, PartitionPlan pplan, double cost, long memory) throws Exception {
        this.upper_bounds_pplan = pplan;
        this.upper_bounds_pplan.apply(this.info.catalog_db);
        this.upper_bounds_vertex = StateVertex.getUpperBoundVertex(cost, memory);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("UpperBounds Cost Estimate:   %.02f", this.upper_bounds_vertex.cost));
            LOG.debug(String.format("UpperBounds Memory Estimate: %.02f", (this.upper_bounds_vertex.cost / (double)hints.max_memory_per_partition)));
        }
    }
    
    public StateVertex getUpperBounds() {
        return (this.upper_bounds_vertex);
    }

//    public void setTraversalAttributes(Map<String, List<String>> attributes, int num_tables) {
//        this.base_traversal_attributes.clear();
//        this.base_traversal_attributes.putAll(attributes);
//        this.num_tables = num_tables;
//    }

    /**
     * Returns true if the last search halt because of a limit
     * Returns false if the last search completed because it ran out of attributes to investigate
     * If the search hasn't been run yet, returns null 
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
     * @return
     */
    public HaltReason getLastHaltReason() {
        if (this.thread != null) return (this.thread.halt_reason);
        return (null);
    }
    
    public Long getLastBackTrackCount() {
        if (this.thread != null) return (this.thread.backtrack_ctr);
        return (null);
    }
    
    /**
     * 
     * @param hints
     * @throws Exception
     */
    protected void init(final DesignerHints hints) throws Exception {
        assert(this.agraph != null) : "Missing global AccessGraph";
        assert(this.table_visit_order.isEmpty() == false) : "Missing Table Visit Order";
        if (hints.enable_procparameter_search) assert(this.proc_visit_order.isEmpty() == false) : "Missing Procedure Visit Order";
        
        if (debug.get()) LOG.debug("Initializing partitioner for new generate run");
        
        // (1) We first need to construct an AccessGraph that represents the entire workload for all procedures
//        final AccessGraph agraph = AccessGraphGenerator.convertToSingleColumnEdges(info.catalog_db, this.generateAccessGraph());
        
//        List<String> table_visit_order = null;
//        List<String> proc_visit_order = null;
//        boolean need_attributes = this.base_traversal_attributes.isEmpty();
//        if (need_attributes) {
//            table_visit_order = AbstractPartitioner.generateTableOrder(this.info, agraph, hints);
//            //GraphVisualizationPanel.createFrame(agraph).setVisible(true);
//            proc_visit_order = this.generateProcedureOrder(info.catalog_db, hints);
//        } else {
//            assert(this.num_tables < this.base_traversal_attributes.size());
//            table_visit_order = new ArrayList<String>();
//            proc_visit_order = new ArrayList<String>();
//            for (int i = 0, cnt = this.base_traversal_attributes.size(); i < cnt; i++) {
//                if (i < this.num_tables) {
//                    table_visit_order.add(this.base_traversal_attributes.get(i));
//                } else {
//                    proc_visit_order.add(this.base_traversal_attributes.get(i));
//                }
//            } // FOR
//        }

        // (2) Then get the ordered list of tables that we will visit during the search 
//        if (debug.get()) LOG.debug("Table Visit Order: " + table_visit_order);
            
        // (3) Now for each table we're going to visit, come up with a ordered list of how
        //     we will visit the columns of each table. We also will generate filters so that
        //     we can quickly walk through the workload and only evaluate queries that access the
        //     tables for our level in the search tree.
        if (debug.get()) LOG.debug("Creating table specific data structures for the " + table_visit_order.size() + " traversal levels");
        List<Table> filter_tables = new ArrayList<Table>();
        
        // IMPORTANT: Add in any table that is not in the attributes list
        for (Table catalog_tbl : info.catalog_db.getTables()) {
            if (!table_visit_order.contains(catalog_tbl)) {
                filter_tables.add(catalog_tbl);
            }
        } // FOR
        if (debug.get()) LOG.debug("Tables to never filter: " + filter_tables);
        // Now construct all of the workload filters for this level of the traversal
        for (Table catalog_tbl : table_visit_order) {
            filter_tables.add(catalog_tbl);
            this.traversal_filters.put(catalog_tbl, new WorkloadFilter(filter_tables));
        } // FOR        
        
        // (4) Lastly, we need to add the list Procedures that we are going to need to select
        //     ProcParameters for. We don't actually want to populate the entries at this point
        //     because we will be able to throw out of a lot of the candidates once we know 
        //     what the tables are partitioned on using our ParameterCorrelations object
//        assert(proc_visit_order.size() == info.workload.getProcedureHistogram().getValueCount()) :
//            "Missing Procedures from visit order list:\n" +
//            "Expected[" + info.workload.getProcedureHistogram().getValueCount() + "]: " + info.workload.getProcedureHistogram().values() + "\n" +
//            "Got Back[" + proc_visit_order.size() + "]: " + proc_visit_order; 
//        if (debug.get()) LOG.debug("Procedure Visit Order: " + proc_visit_order);
//        if (hints.enable_procparameter_search) {
//            for (String proc_key : proc_visit_order) {
//                assert(proc_key != null);
//                assert(proc_key.isEmpty() == false);
//                Procedure catalog_proc = CatalogKey.getFromKey(info.catalog_db, proc_key, Procedure.class);
//                
//                // Just put in an empty list for now -- WHY???
//                if (need_attributes) this.base_traversal_attributes.put(proc_key, new LinkedList<String>());
//                
//                // Important: Don't put in any workload filters here because we want to estimate the cost of
//                // all procedures (even though we may not have selected a ProcParameter yet) This is necessary
//                // to ensure that our cost estimates never decrease as we traverse the search tree
//                this.traversal_filters.put(catalog_proc, null);
//            } // FOR
//        }

        // Pure genius! Then use the PrimaryKeyPartitioner to calculate an upper-bounds
        if (this.upper_bounds_pplan == null) {
            if (debug.get()) LOG.debug("Calculating upper bounds...");
            PartitionPlan pplan = new MostPopularPartitioner(this.designer, this.info).generate(hints);
            
            // Cost Estimate
            AbstractCostModel cost_model = info.getCostModel();
            assert(cost_model != null);
            cost_model.applyDesignerHints(hints);
            cost_model.clear();
            double cost = cost_model.estimateCost(this.info.catalog_db, this.info.workload);

            // Memory Estimate
            long memory = this.info.getMemoryEstimator().estimate(this.info.catalog_db, info.getNumPartitions());
            assert(memory <= hints.max_memory_per_partition) : memory + " <= " + hints.max_memory_per_partition + "\n" + pplan;
            this.setUpperBounds(hints, pplan, cost, memory);
        }
        
        // We first need a starting node as the root of the graph
        this.best_vertex = StateVertex.getStartVertex(this.upper_bounds_vertex.getCost(), this.upper_bounds_vertex.getMemory());
    }
    
    /**
     * 
     */
    @Override
    public PartitionPlan generate(final DesignerHints hints) throws Exception {
//        hints.addTablePartitionCandidate(info.catalog_db, "CUSTOMER", "C_D_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "CUSTOMER_NAME", Designer.REPLICATED_COLUMN_NAME);
//        hints.addTablePartitionCandidate(info.catalog_db, "DISTRICT", "D_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "HISTORY", "H_C_D_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "ITEM", "I_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "NEW_ORDER", "NO_D_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "ORDERS", "O_D_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "ORDER_LINE", "OL_D_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "STOCK", "S_I_ID");
//        hints.addTablePartitionCandidate(info.catalog_db, "WAREHOUSE", "W_ID");
        
        /*
        for (Table catalog_tbl : info.catalog_db.getTables()) {
            List<String> force_cols = new ArrayList<String>();
            if (catalog_tbl.getName().equalsIgnoreCase("CUSTOMER_NAME")) {
                force_cols.add("C_D_ID");
            } else if (catalog_tbl.getName().equalsIgnoreCase("ITEM")) {
                force_cols.add("I_ID");
            } else if (catalog_tbl.getName().equalsIgnoreCase("HISTORY")) {
                force_cols.add("H_C_W_ID");
            }
            for (String col_name : force_cols) {
                Column catalog_col = catalog_tbl.getColumns().get(col_name);
                hints.addTablePartitionCandidate(catalog_tbl, catalog_col);
                //hints.enablePartitionCandidateDebugging(catalog_col);
            } // FOR
        }
        */
        
        // Initialize our various data structures
        assert(this.info.getCostModel() != null);
        this.info.getCostModel().applyDesignerHints(hints);
        this.init(hints);
        
        if (debug.get()) {
            LOG.debug("Exhaustive Search:    " + hints.exhaustive_search);
            LOG.debug("Greedy Search:        " + hints.greedy_search);
            LOG.debug("Local Search Time:    " + hints.limit_local_time);
            LOG.debug("Back Track Limit:     " + hints.limit_back_tracks);
            LOG.debug("Upper Bounds Cost:    " + this.upper_bounds_vertex.getCost());
            LOG.debug("Upper Bounds Memory:  " + this.upper_bounds_vertex.getMemory());
            LOG.debug("Number of Partitions: " + CatalogUtil.getNumberOfPartitions(info.catalog_db));
            LOG.debug("Memory per Partition: " + hints.max_memory_per_partition);
            LOG.debug("Cost Model Weights:   [execution=" + info.getCostModel().getExecutionWeight() + ", entropy=" + info.getCostModel().getEntropyWeight() + "]"); 
            if (trace.get()) LOG.trace("Procedure Histogram:\n" + info.workload.getProcedureHistogram());
        }
        
        this.thread = new TraverseThread(info, hints, this.best_vertex, this.agraph, this.table_visit_order, this.proc_visit_order);
        thread.run(); // BLOCK
        
        PartitionPlan pplan = null;
        
        // If the best solution is still the start vertex, then we know that it was the upper bound
        // limit was the best solution, so we can just use that. What a waste of a search!
        if (this.best_vertex.isStartVertex()) {
            if (debug.get()) LOG.debug("No new solution was found. Using upper bounds");
            assert(this.upper_bounds_vertex.isUpperBoundVertex());
            assert(this.upper_bounds_pplan != null);
            assert(this.best_vertex.getCost().equals(this.upper_bounds_vertex.getCost())) : this.best_vertex.getCost() + " == " + this.upper_bounds_vertex.getCost();
            pplan = this.upper_bounds_pplan;
            
        // Otherwise reconstruct the PartitionPlan from the StateVertex
        } else {
            Map<String, String> best_catalogkey_map = this.best_vertex.getCatalogKeyMap();
            assert(best_catalogkey_map.size() == (this.table_visit_order.size() + this.proc_visit_order.size())) :
                String.format("Best solution has %d vertices. Should have %d tables and %d procedures!",
                              best_catalogkey_map.size(), this.table_visit_order.size(), this.proc_visit_order.size());
            //System.out.println("MEMORY: " + this.best_vertex.memory);
            
            Map<CatalogType, CatalogType> pplan_map = new HashMap<CatalogType, CatalogType>();
            
            // Apply the changes in the best catalog to the original
            for (Table catalog_tbl : info.catalog_db.getTables()) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                String column_key = best_catalogkey_map.get(table_key);
                if (column_key != null) {
                    Column catalog_col = CatalogKey.getFromKey(info.catalog_db, column_key, Column.class);
                    pplan_map.put(catalog_tbl, catalog_col);
                } else {
                    pplan_map.put(catalog_tbl, catalog_tbl.getPartitioncolumn());
                }
            } // FOR
            
            if (hints.enable_procparameter_search) {
                for (Procedure catalog_proc : info.catalog_db.getProcedures()) {
                    if (AbstractPartitioner.shouldIgnoreProcedure(hints, catalog_proc)) continue;
                    String proc_key = CatalogKey.createKey(catalog_proc);
                    String param_key = best_catalogkey_map.get(proc_key);
                    if (param_key != null) {
                        ProcParameter catalog_proc_param = CatalogKey.getFromKey(info.catalog_db, param_key, ProcParameter.class);
                        pplan_map.put(catalog_proc, catalog_proc_param);
                    } else {
                        pplan_map.put(catalog_proc, CatalogUtil.getProcParameter(catalog_proc));
                    }
                } // FOR
            }
            pplan = PartitionPlan.createFromMap(pplan_map);
            this.setProcedureSinglePartitionFlags(pplan, hints);
        }
        // Make sure that we actually completed the search and didn't just abort
        if (!thread.completed_search) {
            LOG.error("Failed to complete search successfully:\n" + pplan);
            assert(false);
        }
        pplan.apply(info.catalog_db); // Why?
        return (pplan);
    }
    
    /**
     * Return the best StateVertex selected during the search process
     * @return
     */
    protected StateVertex getBestVertex() {
        return (this.best_vertex);
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
        
        private final int num_elements;
        private final int num_tables;
        private final String start_name;
        private StateVertex start;
        private final MemoryEstimator memory_estimator;
        private AbstractCostModel cost_model;
        private final Database search_db;
        private long traverse_ctr = 0;
        private long backtrack_ctr = 0;
        
        private Long halt_time;
        private boolean halt_time_local;
        private boolean halt_search = false;
        private HaltReason halt_reason = null;
        private boolean completed_search = false;
        
        // These tables aren't in our attributes but we still need to use them for estimating size
        private final Set<Table> remaining_tables = new HashSet<Table>();
        
        /**
         * Constructor
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
            this.search_db = info.catalog_db;
            this.search_tables = search_tables;
            this.search_procs = search_procs;
            
            this.all_search_elements.addAll(this.search_tables);
            this.all_search_elements.addAll(this.search_procs);
            this.all_search_keys.addAll(CatalogKey.createKeys(this.all_search_elements));
            
            this.num_tables = this.search_tables.size();
            this.num_elements = this.all_search_elements.size();
            
            // Create a ConstraintPropagator that will keep track of which attributes we can use per database object
            this.agraph = agraph;
            this.cp = new ConstraintPropagator(this.info, this.hints, this.agraph);
            
            // Mark all of our tables that we're not going to search against as updated in our ConstraintPropagator
            if (debug.get()) LOG.debug("Updating ConstraintPropagator with fixed Table info");
            for (Table catalog_tbl : this.info.catalog_db.getTables()) {
                if (this.search_tables.contains(catalog_tbl) == false) {
                    try {
                        this.cp.update(catalog_tbl);
                    } catch (IllegalArgumentException ex) {
                        // IGNORE
                    }
                    this.remaining_tables.add(catalog_tbl);
                }
            } // FOR
            
            if (debug.get()) LOG.debug("Updating ConstraintPropagator with fixed Procedure info");
            for (Procedure catalog_proc : this.info.catalog_db.getProcedures()) {
                if (AbstractPartitioner.shouldIgnoreProcedure(hints, catalog_proc)) continue;
                // Unset all Procedure partitioning parameters
                if (this.search_procs.contains(catalog_proc)) {
                    catalog_proc.setPartitionparameter(NullProcParameter.PARAM_IDX);
                } else {
                    this.cp.update(catalog_proc);
                }
            } // FOR
            
            // For each level in the search tree, compute the sets of tables
            this.previous_tables = (Set<Table>[])(new Set<?>[this.num_elements]);
            Set<Table> temp = new HashSet<Table>(this.remaining_tables);
            for (int i = 0; i < this.num_elements; i++) {
                this.previous_tables[i] = new HashSet<Table>(temp);
                if (i < this.num_tables) this.previous_tables[i].add(this.search_tables.get(i)); 
            } // FOR
            
            // Memory Estimator
            this.memory_estimator = info.getMemoryEstimator();
            
            // Cost Model
            this.cost_model = info.getCostModel();
            assert(this.cost_model != null);
            this.cost_model.applyDesignerHints(hints);
        }
        
        public Database getDatabase() {
            return (this.search_db);
        }
        
        public AbstractCostModel getCostModel() {
            return this.cost_model;
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
            Long stop_local = null; 
            Long stop_total = null;
            
            if (this.hints.limit_local_time != null && this.hints.limit_local_time >= 0) {
                stop_local = this.hints.getNextLocalStopTime();    
            }
            if (this.hints.limit_total_time != null && this.hints.limit_total_time >= 0) {
                stop_total = this.hints.getGlobalStopTime();
            }
            if (stop_local != null && stop_total != null) {
                if (stop_local < stop_total) {
                    this.halt_time = stop_local;
                    this.halt_time_local = true;
                } else {
                    this.halt_time = stop_total;
                    this.halt_time_local = false;
                }
            } else if (stop_local != null) {
                this.halt_time = stop_local;
                this.halt_time_local = true;
            } else if (stop_total != null) {
                this.halt_time = stop_total;
                this.halt_time_local = false;
            }
            if (debug.get() && this.halt_time != null)
                LOG.debug("Remaining Search Time: " + (this.halt_time - System.currentTimeMillis()) / 1000d);  
                
            if (debug.get()) LOG.debug(String.format("Starting Search for %d Elements: %s", this.all_search_elements.size(), this.all_search_elements));
            try {
                this.traverse(start, 0);
            } catch (Exception ex) {
                LOG.fatal(ex.getLocalizedMessage());
                ex.printStackTrace();
                System.exit(1);
            }
            this.completed_search = true;
            BranchAndBoundPartitioner.this.halt_reason = (this.halt_reason != null ? this.halt_reason : HaltReason.EXHAUSTED_SEARCH);
        }
    
        protected void propagateConstraints() {
            // TODO
        }
        
        /**
         * 
         * @param parent
         * @param idx
         * @throws Exception
         */
        protected void traverse(StateVertex parent, int idx) throws Exception {
            assert(idx < this.num_elements && idx >= 0);
            final CatalogType current = this.all_search_elements.get(idx);
            assert(current != null);
            final String current_key = this.all_search_keys.get(idx);
            
            this.traverse_ctr++;
            final boolean is_table = (idx < this.num_tables);
            final boolean is_last_table = ((idx + 1) == this.num_tables);
            // Are we the last element in the list, thereby creating a complete solution
            final boolean complete_solution = (idx + 1 == this.num_elements);
            if (is_table) assert(current instanceof Table);
            final String spacer = BranchAndBoundPartitioner.TRAVERSAL_SPACERS.get(idx);
            
            if (this.halt_search == false) {
                assert(this.halt_reason == null);
                if (hints.limit_back_tracks != null && hints.limit_back_tracks >= 0 && this.backtrack_ctr > hints.limit_back_tracks) {
                    LOG.info("Hit back track limit. Halting search [" + this.backtrack_ctr + "]");
                    this.halt_search = true;
                    this.halt_reason = HaltReason.BACKTRACK_LIMIT;
                } else if (this.halt_time != null && System.currentTimeMillis() >= this.halt_time) {
                    LOG.info("Hit time limit. Halting search [" + this.backtrack_ctr + "]");
                    this.halt_search = true;
                    this.halt_reason = (this.halt_time_local ? HaltReason.LOCAL_TIME_LIMIT : HaltReason.GLOBAL_TIME_LIMIT);
                }
            } else return;
            
            // Get the list of possible attributes that we could use for this current element
            Collection<CatalogType> current_attributes = this.cp.getPossibleValues(current, CatalogType.class);
            final int num_attributes = current_attributes.size();
            assert(current_attributes != null);
            if (trace.get()) LOG.trace("Traverse [current=" + current.getName() + ", # of attributes=" + current_attributes.size() + "]");
    
            // Get our workload filter for this level of the traversal
            Workload.Filter filter = BranchAndBoundPartitioner.this.traversal_filters.get(current);
            
            // Descendant tables used for memory calculations
            Set<Table> all_tables = this.previous_tables[idx];
            assert(all_tables.isEmpty() == false) : "No previous tables at index " + idx + "?";
            
            // The local best vertex is the best vertex for this level in the traversal
            StateVertex local_best_vertex = null;
            
            // Iterate through the columns and find the one with the best cost
            int attribute_ctr = 0;
            for (CatalogType attribute : current_attributes) {
                assert(attribute != null) : "Null attribute key for " + current + ": " + current_attributes; 
                String attribute_key = CatalogKey.createKey(attribute);
                if (trace.get()) LOG.trace("Evaluating " + attribute.fullName());
                boolean memory_exceeded = false;
                Long memory = null;
                CatalogType current_attribute = null;
                
                // Is this the last element we have to look at
                boolean last_attribute = (++attribute_ctr == num_attributes);
                
                // Dynamic Debugging
                this.cost_model.setDebuggingEnabled(this.hints.isDebuggingEnabled(attribute_key));
                
                // IMPORTANT: We have to invalidate the cache for the current element *AND* all those levels
                // below us in the search tree!
                if (this.cost_model.isCachingEnabled()) {
                    for (int i = idx; i < this.num_elements; i++) {
                        if (trace.get()) LOG.trace("Invalidating " + this.all_search_keys.get(i));
                        this.cost_model.invalidateCache(this.all_search_keys.get(i));
                    } // FOR
                // If we're not using caching, then just clear out the cost model completely
                } else {
                    this.cost_model.clear();
                }
                
                // ----------------------------------------------
                // TABLE PARTITIONING KEY
                // ----------------------------------------------
                if (is_table) {
                    Table current_tbl = (Table)current;
                    Column search_col = (Column)attribute;
                    Column current_col = null;
                    
                    // Check whether this is our replication marker column
                    if (search_col instanceof ReplicatedColumn) {
                        current_tbl.setIsreplicated(true);
                        current_col = ReplicatedColumn.get(current_tbl);
                    // MultiColumn
                    } else if (search_col instanceof MultiColumn) {
                        // Nothing special?
                        current_tbl.setIsreplicated(false);
                        current_col = search_col;
                        
                    // Otherwise partition on this particular column
                    } else {
                        current_tbl.setIsreplicated(false);
                        current_col = current_tbl.getColumns().get(search_col.getName());
                    }
                    assert(current_col != null);
                    current_tbl.setPartitioncolumn(current_col);
                    assert(current_col.getName().equals(current_tbl.getPartitioncolumn().getName())) :
                        "Unexpected " + current_col.fullName() + " != " + current_tbl.getPartitioncolumn().fullName();
                    
                    // Estimate memory size
                    if (trace.get()) LOG.trace(String.format("Calculating memory size of current solution [%s]: %s", current_col.fullName(), all_tables));                
                    memory = this.memory_estimator.estimate(search_db, info.getNumPartitions(), all_tables);
                    memory_exceeded = (memory > this.hints.max_memory_per_partition);
                    if (trace.get()) LOG.trace(String.format("%s Memory: %d [exceeded=%s]", current_col.fullName(), memory, memory_exceeded));
                    
                    current_attribute = current_col;
                    
                // ----------------------------------------------
                // PROCEDURE PARTITIONING PARAMETER
                // ----------------------------------------------
                } else {
                    Procedure current_proc = (Procedure)current;
                    ProcParameter current_proc_param = (ProcParameter)attribute;
                    assert(current_proc_param != null);
                    current_proc.setPartitionparameter(current_proc_param.getIndex());
                    memory = parent.memory;
                    current_attribute = current_proc_param;
                }

                // ----------------------------------------------
                // COST ESTIMATION
                // ----------------------------------------------
                // Don't estimate the cost if it doesn't fit
                Double cost = null;
                if (!memory_exceeded) {
                    cost = this.cost_model.estimateCost(search_db, info.workload, filter);
                } else {
                    cost = Double.MAX_VALUE;
                }

                StateVertex state = new StateVertex(parent, current_key, attribute_key, is_table, cost, memory);
//                if (!this.graph.containsVertex(parent)) this.graph.addVertex(parent);
//                this.graph.addEdge(new StateEdge(), parent, state, EdgeType.DIRECTED);

                if (local_best_vertex == null || local_best_vertex.cost > state.cost) local_best_vertex = state;
                assert(local_best_vertex != null);
                
                // ----------------------------------------------
                // DEBUG OUTPUT
                // ----------------------------------------------
                state.debug = this.cost_model.getLastDebugMessages();
                if (debug.get()) LOG.debug(this.createLevelOutput(state, CatalogUtil.getDisplayName(current_attribute, false), spacer, memory_exceeded));
                
                // ----------------------------------------------
                // ANALYSIS
                // ----------------------------------------------
                if (memory_exceeded) continue;
                
                // The cost of our parent can never be greater than our cost
                if (!parent.isStartVertex()) {
                    double fudge = 0.04;
                    double parent_cost = MathUtil.roundToDecimals(parent.cost, 2);
                    double current_cost = MathUtil.roundToDecimals(cost, 2) + fudge;
                    
                    if (!(parent_cost <= current_cost) && debug.get()) {
                        LOG.debug("Parent Cost Model Info:\n " + parent.debug);
                        LOG.debug("Parent:\n" + parent.toString());
                        LOG.debug(StringUtil.DOUBLE_LINE);
                        LOG.debug("Current:\n" + state.toString());
                        LOG.debug("Last Cost Model:\n" + state.debug);
                    }
                    assert(parent_cost <= current_cost) :
                        attribute_key + ": Parent[" + parent.cost + "] <= Current[" + cost + "]" + "\n" +
                        "Rounded: Parent[" + MathUtil.roundToDecimals(parent.cost, 2) + "] <= Current[" + MathUtil.roundToDecimals(cost, 2) + "]";
                }
                
                // If this is a complete solution, then check whether if it is the best one that we have seen thus far
                // Allow the current solution to be the best solution if the following criteria are met:
                //  (1) It's a complete solution
                //  (2) It has not exhausted our per-partition memory limit
                //  (4) It is less than the upper bounds limit
                //  (3) And one of the following is true:
                //      (a) The current best solution is the start vertex
                //      (b) Or the current solution has a cost less than the current best solution
                if (complete_solution &&
                    !memory_exceeded && 
                    cost < BranchAndBoundPartitioner.this.upper_bounds_vertex.cost &&
                    (BranchAndBoundPartitioner.this.best_vertex.isStartVertex() || cost < BranchAndBoundPartitioner.this.best_vertex.cost)) {
                    assert(best_vertex.cost > state.cost) : "Best=" + best_vertex.cost + ", Current=" + state.cost;
                    assert(upper_bounds_vertex.cost > state.cost) : "Upper=" + upper_bounds_vertex.cost + ", Current=" + state.cost;
                    
                    BranchAndBoundPartitioner.this.best_vertex = state;
                    if (trace.get()) LOG.trace("Last Cost Model Info:\n " + this.cost_model.getLastDebugMessages());
                    if (debug.get()) LOG.debug("New Best Solution:\n" + StringUtil.box(best_vertex.toString()));
                    
                    // Log new solution cost
                    if (this.hints.shouldLogSolutionCosts()) this.hints.logSolutionCost(state.cost);
                    
//                        for (int i = 0; i < ((TimeIntervalCostModel)this.cost_model).getIntevalCount(); i++) {
//                            System.err.println("Interval #" + i);
//                            System.err.println(((TimeIntervalCostModel)this.cost_model).getCostModel(i).getTxnPartitionAccessHistogram());
//                            System.err.println("================================================");
//                        }
//                        
//                        System.exit(1);
                }
                
                // ----------------------------------------------
                // CONTINUE SEARCH TRAVERSAL
                // ----------------------------------------------
                
                // We now need to look to see whether we should continue down this path. Much like
                // selecting a new best solution, the following criteria must be met in order for
                // the partitioner to be allowed to continue down a search path:
                //  (1) If this is last attribute at this level in the tree
                //          AND we're looking at a TABLE
                //          AND we're doing a greedy search
                //      Then we'll always want to keep traversing here.
                // Otherwise, the following the criteria must be met:
                //  (1) It must not be a complete solution
                //  (2) The current catalog item must be a table (no procedures!)
                //  (3) The cost must be less than the current best solution cost
                //  (4) The cost must be less than the upper bounds limit
                // Or we can just say screw all that and keep going if the exhaustive flag is enabled
                if ((last_attribute && is_table && this.hints.greedy_search) ||
                    this.hints.exhaustive_search || (
                    !complete_solution &&
                    is_table &&
                    cost < BranchAndBoundPartitioner.this.best_vertex.cost &&
                    cost < BranchAndBoundPartitioner.this.upper_bounds_vertex.cost)) {
                    
                    // IMPORTANT: If this is the last table in our traversal, then we need to switch over 
                    // to working Procedures->ProcParameters. We have to now calculate the list of attributes
                    // that we want to consider
//                    if (is_last_table && this.hints.enable_procparameter_search) {
//                        for (int i = idx + 1; i < this.num_elements; i++) {
//                            Procedure catalog_proc = (Procedure)this.all_search_elements.get(i);
//                            LinkedList<String> attributes = AbstractPartitioner.generateProcParameterOrder(info, this.search_db, catalog_proc, hints);
//                            
//                            // We should have ProcParameter candidates!
//                            assert(attributes.isEmpty() == false) : "No ProcParameter candidates: " + catalog_proc + "\n" + state;
//                            this.traversal_attributes.get(proc_key).clear();
//                            this.traversal_attributes.get(proc_key).addAll(attributes);
//                        } // FOR
//                    }

                    // We only traverse if this is a table. The ProcParameter selection is a simple greedy algorithm
                    if (this.hints.greedy_search == false || (this.hints.greedy_search == true && last_attribute)) {
                        if (debug.get() && this.hints.greedy_search)
                            LOG.debug(this.createLevelOutput(local_best_vertex, "GREEDY->" + local_best_vertex.getPartitionKey(), spacer, false));
                        this.cp.update(current);
                        this.traverse((this.hints.greedy_search ? local_best_vertex : state), idx + 1);
                        this.cp.reset(current);
                    }
                }
                
                if (this.halt_search) break;
            } // FOR

            
            // ProcParameter Traversal
            if (!is_table && !this.halt_search) {
                assert(local_best_vertex != null) : "Missing local best vertex for " + current_key;

                // Set the partitioning ProcParameter in this Procedure to be the one that 
                // had the lowest cost in our search up above.
                Procedure current_proc = (Procedure)current;
                String best_key = local_best_vertex.getPartitionKey();
                ProcParameter current_param = CatalogKey.getFromKey(this.search_db, best_key, ProcParameter.class);
                assert(current_param != null);
                current_proc.setPartitionparameter(current_param.getIndex());
                
                // Is this necessary?
                this.cost_model.invalidateCache(current_proc);
                
                if (debug.get()) LOG.debug(this.createLevelOutput(local_best_vertex, CatalogUtil.getDisplayName(current_param), spacer, false));
                
                // If there are more Procedures to partition and we have gone past our best cost
                // our upper bounds, then keep going... 
                if (complete_solution == false && hints.enable_procparameter_search &&
                     (this.hints.greedy_search == true) || 
                     (local_best_vertex.cost < best_vertex.cost &&
                      local_best_vertex.cost < upper_bounds_vertex.cost)) {
                    this.cp.update(current_proc);
                    this.traverse(local_best_vertex, idx + 1);
                    this.cp.reset(current_proc);
                }

                // Important: We will have to unset the Procedure partitioning parameter
                // if this was the last table and we are going back up the tree. This is because
                // unlike Tables, where we exclude queries using WorkloadFilters, we can't do
                // the same thing for ProcParameters
                // manually once we finish up with this table because the next passes will
                // overwrite the catalog that we saved off
                for (int i = idx; i < this.num_elements; i++) {
                    Procedure catalog_proc = (Procedure)this.all_search_elements.get(i);
                    catalog_proc.setPartitionparameter(-1);
                } // FOR
            }
            if (this.halt_search == false) this.backtrack_ctr++;
            return;
        }
        
        private StringBuilder createLevelOutput(StateVertex state, String label, String spacer, boolean memory_exceeded) {
            final StringBuilder debug = new StringBuilder();
            if (!BranchAndBoundPartitioner.this.best_vertex.isStartVertex()) {
                debug.append(String.format("[%.02f] ", BranchAndBoundPartitioner.this.best_vertex.cost));
            } else {
                debug.append("[----] ");
            }
            final String f = "%s %s%s: %s  (memory=%d, traverse=%d, backtracks=%d)";
            debug.append(String.format(f,
                    (memory_exceeded ? "E" : " "),
                    spacer,
                    label,
                    (memory_exceeded ? "XXXXXX" : state.cost),
                    state.memory,
                    this.traverse_ctr,
                    this.backtrack_ctr
            ));
            return (debug);
        }
    } // THREAD

//    public static void main(String[] vargs) throws Exception {
////        ArgumentsParser args = ArgumentsParser.load(vargs);
////        DesignerInfo info = new DesignerInfo(args.catalog_db, args.workload, args.stats);
////        BranchAndBoundPartitioner partitioner = new BranchAndBoundPartitioner(info);
////        long start = System.currentTimeMillis();
////        PartitionPlan design = partitioner.generate();
////        LOG.debug("STOP: " + (System.currentTimeMillis() - start) / 1000d);
//    }

}