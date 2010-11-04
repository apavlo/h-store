/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.*;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.costmodel.*;
import edu.brown.designer.*;
import edu.brown.utils.*;
import edu.brown.workload.*;
import edu.uci.ics.jung.graph.*;

/**
 * @author pavlo
 *
 */
public class BranchAndBoundPartitioner extends AbstractPartitioner {
    protected static final Logger LOG = Logger.getLogger(BranchAndBoundPartitioner.class);

    /**
     * Debug Output Spacers
     */
    public static final Map<Integer, String> TRAVERSAL_SPACERS = new HashMap<Integer, String>();
    static {
        // Debug Spacer
        String spacer = "";
        for (int i = 0; i < 100; i++) {
            spacer += "  "; // StringUtil.SPACER;
            TRAVERSAL_SPACERS.put(i, new String(spacer));
        } // FOR
    } // STATIC
    
    /**
     * 
     */
    protected static class StateVertex extends ListOrderedMap<String, String> {
        private static final long serialVersionUID = 1L;
        
        private static final String START_VERTEX_NAME = "*START*";
        private static final String UPPERBOUND_NAME = "*UPPERBOUND*";
        
        private final String catalog_key;
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
        public StateVertex(StateVertex parent, String catalog_key, String partition_key, Double cost, Long memory) {
            this.catalog_key = catalog_key;
            this.cost = new Double(cost);
            this.memory = memory;
            
            if (parent != null) this.putAll(parent);
            if (partition_key != null) this.put(catalog_key, partition_key);
        }

        /**
         * Generate a new starting placeholder vertex
         * @return
         */
        public static StateVertex getStartVertex(Double cost, Long memory) {
            return (new StateVertex(null, START_VERTEX_NAME, null, cost, memory));
        }
        
        /**
         * Generate a new upper-bound placeholder vertex
         * @param cost
         * @param memory
         * @return
         */
        public static StateVertex getUpperBoundVertex(Double cost, Long memory) {
            return (new StateVertex(null, UPPERBOUND_NAME, null, cost, memory));
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
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("StateVertex[").append(this.catalog_key).append("]\n")
              .append("Cost=").append(this.cost).append("\n")
              .append("Memory=").append(this.memory).append("\n");
            for (String table_key : this.keySet()) {
                sb.append(String.format("%-25s%s\n", CatalogKey.getNameFromKey(table_key), CatalogKey.getNameFromKey(this.get(table_key))));
            }
            return (sb.toString());
        }
    } // END CLASS

    /**
     * Empty placeholder so that we can use JUNG graphs 
     */
    protected class StateEdge {
        // Nothing...
    } // END CLASS
    
    
    protected StateVertex best_vertex = null;
    protected StateVertex upper_bounds_vertex = null;
    protected PartitionPlan upper_bounds_pplan = null;
    protected final Map<String, WorkloadFilter> traversal_filters = new HashMap<String, WorkloadFilter>();
    protected TraverseThread thread = null;

    /**
     * The key of this map is the CatalogKey of the element that we are trying to plan
     * The value is the list of partitioning attributes that can be set for the key 
     */
    protected final ListOrderedMap<String, List<String>> base_traversal_attributes = new ListOrderedMap<String, List<String>>();
    protected int num_tables;

    /**
     * @param stats_catalog_db
     * @param workload
     */
    public BranchAndBoundPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    public void setUpperBounds(final DesignerHints hints, PartitionPlan pplan, double cost, long memory) throws Exception {
        this.upper_bounds_pplan = pplan;
        this.upper_bounds_pplan.apply(this.info.catalog_db);
        this.upper_bounds_vertex = StateVertex.getUpperBoundVertex(cost, memory);
        LOG.info(String.format("UpperBounds Cost Estimate:   %.02f", this.upper_bounds_vertex.cost));
        LOG.info(String.format("UpperBounds Memory Estimate: %.02f", (this.upper_bounds_vertex.cost / (double)hints.max_memory_per_partition)));
    }
    
    public StateVertex getUpperBounds() {
        return (this.upper_bounds_vertex);
    }
    
    public void setTraversalAttributes(Map<String, List<String>> attributes, int num_tables) {
        this.base_traversal_attributes.clear();
        this.base_traversal_attributes.putAll(attributes);
        this.num_tables = num_tables;
    }

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
     * 
     * @param hints
     * @throws Exception
     */
    protected void init(final DesignerHints hints) throws Exception {
        LOG.debug("Initializing partitioner for new generate run");
        
        // (1) We first need to construct an AccessGraph that represents the entire workload for all procedures
        final AccessGraph agraph = this.generateAccessGraph();
        
        List<String> table_visit_order = null;
        List<String> proc_visit_order = null;
        boolean need_attributes = this.base_traversal_attributes.isEmpty();
        if (need_attributes) {
            table_visit_order = generateTableOrder(this.info, agraph, hints);
            proc_visit_order = this.generateProcedureOrder(info.catalog_db, hints);
        } else {
            assert(this.num_tables < this.base_traversal_attributes.size());
            table_visit_order = new ArrayList<String>();
            proc_visit_order = new ArrayList<String>();
            for (int i = 0, cnt = this.base_traversal_attributes.size(); i < cnt; i++) {
                if (i < this.num_tables) {
                    table_visit_order.add(this.base_traversal_attributes.get(i));
                } else {
                    proc_visit_order.add(this.base_traversal_attributes.get(i));
                }
            } // FOR
        }

        // (2) Then get the ordered list of tables that we will visit during the search 
        LOG.info("Table Visit Order: " + table_visit_order);
            
        // (3) Now for each table we're going to visit, come up with a ordered list of how
        //     we will visit the columns of each table. We also will generate filters so that
        //     we can quickly walk through the workload and only evaluate queries that access the
        //     tables for our level in the search tree.
        LOG.debug("Creating table specific data structures for the " + table_visit_order.size() + " traversal levels");
        List<Table> filter_tables = new ArrayList<Table>();
        
        // IMPORTANT: Add in any table that is not in the attributes list
        for (Table catalog_tbl : info.catalog_db.getTables()) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            if (!table_visit_order.contains(table_key)) {
                filter_tables.add(catalog_tbl);
            }
        } // FOR
        LOG.debug("Tables to never filter: " + filter_tables);
        
        for (String table_key : table_visit_order) {
            Table catalog_tbl = CatalogKey.getFromKey(info.catalog_db, table_key, Table.class);
        
            // Columns Visit Order
            if (need_attributes) {
                LinkedList<String> columns_visit_order = generateColumnOrder(info, agraph, catalog_tbl, hints);
                this.base_traversal_attributes.put(table_key, columns_visit_order);
                this.num_tables++;
                LOG.info(catalog_tbl.getName() + " Column Visit Order: " + columns_visit_order);
            }

            // Now construct all of the workload filters for this level of the traversal
            filter_tables.add(catalog_tbl);
            this.traversal_filters.put(table_key, new WorkloadFilter(filter_tables));
        } // FOR
        
        // (4) Lastly, we need to add the list Procedures that we are going to need to select
        //     ProcParameters for. We don't actually want to populate the entries at this point
        //     because we will be able to throw out of a lot of the candidates once we know 
        //     what the tables are partitioned on using our ParameterCorrelations object
//        assert(proc_visit_order.size() == info.workload.getProcedureHistogram().getValueCount()) :
//            "Missing Procedures from visit order list:\n" +
//            "Expected[" + info.workload.getProcedureHistogram().getValueCount() + "]: " + info.workload.getProcedureHistogram().values() + "\n" +
//            "Got Back[" + proc_visit_order.size() + "]: " + proc_visit_order; 
        LOG.info("Procedure Visit Order: " + proc_visit_order);
        if (hints.enable_procparameter_search) {
            for (String proc_key : proc_visit_order) {
                assert(proc_key != null);
                assert(proc_key.isEmpty() == false);
                
                // Just put in an empty list for now
                if (need_attributes) this.base_traversal_attributes.put(proc_key, new LinkedList<String>());
                
                // Important: Don't put in any workload filters here because we want to estimate the cost of
                // all procedures (even though we may not have selected a ProcParameter yet) This is necessary
                // to ensure that our cost estimates never decrease as we traverse the search tree
                this.traversal_filters.put(proc_key, null);
            } // FOR
        }

        // Pure genius! Then use the PrimaryKeyPartitioner to calculate an upper-bounds
        if (this.upper_bounds_pplan == null) {
            LOG.info("Calculating upper bounds...");
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
        
        LOG.debug("Number of partitions: " + CatalogUtil.getNumberOfPartitions(info.catalog_db));
        LOG.debug("Memory per Partition: " + hints.max_memory_per_partition);
        LOG.debug("Cost Model Weights:   [execution=" + info.getCostModel().getExecutionWeight() + ", entropy=" + info.getCostModel().getEntropyWeight() + "]"); 
        LOG.debug("Procedure Histogram:\n" + info.workload.getProcedureHistogram());
        
        this.thread = new TraverseThread(info, hints, this.best_vertex, this.base_traversal_attributes, this.num_tables);
        thread.run();
        
        PartitionPlan pplan = null;
        
        // If the best solution is still the start vertex, then we know that it was the upper bound
        // limit was the best solution, so we can just use that. What a waste of a search!
        if (this.best_vertex.isStartVertex()) {
            LOG.info("No new solution was found. Using upper bounds");
            assert(this.upper_bounds_vertex.isUpperBoundVertex());
            assert(this.upper_bounds_pplan != null);
            assert(this.best_vertex.getCost().equals(this.upper_bounds_vertex.getCost())) : this.best_vertex.getCost() + " == " + this.upper_bounds_vertex.getCost();
            pplan = this.upper_bounds_pplan;
            
        // Otherwise reconstruct the PartitionPlan from the StateVertex
        } else {
            assert(this.best_vertex.size() == this.base_traversal_attributes.size()) :
                "Best solution has " + this.best_vertex.size() + " vertices. " +
                "Should have " + this.base_traversal_attributes.size() + " vertices!";
            //System.out.println("MEMORY: " + this.best_vertex.memory);
            
            Map<CatalogType, CatalogType> pplan_map = new HashMap<CatalogType, CatalogType>();
            
            // Apply the changes in the best catalog to the original
            for (Table catalog_tbl : info.catalog_db.getTables()) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                String column_key = this.best_vertex.get(table_key);
                if (column_key != null) {
                    Column catalog_col = CatalogKey.getFromKey(info.catalog_db, column_key, Column.class);
                    pplan_map.put(catalog_tbl, catalog_col);
                } else {
                    pplan_map.put(catalog_tbl, catalog_tbl.getPartitioncolumn());
                }
            } // FOR
            
            if (hints.enable_procparameter_search) {
                for (Procedure catalog_proc : info.catalog_db.getProcedures()) {
                    String proc_key = CatalogKey.createKey(catalog_proc);
                    String param_key = this.best_vertex.get(proc_key);
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
        
        private final DesignerInfo info;
        private final DesignerHints hints;
        private final DelegateForest<StateVertex, StateEdge> graph = new DelegateForest<StateVertex, StateEdge>();
        private final ListOrderedMap<String, List<String>> traversal_attributes;
        private final int num_elements;
        private final int num_tables;
        private final String start_name;
        private StateVertex start;
        private final MemoryEstimator memory_estimator;
        private AbstractCostModel cost_model;
        private final Database search_db;
        private long traverse_ctr = 0;
        private long backtrack_ctr = 0;
        
        private boolean halt_search = false;
        private boolean completed_search = false;
        private long halt_time;
        
        // These tables aren't in our attributes but we still need to use them for estimating size
        private final Set<Table> remaining_tables = new HashSet<Table>();
        
        /**
         * Constructor
         * @param info
         * @param start
         * @param traversal_attributes
         * @param children
         */
        public TraverseThread(DesignerInfo info, DesignerHints hints, StateVertex start, ListOrderedMap<String, List<String>> traversal_attributes, int num_tables) {
            this.info = info;
            this.hints = hints;
            this.start = start;
            this.start_name = CatalogKey.getNameFromKey(start.getCatalogKey());
            this.traversal_attributes = traversal_attributes;
            this.num_elements = this.traversal_attributes.size();
            
            // This the catalog that this thread will muck around with
            Database clone_db = null;
            try {
                clone_db = CatalogUtil.cloneDatabase(info.catalog_db);
            } catch (Exception ex) {
                LOG.fatal("Failed to clone database", ex);
                System.exit(1);
            }
            this.search_db = clone_db;
//            Catalog search_catalog = new Catalog();
//            search_catalog.execute(info.catalog_db.getCatalog().serialize());
//            this.search_db = CatalogUtil.getDatabase(search_catalog);
//            this.search_db = info.catalog_db;
            this.num_tables = num_tables;
            
            // Unset all Procedure partitioning parameters
            for (int i = this.num_tables; i < this.traversal_attributes.size(); i++) {
                String proc_key = this.traversal_attributes.get(i);
                Procedure catalog_proc = CatalogKey.getFromKey(search_db, proc_key, Procedure.class);
                catalog_proc.setPartitionparameter(NullProcParameter.PARAM_IDX);
            } // FOR
            
            for (Table catalog_tbl : search_db.getTables()) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                if (!this.traversal_attributes.containsKey(table_key)) this.remaining_tables.add(catalog_tbl);
            }
            
            
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
            long stop_total = this.hints.getGlobalStopTime();
            long stop_local = this.hints.getNextLocalStopTime();
            this.halt_time = Math.min(stop_local, stop_total);
                
            try {
                this.graph.addVertex(this.start);
                this.traverse(start, 0);
            } catch (Exception ex) {
                LOG.fatal(ex.getLocalizedMessage());
                ex.printStackTrace();
                System.exit(1);
            }
            this.completed_search = true;
            System.gc();
        }
    
        /**
         * 
         * @param parent
         * @param idx
         * @throws Exception
         */
        protected void traverse(StateVertex parent, int idx) throws Exception {
            assert(idx < this.traversal_attributes.size());
            final String current_key = this.traversal_attributes.get(idx);
            final boolean is_table = (idx < this.num_tables);
            final boolean is_last_table = ((idx + 1) == this.num_tables);
            CatalogType current = null;
            
            if (!this.halt_search) {
                if (hints.limit_back_tracks != null && is_table && this.backtrack_ctr > hints.limit_back_tracks) {
                    LOG.info("Hit back track limit. Halting search [" + this.backtrack_ctr + "]");
                    this.halt_search = true;
                } else if (System.currentTimeMillis() >= this.halt_time) {
                    LOG.info("Hit time limit. Halting search [" + this.backtrack_ctr + "]");
                    this.halt_search = true;
                }
            } else {
                return;
            }
            
            // Table -> Columns
            if (is_table) {
                current = CatalogKey.getFromKey(this.search_db, current_key, Table.class);
            // Procedure -> ProcParameter
            } else {
                current = CatalogKey.getFromKey(this.search_db, current_key, Procedure.class);
            }
            LOG.debug("Traverse [current=" + current.getName() + ", # of attributes=" + this.traversal_attributes.get(current_key).size() + "]");
    
            // Get our workload filter for this level of the traversal
            AbstractWorkload.Filter filter = BranchAndBoundPartitioner.this.traversal_filters.get(current_key);
            
            // Create a clone of the catalog and add in our table
//            Catalog new_catalog = CatalogUtil.cloneBaseCatalog(parent.catalog_db.getCatalog());
//            for (Table catalog_tbl : parent.catalog_db.getTables()) {
//                CatalogUtil.clone(catalog_tbl, new_catalog);
//            } // FOR
//            Database new_catalog_db = CatalogUtil.getDatabase(new_catalog);
//            Table new_catalog_tbl = CatalogUtil.clone(current_tbl, new_catalog);
//            CatalogUtil.cloneConstraints(info.catalog_db, new_catalog_db);
            
            // Are we the last element in the list, thereby creating a complete solution
            final boolean complete_solution = (idx + 1 == this.num_elements);
            final String spacer = BranchAndBoundPartitioner.TRAVERSAL_SPACERS.get(idx);
            assert(this.traversal_attributes.containsKey(current_key)) : 
                "Missing traversal attributes for " + current_key + "\n" + this.traversal_attributes;
            
            // Descendant tables used for memory calculations
            Set<Table> previous_tables = new HashSet<Table>(this.remaining_tables);
            for (int i = 0; i <= idx; i++) {
                if (i >= this.num_tables) break;
                previous_tables.add(CatalogKey.getFromKey(search_db, this.traversal_attributes.get(i), Table.class)); 
            }
            assert(!previous_tables.isEmpty()) : "No tables at index " + idx + "?";
            
            // The local best vertex is the best vertex for this level in the traversal
            StateVertex local_best_vertex = null;
            
            // Iterate through the columns and find the one with the best cost
            for (String attribute_key : this.traversal_attributes.get(current_key)) {
                assert(attribute_key != null) : "Null attribute key for " + current + ": " + this.traversal_attributes.get(current_key); 
                LOG.debug("Evaluating " + current.getName() + "." + CatalogKey.getNameFromKey(attribute_key));
                this.traverse_ctr++;
                boolean memory_exceeded = false;
                Long memory = null;
                CatalogType current_attribute = null;
                
                // Dynamic Debugging
                this.cost_model.setDebuggingEnabled(this.hints.isDebuggingEnabled(attribute_key));
                
                // IMPORTANT: We have to invalidate the cache for the current element *AND* all those levels
                // below us in the search tree!
                if (this.cost_model.isCachingEnabled()) {
                    for (int i = idx; i < this.num_elements; i++) {
                        LOG.debug("Invalidating " + this.traversal_attributes.get(i));
                        this.cost_model.invalidateCache(this.traversal_attributes.get(i));
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
                    Column search_col = CatalogKey.getFromKey(this.search_db, attribute_key, Column.class);
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
                        "Unexpected " + current_col.getName() + " != " + current_tbl.getPartitioncolumn().getName();
                    
                    // Estimate memory size
                    LOG.debug("Calculating memory size of current solution [" + CatalogUtil.getDisplayName(current_col) + "]");                
                    memory = this.memory_estimator.estimate(search_db, info.getNumPartitions(), previous_tables);
                    memory_exceeded = (memory > this.hints.max_memory_per_partition);
                    
                    current_attribute = current_col;
                    
                // ----------------------------------------------
                // PROCEDURE PARTITIONING PARAMETER
                // ----------------------------------------------
                } else {
                    Procedure current_proc = (Procedure)current;
                    ProcParameter current_proc_param = CatalogKey.getFromKey(this.search_db, attribute_key, ProcParameter.class);
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

                StateVertex state = new StateVertex(parent, current_key, attribute_key, cost, memory);
//                if (!this.graph.containsVertex(parent)) this.graph.addVertex(parent);
//                this.graph.addEdge(new StateEdge(), parent, state, EdgeType.DIRECTED);

                if (local_best_vertex == null || local_best_vertex.cost > state.cost) local_best_vertex = state;
                assert(local_best_vertex != null);
                
                // ----------------------------------------------
                // DEBUG OUTPUT
                // ----------------------------------------------
                state.debug = this.cost_model.getLastDebugMessages();
                LOG.info(this.createLevelOutput(state, CatalogUtil.getDisplayName(current_attribute, false), spacer, memory_exceeded));
                
                // ----------------------------------------------
                // ANALYSIS
                // ----------------------------------------------
                if (memory_exceeded) continue;
                
                // The cost of our parent can never be greater than our cost
                if (!parent.isStartVertex()) {
                    double fudge = 0.04;
                    double parent_cost = MathUtil.roundToDecimals(parent.cost, 2);
                    double current_cost = MathUtil.roundToDecimals(cost, 2) + fudge;
                    
                    if (!(parent_cost <= current_cost)) {
                        LOG.info("Parent Cost Model Info:\n " + parent.debug);
                        LOG.info("Parent:\n" + parent.toString());
                        LOG.info(StringUtil.DOUBLE_LINE);
                        LOG.info("Current:\n" + state.toString());
                        LOG.info("Last Cost Model:\n" + state.debug);
                    }
                    assert(parent_cost <= current_cost) :
                        attribute_key + ": Parent[" + parent.cost + "] <= Current[" + cost + "]" + "\n" +
                        "Rounded: Parent[" + MathUtil.roundToDecimals(parent.cost, 2) + "] <= Current[" + MathUtil.roundToDecimals(cost, 2) + "]";
                }
                
                // If this is a complete solution, then check whether if it is the best one that we have seen thus far
                synchronized (BranchAndBoundPartitioner.this) {
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
                        LOG.debug("Last Cost Model Info:\n " + this.cost_model.getLastDebugMessages());
                        LOG.info("New Best Solution: " + best_vertex.toString());
//                        for (int i = 0; i < ((TimeIntervalCostModel)this.cost_model).getIntevalCount(); i++) {
//                            System.err.println("Interval #" + i);
//                            System.err.println(((TimeIntervalCostModel)this.cost_model).getCostModel(i).getTxnPartitionAccessHistogram());
//                            System.err.println("================================================");
//                        }
//                        
//                        System.exit(1);
                    }
                } // SYNCHRONIZED
                
                // ----------------------------------------------
                // CONTINUE SEARCH TRAVERSAL
                // ----------------------------------------------
                
                // We now need to look to see whether we should continue down this path. Much like
                // selecting a new best solution, the following criteria must be met in order for
                // the partitioner to be allowed to continue down a search path:
                //  (1) It must not be a complete solution
                //  (2) The current catalog item must be a table (no procedures!)
                //  (3) The cost must be less than the current best solution cost
                //  (4) The cost must be less than the upper bounds limit
                // Or we can just say screw all that and keep going if the exhaustive flag is enabled
                if (this.hints.exhaustive_search || (
                    !complete_solution &&
                    is_table &&
                    cost < BranchAndBoundPartitioner.this.best_vertex.cost &&
                    cost < BranchAndBoundPartitioner.this.upper_bounds_vertex.cost)) {
                    
                    // IMPORTANT: If this is the last table in our traversal, then we need to switch over 
                    // to working Procedures->ProcParameters. We have to now calculate the list of attributes
                    // that we want to consider
                    if (is_last_table && this.hints.enable_procparameter_search) {
                        for (int i = idx + 1; i < this.num_elements; i++) {
                            String proc_key = this.traversal_attributes.get(i);
                            Procedure catalog_proc = CatalogKey.getFromKey(this.search_db, proc_key, Procedure.class);
                            LinkedList<String> attributes = BranchAndBoundPartitioner.generateProcParameterOrder(info, this.search_db, catalog_proc, hints);
                            
                            // We should have ProcParameter candidates!
                            assert(attributes.isEmpty() == false) : "No ProcParameter candidates: " + catalog_proc + "\n" + state;
                            this.traversal_attributes.get(proc_key).clear();
                            this.traversal_attributes.get(proc_key).addAll(attributes);
                        } // FOR
                    }

                    // We only traverse if this is a table. The ProcParameter selection is a simple greedy algorithm
                    this.traverse(state, idx + 1);
                }
                
                if (this.halt_search) break;
            } // FOR

            // ProcParameter Traversal
            if (!is_table && !this.halt_search) {
                assert(local_best_vertex != null) : "Missing local best vertex for " + current_key;

                // Set the partitioning ProcParameter in this Procedure to be the one that 
                // had the lowest cost in our search up above.
                Procedure current_proc = (Procedure)current;
                String best_key = local_best_vertex.get(current_key);
                ProcParameter current_param = CatalogKey.getFromKey(this.search_db, best_key, ProcParameter.class);
                assert(current_param != null);
                current_proc.setPartitionparameter(current_param.getIndex());
                
                // Is this necessary?
                this.cost_model.invalidateCache(current_proc);
                
                LOG.info(this.createLevelOutput(local_best_vertex, CatalogUtil.getDisplayName(current_param), spacer, false));
                
                // If there are more Procedures to partition and we have gone past our best cost
                // our upper bounds, then keep going... 
                if (!complete_solution &&
                    local_best_vertex.cost < best_vertex.cost &&
                    local_best_vertex.cost < upper_bounds_vertex.cost) {
                    this.traverse(local_best_vertex, idx + 1);
                }

                // Important: We will have to unset the Procedure partitioning parameter
                // if this was the last table and we are going back up the tree. This is because
                // unlike Tables, where we exclude queries using WorkloadFilters, we can't do
                // the same thing for ProcParameters
                // manually once we finish up with this table because the next passes will
                // overwrite the catalog that we saved off
                for (int i = idx; i < this.num_elements; i++) {
                    String proc_key = this.traversal_attributes.get(i);
                    Procedure catalog_proc = CatalogKey.getFromKey(this.search_db, proc_key, Procedure.class);
                    catalog_proc.setPartitionparameter(-1);
                } // FOR
            }
            this.backtrack_ctr++;
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