package edu.brown.optimizer;

import java.util.*;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;

import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class PlanOptimizerState {
    private static final Logger LOG = Logger.getLogger(PlanOptimizerState.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());

    /** convenience pointer to the database object in the catalog */
    public final Database catalog_db;

    /** Context object with planner-local information. */
    public final PlannerContext plannerContext;
    
    /** All the columns a plan node references */
    public final Map<AbstractPlanNode, Set<Column>> planNodeColumns = new HashMap<AbstractPlanNode, Set<Column>>();

    /** All referenced columns for a given table */
    public final Map<Table, SortedSet<Column>> tableColumns = new HashMap<Table, SortedSet<Column>>();

    /** Mapping from Column -> Set<PlanColumnGUID> */
    public final Map<Column, Set<Integer>> column_guid_xref = new HashMap<Column, Set<Integer>>();

    /** Mapping from PlanColumnGUID -> Column */
    public final Map<Integer, Column> guid_column_xref = new HashMap<Integer, Column>();

    /** Maintain the old output columns per PlanNode so we can figure out offsets */
    public final Map<AbstractPlanNode, List<Integer>> orig_node_output = new HashMap<AbstractPlanNode, List<Integer>>();

    /**
     * 
     */
    public final Map<Integer, Set<String>> join_tbl_mapping = new HashMap<Integer, Set<String>>();
    
    /**
     * 
     */
    public final List<ProjectionPlanNode> projection_plan_nodes = new ArrayList<ProjectionPlanNode>();
    
    /**
     * 
     */
    public final Set<String> ref_join_tbls = new HashSet<String>();
    
    /**
     * 
     */
    public final SortedMap<Integer, AbstractPlanNode> join_node_index = new TreeMap<Integer, AbstractPlanNode>();
    
    /**
     * 
     */
    public final Map<AbstractPlanNode, Map<String, Integer>> join_outputs = new HashMap<AbstractPlanNode, Map<String,Integer>>();
    
    // ------------------------------------------------------------
    // INTERNAL STATE 
    // ------------------------------------------------------------
    
    /**
     * Set of PlanNodes that have been modified and thus are marked as dirty
     * */
    private final Set<AbstractPlanNode> dirtyPlanNodes = new HashSet<AbstractPlanNode>();
    
    // ------------------------------------------------------------
    // CONSTRUCTOR
    // ------------------------------------------------------------
    
    public PlanOptimizerState(Database catalog_db, PlannerContext context) {
        this.catalog_db = catalog_db;
        this.plannerContext = context;
    }
    
    // ------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------
    
    public void clearDirtyNodes() {
        this.dirtyPlanNodes.clear();
    }
    
    public void markDirty(AbstractPlanNode node) {
        if (debug.get())
            LOG.debug("Marking " + node + " as dirty");
        this.dirtyPlanNodes.add(node);
    }
    
    public boolean hasDirtyNodes() {
        return (this.dirtyPlanNodes.isEmpty() == false);
    }
    
    public boolean isDirty(AbstractPlanNode node) {
        return (this.dirtyPlanNodes.contains(node));
    }

    public boolean areChildrenDirty(AbstractPlanNode node) {
        int ctr = 0;
        for (AbstractPlanNode child_node : node.getChildren()) {
            if (this.isDirty(child_node))
                ctr++;
        }
        if (debug.get())
            LOG.debug(String.format("%s has %d dirty children", node, ctr));
        return (ctr > 0);
    }
    
    /** clears the internal data structures that stores the column info **/
    protected void clearColumnInfo() {
        orig_node_output.clear();
        tableColumns.clear();
        column_guid_xref.clear();
        planNodeColumns.clear();
        guid_column_xref.clear();
    }
    
    public void updateColumnInfo(AbstractPlanNode node) {
        this.clearColumnInfo();
        PlanOptimizerUtil.populateTableNodeInfo(this, node);
    }
    
    protected void addTableColumn(Column catalog_col) {
        Table catalog_tbl = catalog_col.getParent();
        if (this.tableColumns.containsKey(catalog_tbl) == false) {
            this.tableColumns.put(catalog_tbl, new TreeSet<Column>(PlanOptimizer.COLUMN_COMPARATOR));
        }
        this.tableColumns.get(catalog_tbl).add(catalog_col);
    }

    public void addColumnMapping(Column catalog_col, Integer guid) {
        if (this.column_guid_xref.containsKey(catalog_col) == false) {
            this.column_guid_xref.put(catalog_col, new HashSet<Integer>());
        }
        this.column_guid_xref.get(catalog_col).add(guid);
        this.guid_column_xref.put(guid, catalog_col);
        if (trace.get()) 
            LOG.trace(String.format("Added Column GUID Mapping: %s => %d", catalog_col.fullName(), guid));
    }

    protected void addPlanNodeColumn(AbstractPlanNode node, Column catalog_col) {
        if (this.planNodeColumns.containsKey(node) == false) {
            this.planNodeColumns.put(node, new HashSet<Column>());
        }
        this.planNodeColumns.get(node).add(catalog_col);
    }
    
}
