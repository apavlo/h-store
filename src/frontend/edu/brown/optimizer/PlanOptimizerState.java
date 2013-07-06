package edu.brown.optimizer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class PlanOptimizerState {
    private static final Logger LOG = Logger.getLogger(PlanOptimizerState.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();

    /**
     * Database Catalog Object
     */
    public final Database catalog_db;

    /**
     * Context object with planner-local information.
     */
    public final PlannerContext plannerContext;

    /**
     * All the columns a plan node references
     */
    protected final Map<AbstractPlanNode, Set<Column>> planNodeColumns = new HashMap<AbstractPlanNode, Set<Column>>();

    /**
     * All referenced columns for a given table
     */
    public final Map<Table, SortedSet<Column>> tableColumns = new HashMap<Table, SortedSet<Column>>();

    /**
     * Mapping from Column -> Set<PlanColumnGUID>
     */
    public final Map<Column, Set<Integer>> column_guid_xref = new HashMap<Column, Set<Integer>>();

    /**
     * Mapping from PlanColumnGUID -> Column
     */
    public final Map<Integer, Column> guid_column_xref = new HashMap<Integer, Column>();

    /**
     * Maintain the original output PlanColumnnGUIDs per PlanNode so we can
     * figure out offsets
     */
    public final Map<AbstractPlanNode, List<Integer>> orig_node_output = new HashMap<AbstractPlanNode, List<Integer>>();

    /**
     * AbstractPlanNode -> TableNames
     */
    public final Map<AbstractPlanNode, Set<String>> join_tbl_mapping = new HashMap<AbstractPlanNode, Set<String>>();

    /**
     * AbstractJoinPlanNode to the order that it appears in the tree
     */
    public final SortedMap<Integer, AbstractJoinPlanNode> join_node_index = new TreeMap<Integer, AbstractJoinPlanNode>();

    /**
     * AbstractJoinPlanNode -> Output PlanColumnDisplayName -> Offset
     */
    public final Map<AbstractJoinPlanNode, Map<String, Integer>> join_outputs = new HashMap<AbstractJoinPlanNode, Map<String, Integer>>();

    // ------------------------------------------------------------
    // INTERNAL STATE
    // ------------------------------------------------------------

    /**
     * Set of PlanNodes that have been modified and thus are marked as dirty
     */
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
        if (trace.val)
            LOG.trace("Marking " + node + " as dirty");
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
        if (debug.val)
            LOG.debug(String.format("%s has %d dirty children", node, ctr));
        return (ctr > 0);
    }

    public void updateColumnInfo(AbstractPlanNode node) {
        // Clears the internal data structures that stores the column info
        if (debug.val)
            LOG.debug("Clearing internal state information");
        this.orig_node_output.clear();
        this.tableColumns.clear();
        this.column_guid_xref.clear();
        this.planNodeColumns.clear();
        this.guid_column_xref.clear();

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
        if (trace.val)
            LOG.trace(String.format("Added Column GUID Mapping: %s => %d", catalog_col.fullName(), guid));
    }

    protected void addPlanNodeColumn(AbstractPlanNode node, Column catalog_col) {
        if (this.planNodeColumns.containsKey(node) == false) {
            this.planNodeColumns.put(node, new HashSet<Column>());
        }
        if (trace.val)
            LOG.trace(String.format("Referenced Columns %s -> %s", node, catalog_col));
        this.planNodeColumns.get(node).add(catalog_col);
    }

    public Collection<Column> getPlanNodeColumns(AbstractPlanNode node) {
        return this.planNodeColumns.get(node);
    }

    // ------------------------------------------------------------
    // DEBUG
    // ------------------------------------------------------------

    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();

        m.put("PlanNode Columns", this.planNodeColumns);
        m.put("PlanNode Hash", Arrays.toString(CollectionUtil.hashCode(this.planNodeColumns.keySet())));
        m.put("Table Columns", this.tableColumns);
        m.put("Column -> PlanColumnGUID", this.column_guid_xref);
        m.put("PlanColumnGUID -> Column", this.guid_column_xref);
        m.put("Original Output PlanColumnGUIDs", this.orig_node_output);
        m.put("JoinPlanNode Tables", this.join_tbl_mapping);
        m.put("JoinPlanNode Depths", this.join_node_index);
        m.put("JoinPlanNode Output", this.join_outputs);

        return (StringUtil.formatMaps(m));
    }
}
