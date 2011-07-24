package org.voltdb.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class VerticalPartitionPlanner {
    private static final Logger LOG = Logger.getLogger(VerticalPartitionPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    
    /** convenience pointer to the database object in the catalog */
    private final Database catalog_db;

    /** Context object with planner-local information. */
    private final PlannerContext context;
    
    private final Map<Table, MaterializedViewInfo> vp_tables = new HashMap<Table, MaterializedViewInfo>();
    private final Map<Table, Collection<Column>> vp_columns = new HashMap<Table, Collection<Column>>();
    
    /**
     * @param context
     *            Information about context
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures
     */
    public VerticalPartitionPlanner(PlannerContext context, Database catalogDb) {
        this.catalog_db = catalogDb;
        this.context = context;
        this.vp_tables.putAll(CatalogUtil.getVerticallyPartitionedTables(catalogDb));
        for (Table catalog_tbl : this.vp_tables.keySet()) {
            Collection<Column> columns = CatalogUtil.getColumns(this.vp_tables.get(catalog_tbl).getGroupbycols());
            this.vp_columns.put(catalog_tbl, columns);
        } // FOR
    }
    
    /**
     * Process the given Statement and rewrite its query plan if it can take advantage of a 
     * vertical partitioning column
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public boolean process(Statement catalog_stmt) throws Exception {
        // Always skip if there are no vertically partitioned tables
        if (this.vp_tables.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": There are no vertically partitioned tables.");
            return (false);
        }
        
        // We can only work our magic on SELECTs
        QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
        if (qtype != QueryType.SELECT) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": QueryType is " + qtype + ".");
            return (false);
        }
        
        // Check whether this query references a table that has a vertical partition
        Collection<Table> tables = CollectionUtils.intersection(this.vp_tables.keySet(), CatalogUtil.getReferencedTables(catalog_stmt));
        if (tables.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": It does not reference a vertical partitioning table.");
            return (false);
        }

        // Now check whether the columns referenced doesn't include what the table is horizontally partitioned
        // but do include the columns that we have in our vertical partition
        Collection<Column> predicate_cols = CatalogUtil.getReferencedColumns(catalog_stmt);
        if (predicate_cols.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not reference any columns in its predicate.");
            return (false);
        }
        Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
        assert(output_cols.isEmpty() == false);
        Set<Table> candidates = new HashSet<Table>();
        for (Table catalog_tbl : tables) {
            MaterializedViewInfo catalog_view = this.vp_tables.get(catalog_tbl);
            assert(catalog_view != null);
            Collection<Column> view_cols = CatalogUtil.getColumns(catalog_view.getGroupbycols());
            assert(view_cols.isEmpty() == false);
            Column partitioning_col = catalog_tbl.getPartitioncolumn();
            assert(partitioning_col != null);
            
            // The current vertical partition is valid for this query if all the following are true:
            //  (1) The partitioning_col is in output_cols
            //  (2) The partitioning_col is *not* in the predicate_cols
            //  (3) At least one of the vertical partition's columns is in predicate_cols 
            if (trace.get()) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("VerticalP", catalog_view.getName());
                m.put("Partitioning Col", partitioning_col.fullName());
                m.put("Output Cols", output_cols);
                m.put("Predicate Cols", predicate_cols);
                m.put("VerticalP Cols", view_cols);
                LOG.trace(String.format("Checking whether %s can use vertical partition for %s\n%s",
                                        catalog_stmt.fullName(), catalog_tbl.getName(), StringUtil.formatMaps(m)));
            }
            if (output_cols.contains(partitioning_col) && 
                predicate_cols.contains(partitioning_col) == false &&
                CollectionUtils.intersection(view_cols, predicate_cols).isEmpty() == false) {
                if (trace.get()) LOG.trace("Valid VP Candidate: " + catalog_tbl);
                candidates.add(catalog_tbl);
            }
        } // FOR
        if (candidates.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not have any valid vertical partitioning references.");
            return (false);
        }
        
        // I think we're all good. So let's rewrite some SQL!
        if (debug.get()) LOG.debug(String.format("Attempting to optimize %s query plan using vertical partitions: %s", catalog_stmt.fullName(), candidates));
        this.optimizeQueryPlan(catalog_stmt, candidates);
        
        return (true);
    }
    
    protected AbstractPlanNode optimizeQueryPlan(Statement catalog_stmt, Collection<Table> target_tbls) throws Exception {
        // FIXME: Only support one table for now
        assert(target_tbls.size() == 1);
        
        // FIXME: Only do multi-partition query plans for now
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        
        Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
        assert(output_cols.isEmpty() == false);
        Collection<Column> predicate_cols = CatalogUtil.getReferencedColumns(catalog_stmt);
        assert(predicate_cols.isEmpty() == false);
        
        // Create the set of all the columns that we need for this query
        // We will determine whether our vertical partition has all the data we need
        // If it does, then we can just replace the target table used in the ScanPlanNode 
        Collection<Column> all_stmt_cols = new HashSet<Column>();
        all_stmt_cols.addAll(output_cols);
        all_stmt_cols.addAll(predicate_cols);
        
        Collection<Column> all_view_cols = new HashSet<Column>();
        for (Table catalog_tbl : target_tbls) {
            all_view_cols.addAll(this.vp_columns.get(catalog_tbl));
        } // FOR
        assert(all_view_cols.isEmpty() == false);
        
        if (all_view_cols.containsAll(all_stmt_cols)) {
            for (Table catalog_tbl : target_tbls) {
                Collection<AbstractPlanNode> nodes = PlanNodeUtil.getPlanNodesReferencingTable(root, catalog_tbl);
                if (nodes.isEmpty()) {
                    LOG.error("No AbstractPlanNode for " + catalog_tbl + "\n" + PlanNodeUtil.debug(root));
                    throw new Exception("Failed to retrieve nodes that reference " + catalog_tbl);
                }
                
                for (AbstractPlanNode node : nodes) {
                    System.err.println(PlanNodeUtil.debugNode(node));
                } // FOR
                
                
            } // FOR (tbl)
            
            
        } else {
            
        }
        
        return (null);
    }
    
    private void applyVerticalPartition(AbstractScanPlanNode node, MaterializedViewInfo catalog_view) throws Exception {
        AbstractScanPlanNode new_node = null;
        PlannerContext context = PlannerContext.singleton();
        Table view_tbl = catalog_view.getDest();
        Index view_idx = CollectionUtil.getFirst(view_tbl.getIndexes());
        boolean use_index = false;
        
        // Check whether this the vertical partition's index has all the columns that we will need to
        // reference in the search key expressions
        if (view_idx != null) {
            Collection<Column> idx_cols = CatalogUtil.getColumns(view_idx.getColumns());
            Collection<AbstractExpression> exps = PlanNodeUtil.getExpressionsForPlanNode(node);
            Collection<Column> exp_cols = new HashSet<Column>();
            for (AbstractExpression exp : exps) {
                exp_cols.addAll(ExpressionUtil.getReferencedColumns(catalog_db, exp));
            } // FOR
            use_index = idx_cols.containsAll(exp_cols);
        }
        
        // IndexScan
        if (use_index) {
            new_node = new IndexScanPlanNode(context, PlanAssembler.getNextPlanNodeId());
            
            
        // SequentialScan
        } else {
            new_node = new SeqScanPlanNode(context, PlanAssembler.getNextPlanNodeId());
        }
        
    }
    
}
