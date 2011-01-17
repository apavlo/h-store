package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanColumn.Storage;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 * @author sw47
 */
public class PlanOptimizer {
    private static final Logger LOG = Logger.getLogger(PlanOptimizer.class);

    protected static final PlanNodeType TO_IGNORE[] = {
        PlanNodeType.AGGREGATE,
        PlanNodeType.NESTLOOP,
    };
    
    /**
     * 
     */
    private static final Comparator<Column> COLUMN_COMPARATOR = new Comparator<Column>() {
        public int compare(Column c0, Column c1) {
            Integer i0 = c0.getIndex();
            assert(i0 != null) : "Missing index for " + c0;
            Integer i1 = c1.getIndex();
            assert(i1 != null) : "Missing index for " + c1;
            return (i0.compareTo(i1));
        }
    };

    
    /** convenience pointer to the database object in the catalog */
    private final Database m_catalogDb;

    /** Context object with planner-local information. */
    private final PlannerContext m_context;
    
    private String sql;
    
    // Set of PlanNodes that have been modified and thus are marked as dirty
    protected final Set<AbstractPlanNode> dirtyPlanNodes = new HashSet<AbstractPlanNode>();

    // All the columns a plan node references
    protected final Map<AbstractPlanNode, Set<Column>> planNodeColumns = new HashMap<AbstractPlanNode, Set<Column>>();

    // All referenced columns for a given table
    protected final Map<Table, SortedSet<Column>> tableColumns = new HashMap<Table, SortedSet<Column>>();
    
    // Mapping from Column -> Set<PlanColumnGUID>
    protected final Map<Column, Set<Integer>> column_guid_xref = new HashMap<Column, Set<Integer>>();
    
    // Mapping from PlanColumnGUID -> Column
    protected final Map<Integer, Column> guid_column_xref= new HashMap<Integer, Column>();
    
    // Maintain the old output columns per PlanNode so we can figure out offsets
    final Map<AbstractPlanNode, List<Integer>> orig_node_output = new HashMap<AbstractPlanNode, List<Integer>>();
    
    /**
     * @param context
     *            Information about context
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures
     */
    public PlanOptimizer(PlannerContext context, Database catalogDb) {
        m_catalogDb = catalogDb;
        m_context = context;
    }

    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    private void markDirty(AbstractPlanNode node) {
        if (LOG.isTraceEnabled()) LOG.trace("Marking " + node + " as dirty");
        this.dirtyPlanNodes.add(node);
    }
    private boolean isDirty(AbstractPlanNode node) {
        return (this.dirtyPlanNodes.contains(node));
    }
    private boolean areChildrenDirty(AbstractPlanNode node) {
        int ctr = 0;
        for (AbstractPlanNode child_node : node.getChildren()) {
            if (this.isDirty(child_node)) ctr++;
        }
        if (LOG.isDebugEnabled()) LOG.debug(String.format("%s has %d dirty children", node, ctr));
        return (ctr > 0);
    }

    protected Table getTableByName(String tableName) {
        for (Table t : tableColumns.keySet()) {
            if (t.getName().equals(tableName)) {
                return t;
            }
        }
        // table does not exist
        return null;
    }
    protected void addTableColumn(Column catalog_col) {
        Table catalog_tbl = catalog_col.getParent();
        if (this.tableColumns.containsKey(catalog_tbl) == false) {
            this.tableColumns.put(catalog_tbl, new TreeSet<Column>(PlanOptimizer.COLUMN_COMPARATOR));
        }
        this.tableColumns.get(catalog_tbl).add(catalog_col);
    }
    protected void addColumnMapping(Column catalog_col, Integer guid) {
        if (this.column_guid_xref.containsKey(catalog_col) == false) {
            this.column_guid_xref.put(catalog_col, new HashSet<Integer>());
        }
        this.column_guid_xref.get(catalog_col).add(guid);
        this.guid_column_xref.put(guid, catalog_col);
    }
    protected void addPlanNodeColumn(AbstractPlanNode node, Column catalog_col) {
        if (this.planNodeColumns.containsKey(node) == false) {
            this.planNodeColumns.put(node, new HashSet<Column>());
        }
        this.planNodeColumns.get(node).add(catalog_col);
    }

    // ----------------------------------------------------------------------------
    // OPTIMIZATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * 
     */
    protected void optimize(String sql, AbstractPlanNode root) {
        final boolean trace = LOG.isTraceEnabled();
        this.sql = sql;
//        if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(root));
        
        // Check if our tree contains anything that we want to ignore
        Set<PlanNodeType> types = PlanNodeUtil.getPlanNodeTypes(root);
        if (trace) LOG.trace("PlanNodeTypes: " + types);
        for (PlanNodeType t : TO_IGNORE) {
            if (types.contains(t)) {
                if (trace) LOG.trace(String.format("Tree rooted at %s contains %s. Skipping optimization...", root, t));
                return;
            }
        } // FOR
        
        if (trace) LOG.trace("Attempting to optimize: " + sql);
        
        this.populateTableNodeInfo(root);
        this.process(root);
    }

    /**
     * Populates the two data structures with information on the planNodes and
     * Tables and their referenced columns
     */
    protected void populateTableNodeInfo(final AbstractPlanNode rootNode) {
        final boolean trace = LOG.isTraceEnabled();
        if (trace) LOG.trace("Populating Table Information"); // \n" + PlanNodeUtil.debug(rootNode));

        // Traverse tree and build up our data structures that maps all nodes to the columns they affect
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                try {
                    extractColumnInfo(element, this.getDepth() == 0);
                } catch (Exception ex) {
                    // LOG.fatal(PlanNodeUtil.debug(rootNode));
                    LOG.fatal("Failed to extract column information for " + element, ex);
                    System.exit(1);
                }
            }
        }.traverse(rootNode);
    }

    /**
     * 
     * @param node
     * @param is_root
     * @throws Exception
     */
    protected void extractColumnInfo(AbstractPlanNode node, boolean is_root) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        // Store the original output column information per node
        this.orig_node_output.put(node, new ArrayList<Integer>(node.m_outputColumns));
        
        // Get all of the AbstractExpression roots for this node
        final Set<AbstractExpression> exps = PlanNodeUtil.getExpressions(node);
        
        // If this is the root node, then include the output columns
        if (is_root) {
            for (Integer col_guid : node.m_outputColumns) {
                PlanColumn col = m_context.get(col_guid);
                assert(col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null) exps.add(col.getExpression());
            } // FOR
        }
        
        if (trace) LOG.trace("Extracted " + exps.size() + " expressions for " + node);
        
        // Now go through our expressions and extract out the columns that are referenced
        for (AbstractExpression exp : exps) {
            for (Column catalog_col : CatalogUtil.getReferencedColumns(m_catalogDb, exp)) {
                if (trace) LOG.trace(node + " => " + CatalogUtil.getDisplayName(catalog_col));
                this.addTableColumn(catalog_col);
                this.addPlanNodeColumn(node, catalog_col);
            } // FOR
        } // FOR
        
        // Populate our map from Column objects to PlanColumn GUIDs
        for (Integer col_guid : node.m_outputColumns) {
            PlanColumn col = m_context.get(col_guid);
            assert(col != null) : "Invalid PlanColumn #" + col_guid;
            if (col.getExpression() != null) {
                Set<Column> catalog_cols = CatalogUtil.getReferencedColumns(m_catalogDb, col.getExpression());
                // If there is more than one column, then it's some sort of compound expression
                // So we don't want to include in our mapping
                if (catalog_cols.size() == 1) {
                    this.addColumnMapping(CollectionUtil.getFirst(catalog_cols), col_guid);
                }
            }
        } // FOR
        
    }
    
    
    /**
     * 
     * @param scan_node
     * @return
     */
    protected boolean addInlineProjection(final AbstractScanPlanNode scan_node) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        Set<Table> tables = CatalogUtil.getReferencedTablesNonRecursive(m_catalogDb, scan_node);
        if (tables.size() != 1) {
            LOG.error(PlanNodeUtil.debugNode(scan_node));
        }
        assert(tables.size() == 1) : scan_node + ": " + tables;
        Table catalog_tbl = CollectionUtil.getFirst(tables);

        Set<Column> output_columns = this.tableColumns.get(catalog_tbl);
        
        // Stop if there is no column information.
        // XXX: Is this a bad thing?
        if (output_columns == null) {
            if (trace) LOG.warn("No column information for " + catalog_tbl);
            return (false);
        // Only create the projection if the number of columns we need to output is less
        // then the total number of columns for the table
        } else if (output_columns.size() == catalog_tbl.getColumns().size()) {
            if (trace) LOG.warn("All columns needed in query. No need for inline projection on " + catalog_tbl);
            return (false);
        }
        
        // Create new projection and add in all of the columns that our table will ever need
        ProjectionPlanNode proj_node = new ProjectionPlanNode(m_context, PlanAssembler.getNextPlanNodeId());
        if (debug) LOG.debug(String.format("Adding inline Projection for %s with %d columns. Original table has %d columns", catalog_tbl.getName(), output_columns.size(), catalog_tbl.getColumns().size()));
        int idx = 0;
        for (Column catalog_col : output_columns) {
            Set<Integer> guids = column_guid_xref.get(catalog_col);
            assert(guids != null && guids.isEmpty() == false) : "No PlanColumn GUID for " + CatalogUtil.getDisplayName(catalog_col);
            Integer col_guid = CollectionUtil.getFirst(guids);
            PlanColumn orig_col = m_context.get(col_guid);
            assert(orig_col != null);

            // Always try make a new PlanColumn and update the TupleValueExpresion index
            // This ensures that we always get the ordering correct
            TupleValueExpression clone_exp = (TupleValueExpression)orig_col.getExpression().clone();
            clone_exp.setColumnIndex(idx);
            Storage storage = (catalog_tbl.getIsreplicated() ? Storage.kReplicated : Storage.kPartitioned);
            PlanColumn new_col = m_context.getPlanColumn(clone_exp, orig_col.displayName(), orig_col.getSortOrder(), storage);
            assert(new_col != null);
            proj_node.appendOutputColumn(new_col);
            this.addColumnMapping(catalog_col, new_col.guid());
            idx++;
        } // FOR
        if (trace) LOG.trace("New Projection Output Columns:\n" + PlanNodeUtil.debugNode(proj_node));

        // Add projection inline to scan node  
        scan_node.addInlinePlanNode(proj_node);
        assert(proj_node.isInline());
        
        // Then make sure that we update it's output columns to match the inline output
        scan_node.m_outputColumns.clear();
        scan_node.m_outputColumns.addAll(proj_node.m_outputColumns);
        
        // add element to the "dirty" list
        this.markDirty(scan_node);
        if (trace) LOG.trace(String.format("Added inline %s with %d columns to leaf node %s", proj_node, proj_node.getOutputColumnCount(), scan_node));
        return (true);
    }
    
    protected boolean updateDistinctColumns(DistinctPlanNode dist_node) {
        final boolean trace = LOG.isTraceEnabled();
        
        // We really have one child here
        assert(dist_node.getChildCount() == 1) : dist_node;
        AbstractPlanNode child_node = dist_node.getChild(0);
        assert(child_node != null);

        // Find the offset of our distinct column in our output. That will
        // tell us where to get the guid in the input table information
        int orig_guid = dist_node.getDistinctColumnGuid();
        PlanColumn orig_pc = m_context.get(orig_guid);
        assert(orig_pc != null);

        dist_node.setOutputColumns(child_node.m_outputColumns);
        
        PlanColumn new_pc = null;
        int new_idx = 0;
        for (Integer guid : dist_node.m_outputColumns) {
            PlanColumn pc = m_context.get(guid);
            assert(pc != null);
            if (pc.equals(orig_pc, true, true)) {
                if (trace) LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
                new_pc = pc;
                break;
            }
            new_idx++;
        } // FOR
        assert(new_pc != null);
        
        // Now we can update output columns and set the distinct column to be the guid
        dist_node.setDistinctColumnGuid(new_pc.guid());
        
        markDirty(dist_node);
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Updated %s with proper distinct column guid: ORIG[%d] => NEW[%d]", dist_node, orig_guid, new_pc.guid()));
        
        return (true);
    }
    
    
    protected boolean updateAggregateColumns(AggregatePlanNode agg_node) {
        final boolean trace = LOG.isTraceEnabled();
        
        // We really have one child here
        assert(agg_node.getChildCount() == 1) : agg_node;
        AbstractPlanNode child_node = agg_node.getChild(0);
        assert(child_node != null);

        agg_node.setOutputColumns(child_node.m_outputColumns);
        
        for (int i = 0, cnt = agg_node.getAggregateColumnGuids().size(); i < cnt; i++) {
            Integer orig_guid = agg_node.getAggregateColumnGuids().get(i);
            PlanColumn orig_pc = m_context.get(orig_guid);
            assert(orig_pc != null);
    
            PlanColumn new_pc = null;
            int new_idx = 0;
            for (Integer guid : agg_node.m_outputColumns) {
                PlanColumn pc = m_context.get(guid);
                assert(pc != null);
                if (pc.equals(orig_pc, true, true)) {
                    if (trace) LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
                    new_pc = pc;
                    break;
                }
                new_idx++;
            } // FOR
            assert(new_pc != null);
            agg_node.getAggregateColumnGuids().set(i, new_pc.guid());
        } // FOR
        
//        // Now we can update output columns and set the distinct column to be the guid
//        dist_node.setDistinctColumnGuid(new_pc.guid());
//        
//        markDirty(dist_node);
//        if (LOG.isDebugEnabled()) LOG.debug(String.format("Updated %s with proper distinct column guid: ORIG[%d] => NEW[%d]", dist_node, orig_guid, new_pc.guid()));
        
        return (true);
    }
    

    /**
     * 
     * @param join_node
     * @return
     */
    protected boolean updateJoinsColumns(AbstractJoinPlanNode join_node) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        // There's always going to be two input tables. One is always going to come
        // from a child node, while the second may come from a child node *or* directly from
        // a table being scanned. Therefore, we need to first figure out the original size
        // of the first input table and then use that to adjust the offsets of the new tables
        AbstractPlanNode outer_node = join_node.getChild(0);
        assert(outer_node != null);
        List<Integer> outer_new_input_guids = outer_node.m_outputColumns;
        if (debug) LOG.debug("Calculating OUTER offsets from child node: " + outer_node);
        
        // List of PlanColumn GUIDs for the new output list
        List<Integer> new_output_guids = new ArrayList<Integer>();
        
        // Go and build a map from original offsets to the new offsets that need to be stored 
        // for the TupleValueExpressions (and possible TupleAddressExpression)
        final Map<Integer, Integer> offset_xref = new HashMap<Integer, Integer>();
        List<Integer> outer_orig_input_guids = PlanOptimizer.this.orig_node_output.get(outer_node);
        assert(outer_orig_input_guids != null);
        StringBuilder sb = new StringBuilder();
        for (int orig_idx = 0, cnt = outer_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
            int orig_col_guid = outer_orig_input_guids.get(orig_idx);
            PlanColumn orig_pc = m_context.get(orig_col_guid);
            
            // Figure out what the new PlanColumn GUID is for this column
            // It may be the case that we need to make a new one because the underlying expession has the wrong offsets
            PlanColumn new_pc = null;
            Integer new_idx = null;
            
            // Find the new index of this same PlanColumn guid
            new_idx = outer_new_input_guids.indexOf(orig_col_guid);
            if (new_idx != -1) {
                new_pc = m_context.get(orig_col_guid);
                new_output_guids.add(orig_col_guid);
                if (debug) LOG.debug(String.format("OUTER OFFSET %d => %d", orig_idx, new_idx));
                
            // Check whether we even have this column. We'll compare everything but the Expression
            } else {
                new_idx = 0;
                for (Integer guid : outer_new_input_guids) {
                    PlanColumn pc = m_context.get(guid);
                    assert(pc != null);
                    if (pc.equals(orig_pc, true, true)) {
                        if (trace) LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", orig_idx, orig_pc, pc));
                        new_pc = pc;
                        break;
                    }
                    new_idx++;
                } // FOR
                
                // If we have this PlanColumn, then we need to clone it and set the new column index
                // Make sure that we replace update outer_new_input_guids
                if (new_pc != null) {
                    assert(new_idx != -1);
                    TupleValueExpression clone_exp = (TupleValueExpression)orig_pc.getExpression().clone();
                    clone_exp.setColumnIndex(new_idx);
                    PlanColumn new_col = m_context.getPlanColumn(clone_exp, orig_pc.displayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                    assert(new_col != null);
                    outer_new_input_guids.set(new_idx, new_col.guid());
                    new_output_guids.add(new_col.guid());
                    if (debug) LOG.debug(String.format("OUTER OFFSET %d => %d [new_guid=%d]", orig_idx, new_idx, new_col.guid()));
                } else {
                    new_idx = null;
                }
            }
            
            if (new_idx != null) {
                assert(offset_xref.containsKey(orig_idx) == false) : orig_idx + " ==> " + offset_xref; 
                offset_xref.put(orig_idx, new_idx);
            } else {
                String msg = String.format("[%02d] Failed to find new offset for OUTER %s", orig_idx, orig_pc);
                sb.append(msg).append("\n");
                if (debug) LOG.warn(msg);
            }
        } // FOR
        if (trace) LOG.trace("Original Outer Input GUIDs: " + outer_orig_input_guids);
        if (trace) LOG.trace("New Outer Input GUIDs:      " + outer_new_input_guids);
        if (outer_new_input_guids.size() != offset_xref.size()) {
            LOG.info("Outer Node: " + outer_node);
            
            String temp = "";
            for (int i = 0; i < outer_orig_input_guids.size(); i++) {
                PlanColumn pc = m_context.get(outer_orig_input_guids.get(i));
                temp += String.format("[%02d] %s\n", i, pc);
                temp += ExpressionUtil.debug(pc.getExpression()) + "\n--------\n";
            }
            temp += "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
            LOG.info("Original Outer Input GUIDs: " + outer_orig_input_guids + "\n" + temp);
            
            temp = "";
            for (int i = 0; i < outer_new_input_guids.size(); i++) {
                PlanColumn pc = m_context.get(outer_new_input_guids.get(i));
                temp += String.format("[%02d] %s\n", i, pc);
                temp += ExpressionUtil.debug(pc.getExpression()) + "\n--------\n";
            }
            LOG.info("New Outer Input GUIDs:      " + outer_new_input_guids + "\n" + temp);
            
            LOG.info("Output Xref Offsets:      " + offset_xref);
//            LOG.info("Trace Information:\n" + sb);
            LOG.error("Unexpected Query Plan\n" + sql + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getRoot(join_node)));
        }
        assert(outer_new_input_guids.size() == offset_xref.size()) : sql;

        // For the inner table, we always have to offset ourselves based on the size
        // of the new outer table
        int offset = outer_new_input_guids.size();
        
        // Whether we are in a NestLoop join or not
        boolean is_nestloop = false;
        
        AbstractPlanNode inner_node = null;
        
        // --------------------------------------------
        // NEST LOOP
        // --------------------------------------------
        if (join_node.getChildCount() > 1) {
            assert(join_node instanceof NestLoopPlanNode);
            is_nestloop = true;
            
            inner_node = join_node.getChild(1);
            if (debug) LOG.debug("Calculating INNER offsets from child node: " + inner_node);
            
            List<Integer> inner_orig_input_guids = PlanOptimizer.this.orig_node_output.get(inner_node);
            assert(inner_orig_input_guids != null);
            List<Integer> inner_new_input_guids = inner_node.m_outputColumns;
            
            for (int orig_idx = 0, cnt = inner_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
                int col_guid = inner_orig_input_guids.get(orig_idx);
                
                // Find the new index of this same PlanColumn guid
                int new_idx = inner_new_input_guids.indexOf(col_guid);
                if (new_idx != -1) {
                    int offset_orig_idx = outer_orig_input_guids.size() + orig_idx;
                    int offset_new_idx = offset + new_idx;
                    if (trace) LOG.trace(String.format("INNER NODE OFFSET %d => %d", offset_orig_idx, offset_new_idx));
                    assert(offset_xref.containsKey(offset_orig_idx) == false) : orig_idx + " ==> " + offset_xref;
                    offset_xref.put(offset_orig_idx, offset_new_idx);
                    new_output_guids.add(col_guid);
                } else {
                    PlanColumn pc = m_context.get(col_guid);
                    LOG.warn("Failed to find new offset for INNER " + pc);
                }
            } // FOR
            if (trace) LOG.trace("Original Inner Input GUIDs: " + inner_orig_input_guids);
            if (trace) LOG.trace("New Inner Input GUIDs:      " + inner_new_input_guids);
        
        // ---------------------------------------------------
        // NEST LOOP INDEX
        // ---------------------------------------------------
        } else {
            // Otherwise, just grab all of the columns for the target table in the inline scan
            assert(join_node instanceof NestLoopIndexPlanNode);
            IndexScanPlanNode idx_node = join_node.getInlinePlanNode(PlanNodeType.INDEXSCAN);
            assert(idx_node != null);
            inner_node = idx_node;
            
            Table catalog_tbl = null;
            try {
                catalog_tbl = CollectionUtil.getFirst(CatalogUtil.getReferencedTablesNonRecursive(m_catalogDb, idx_node));
            } catch (Exception ex) {
                LOG.fatal(ex);
                System.exit(1);
            }
            assert(catalog_tbl != null);
            if (debug) LOG.debug("Calculating INNER offsets from INLINE Scan: " + catalog_tbl);
            
            for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
                int i = catalog_col.getIndex();
                int offset_orig_idx = outer_orig_input_guids.size() + i;
                int offset_new_idx = offset + i;
                if (trace) LOG.trace(String.format("INNER INLINE OFFSET %d => %d", offset_orig_idx, offset_new_idx));
                offset_xref.put(offset_orig_idx, offset_new_idx);
                
                // Since we're going in order, we know what column is at this position.
                // That means we can grab the catalog object and convert it to a PlanColumn GUID
                // Always try make a new PlanColumn and update the TupleValueExpresion index
                // This ensures that we always get the ordering correct
                Integer col_guid = CollectionUtil.getFirst(column_guid_xref.get(catalog_col));
                PlanColumn orig_col = m_context.get(col_guid);
                assert(orig_col != null);
                TupleValueExpression clone_exp = (TupleValueExpression)orig_col.getExpression().clone();
                clone_exp.setColumnIndex(offset_new_idx);
                Storage storage = (catalog_tbl.getIsreplicated() ? Storage.kReplicated : Storage.kPartitioned);
                PlanColumn new_col = m_context.getPlanColumn(clone_exp, orig_col.displayName(), orig_col.getSortOrder(), storage);
                assert(new_col != null);
                new_output_guids.add(new_col.guid());
            } // FOR
        }
        if (trace) LOG.trace("Output Xref Offsets:      " + offset_xref);
        if (trace) LOG.trace("New Output Columns GUIDS: " + new_output_guids);
       
        // Get all of the AbstractExpression roots for this node
        // Now fix the offsets for everyone
        for (AbstractExpression exp : PlanNodeUtil.getExpressions(join_node)) {
            new ExpressionTreeWalker() {
                @Override
                protected void callback(AbstractExpression exp_element) {
                    if (exp_element instanceof TupleValueExpression) {
                        TupleValueExpression tv_exp = (TupleValueExpression)exp_element;
                        int orig_idx = tv_exp.getColumnIndex();
                        
                        // If we're in a NestLoopJoin (and not a NestLoopIndexJoin), then what we need to
                        // do is take the original offset (which points to a column in the original inner input), and s
                        
                        Integer new_idx = offset_xref.get(orig_idx);
                        if (new_idx == null) LOG.debug(m_context.debug());
                        assert(new_idx != null) : "Missing Offset: " + ExpressionUtil.debug(tv_exp);
                        if (debug) LOG.debug(String.format("Changing %s.%s [%d ==> %d]", tv_exp.getTableName(), tv_exp.getColumnName(), orig_idx, new_idx));
                        if (orig_idx != new_idx) {
                            tv_exp.setColumnIndex(new_idx);    
                        }
                        
                    }
                }
            }.traverse(exp);
        }
        
        // Then update the output columns to reflect the change
        join_node.setOutputColumns(new_output_guids);
        for (int new_idx = 0, cnt = join_node.m_outputColumns.size(); new_idx < cnt; new_idx++) {
            Integer col_guid = join_node.m_outputColumns.get(new_idx);
            PlanColumn pc = m_context.get(col_guid);
            
            // Look at what our offset used versus what it is needs to be
            // If it's different, then we need to make a new PlanColumn.
            // Note that we will clone TupleValueExpression so that we do not mess with
            // other PlanColumns
            // Assume that AbstractExpression is always a TupleValueExpression
            TupleValueExpression tv_exp = (TupleValueExpression)pc.getExpression();
            assert(tv_exp != null);
            int orig_idx = tv_exp.getColumnIndex();
            // assert(new_idx == offset_xref.get(orig_idx)) : String.format("Offset Mismatch [orig_idx=%d] =>  [%d] != [%d]:\noffset_xref = %s\n%s", orig_idx, new_idx, offset_xref.get(orig_idx), offset_xref, PlanNodeUtil.debugNode(element));
            if (orig_idx != new_idx) {
                TupleValueExpression clone_exp = null;
                try {
                    clone_exp = (TupleValueExpression)tv_exp.clone();
                } catch (Exception ex) {
                    LOG.fatal(ex);
                    System.exit(1);
                }
                assert(clone_exp != null);
                clone_exp.setColumnIndex(new_idx);
                PlanColumn new_pc = m_context.getPlanColumn(clone_exp, pc.displayName(), pc.getSortOrder(), pc.getStorage());
                assert(new_pc != null);
                join_node.m_outputColumns.set(new_idx, new_pc.guid());
            }
            if (trace) LOG.trace(String.format("OUTPUT[%d] => %s", new_idx, m_context.get(join_node.m_outputColumns.get(new_idx))));
        } // FOR
        
        // If the inner_node is inline (meaning it was a NestLoopIndex), then we need to also update
        // its output columns to match our new ones
//        if (inner_node.isInline()) {
//            assert(inner_node instanceof IndexScanPlanNode);
//            inner_node.setOutputColumns(element.m_outputColumns);
//            if (trace) LOG.trace("Updated INNER inline " + inner_node + " output columns");
//        }

//        if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(rootNode));
//        LOG.debug(PlanNodeUtil.debugNode(element));
        
        this.markDirty(join_node);

        return (true);
    }

    /**
     * 
     * @param rootNode
     */
    protected void process(final AbstractPlanNode rootNode) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        final int total_depth = PlanNodeUtil.getDepth(rootNode);
        
        if (debug) LOG.debug("Optimizing PlanNode Tree [total_depth=" + total_depth + "]");
//        if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(rootNode));
        
        // walk the tree a second time to add inline projection to the
        // bottom most scan node and propagate that
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (trace) {
                    LOG.trace("Current Node: " + element);
                    LOG.trace("Current Depth: " + this.getDepth());
                }
        
                // ---------------------------------------------------
                // LEAF SCANS
                // ---------------------------------------------------
                if (element.getChildCount() == 0 && element instanceof AbstractScanPlanNode) {
                    AbstractScanPlanNode scan_node = (AbstractScanPlanNode)element;
                    try {
                        if (addInlineProjection(scan_node) == false) {
                            this.stop();
                            return;
                        }
                    } catch (Exception ex) {
                        LOG.fatal("Failed to add inline projection to " + element, ex);
                        System.exit(1);
                    }
                    
                // ---------------------------------------------------
                // JOIN
                // ---------------------------------------------------
                } else if (element instanceof AbstractJoinPlanNode) {
                    AbstractJoinPlanNode join_node = (AbstractJoinPlanNode)element;
                    try {
                        if (areChildrenDirty(join_node) && updateJoinsColumns(join_node) == false) {
                            this.stop();
                            return;
                        }
                    } catch (Exception ex) {
                        LOG.fatal("Failed to update join columns in " + element, ex);
                        System.exit(1);
                    }

                // ---------------------------------------------------
                // DISTINCT
                // ---------------------------------------------------
                } else if (element instanceof DistinctPlanNode) {
                    DistinctPlanNode dst_node = (DistinctPlanNode)element;
                    
                    if (areChildrenDirty(dst_node) && updateDistinctColumns(dst_node) == false) {
                        this.stop();
                        return;
                    }
                    
                // ---------------------------------------------------
                // SEND/RECEIVE/LIMIT
                // ---------------------------------------------------
                } else if (element instanceof SendPlanNode || element instanceof ReceivePlanNode) {
                    // We really have one child here
                    assert(element.getChildCount() == 1) : element;
                    AbstractPlanNode child_node = element.getChild(0);
                    assert(child_node != null);
                    
                    if (dirtyPlanNodes.contains(child_node)) {
                        element.setOutputColumns(child_node.m_outputColumns);
                        markDirty(element);
                        if (debug) LOG.debug(String.format("Updated %s with %d output columns to match child node %s output", element, element.getOutputColumnCount(), child_node));
                    }
                }
                return;
            }
        }.traverse(rootNode);
        if (debug) LOG.trace("Finished Optimizations!");
//        if (debug) LOG.debug("Optimized PlanNodeTree:\n" + PlanNodeUtil.debug(rootNode));
    }

}