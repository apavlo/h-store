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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
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
    
    /** convenience pointer to the database object in the catalog */
    final Database m_catalogDb;

    /** Context object with planner-local information. */
    final PlannerContext m_context;

    /** All the columns a plan node references */
    final Map<AbstractPlanNode, Set<Column>> planNodeColumns = new HashMap<AbstractPlanNode, Set<Column>>();
    /** All referenced columns for a given table */
    final Map<Table, SortedSet<Column>> tableColumns = new HashMap<Table, SortedSet<Column>>();

    final Map<Column, Set<Integer>> column_guid_xref = new HashMap<Column, Set<Integer>>();
    final Map<Integer, Column> guid_column_xref= new HashMap<Integer, Column>();
    
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
    
    protected void optimize(AbstractPlanNode root) {
//        if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(root));
        
        // If this contains a NestLoop, then we won't try to optimize it
        Set<NestLoopPlanNode> nest_nodes = PlanNodeUtil.getPlanNodes(root, NestLoopPlanNode.class);
        if (nest_nodes.isEmpty() == false) {
            LOG.debug("Skipping the optimization of a query plan node containing a NestLoopPlanNode");
            return;
        }
        
        this.populateTableNodeInfo(root);
        this.propagateProjections(root);
    }

    /**
     * Populates the two data structures with information on the planNodes and
     * Tables and their referenced columns
     */
    protected void populateTableNodeInfo(final AbstractPlanNode rootNode) {
        final boolean trace = LOG.isTraceEnabled();
        LOG.debug("Populating Table Information"); // \n" + PlanNodeUtil.debug(rootNode));
        
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
//                if (trace) LOG.trace(PlanNodeUtil.debugNode(element));
                // call function to build the data structure that maps all nodes to the columns they affect
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
            this.tableColumns.put(catalog_tbl, new TreeSet<Column>(
                new Comparator<Column>() {
                    public int compare(Column c0, Column c1) {
                        Integer i0 = c0.getIndex();
                        assert(i0 != null) : "Missing index for " + c0;
                        Integer i1 = c1.getIndex();
                        assert(i1 != null) : "Missing index for " + c1;
                        return (i0.compareTo(i1));
                    }
            }));
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
        
        if (debug) LOG.debug("Extracted " + exps.size() + " expressions for " + node);
        
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

    protected void propagateProjections(final AbstractPlanNode rootNode) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        final int total_depth = PlanNodeUtil.getDepth(rootNode);
        
        if (debug) LOG.debug("Inserting Projections into PlanNode Tree [total_depth=" + total_depth + "]");
//        if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(rootNode));
        
        // Set of "dirty" plannodes ids
        final Set<AbstractPlanNode> dirtyPlanNodes = new HashSet<AbstractPlanNode>();
        
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
                    Set<Table> tables = null; 
                    try {
                        tables = CatalogUtil.getReferencedTablesNonRecursive(m_catalogDb, scan_node);
                    } catch (Exception ex) {
                        LOG.fatal("Failed to get tables referenced in " + scan_node, ex);
                        System.exit(1);
                    }
                    if (tables.size() != 1) {
                        LOG.error(PlanNodeUtil.debugNode(scan_node));
                    }
                    assert(tables.size() == 1) : scan_node + ": " + tables;
                    Table catalog_tbl = CollectionUtil.getFirst(tables);

                    // Create new projection and add in all of the columns that our table will ever need
                    ProjectionPlanNode proj_node = new ProjectionPlanNode(m_context, PlanAssembler.getNextPlanNodeId());

                    // populate projection node with output columns
                    if (debug) LOG.debug("Table columns in propagateProjections: " + tableColumns.get(catalog_tbl));
                    for (Column catalog_col : tableColumns.get(catalog_tbl)) {
                        assert(column_guid_xref.isEmpty() == false) : "No PlanColumn GUID for " + CatalogUtil.getDisplayName(catalog_col);
                        Integer col_guid = CollectionUtil.getFirst(column_guid_xref.get(catalog_col));
                        assert(col_guid != null);
                        
                        PlanColumn planColumn = m_context.get(col_guid);
                        assert(planColumn != null);
                        proj_node.appendOutputColumn(planColumn);
                    } // FOR
                    if (trace) LOG.trace("New Projection Output Columns:\n" + PlanNodeUtil.debugNode(proj_node));

                    // Add projection inline to scan node  
                    element.addInlinePlanNode(proj_node);
                    assert(proj_node.isInline());
                    
                    // Then make sure that we update it's output columns to match the inline output
                    scan_node.m_outputColumns.clear();
                    scan_node.m_outputColumns.addAll(proj_node.m_outputColumns);
                    
                    // add element to the "dirty" list
                    dirtyPlanNodes.add(element);
                    if (debug) LOG.debug(String.format("Added inline %s with %d columns to leaf node %s", proj_node, proj_node.getOutputColumnCount(), scan_node));
                    
                // ---------------------------------------------------
                // JOIN
                // ---------------------------------------------------
                } else if (element instanceof AbstractJoinPlanNode) {
                    // There's always going to be two input tables. One is always going to come
                    // from a child node, while the second may come from a child node *or* directly from
                    // a table being scanned. Therefore, we need to first figure out the original size
                    // of the first input table and then use that to adjust the offsets of the new tables
                    
                    AbstractPlanNode outer_node = element.getChild(0);
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
                    for (int orig_idx = 0, cnt = outer_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
                        int col_guid = outer_orig_input_guids.get(orig_idx);
                        
                        // Find the new index of this same PlanColumn guid
                        int new_idx = outer_new_input_guids.indexOf(col_guid);
                        if (new_idx != -1) {
                            if (trace) LOG.trace(String.format("OUTER OFFSET %d => %d", orig_idx, new_idx));
                            assert(offset_xref.containsKey(orig_idx) == false) : orig_idx + " ==> " + offset_xref; 
                            offset_xref.put(orig_idx, new_idx);
                            new_output_guids.add(col_guid);
                        } else if (debug) {
                            PlanColumn pc = m_context.get(col_guid);
                            LOG.warn("Failed to find new offset for OUTER " + pc);
                        }
                    } // FOR
                    if (trace) LOG.trace("Original Outer Input GUIDs: " + outer_orig_input_guids);
                    if (trace) LOG.trace("New Outer Input GUIDs:      " + outer_new_input_guids);
                    assert(outer_new_input_guids.size() == offset_xref.size());

                    // For the inner table, we always have to offset ourselves based on the size
                    // of the new outer table
                    int offset = outer_new_input_guids.size();
                    
                    // Whether we are in a NestLoop join or not
                    boolean is_nestloop = false;
                    
                    AbstractPlanNode inner_node = null;
                    
                    // --------------------------------------------
                    // NEST LOOP
                    // --------------------------------------------
                    if (element.getChildCount() > 1) {
                        assert(element instanceof NestLoopPlanNode);
                        is_nestloop = true;
                        
                        inner_node = element.getChild(1);
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
                        
                    
                    // --------------------------------------------
                    // NEST LOOP INDEX
                    // --------------------------------------------
                    } else {
                        // Otherwise, just grab all of the columns for the target table in the inline scan
                        assert(element instanceof NestLoopIndexPlanNode);
                        IndexScanPlanNode idx_node = element.getInlinePlanNode(PlanNodeType.INDEXSCAN);
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
                            Integer col_guid = CollectionUtil.getFirst(column_guid_xref.get(catalog_col));
                            assert(col_guid != null) : CatalogUtil.getDisplayName(catalog_col);
                            new_output_guids.add(col_guid);
                        } // FOR
                    }
                    if (trace) LOG.trace("Output Xref Offsets:      " + offset_xref);
                    if (trace) LOG.trace("New Output Columns GUIDS: " + new_output_guids);
                   
                    // Get all of the AbstractExpression roots for this node
                    // Now fix the offsets for everyone
                    for (AbstractExpression exp : PlanNodeUtil.getExpressions(element)) {
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
                    element.m_outputColumns.clear();
                    element.m_outputColumns.addAll(new_output_guids);
                    for (int new_idx = 0, cnt = element.m_outputColumns.size(); new_idx < cnt; new_idx++) {
                        Integer col_guid = element.m_outputColumns.get(new_idx);
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
                            element.m_outputColumns.set(new_idx, new_pc.guid());
                        }
                        if (debug) LOG.debug(String.format("OUTPUT[%d] => %s", new_idx, m_context.get(element.m_outputColumns.get(new_idx))));
                    } // FOR
                    
                    // If the inner_node is inline (meaning it was a NestLoopIndex), then we need to also update
                    // its output columns to match our new ones
//                    if (inner_node.isInline()) {
//                        assert(inner_node instanceof IndexScanPlanNode);
//                        inner_node.setOutputColumns(element.m_outputColumns);
//                        if (trace) LOG.trace("Updated INNER inline " + inner_node + " output columns");
//                    }

//                    if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(rootNode));
//                    LOG.debug(PlanNodeUtil.debugNode(element));
                    
                    dirtyPlanNodes.add(element);
                    
                // ---------------------------------------------------
                // SEND/RECEIVE
                // ---------------------------------------------------
                } else if (element instanceof SendPlanNode || element instanceof ReceivePlanNode) {
                    // We really have one child here
                    assert(element.getChildCount() == 1) : element;
                    AbstractPlanNode child_node = element.getChild(0);
                    assert(child_node != null);
                    
                    if (dirtyPlanNodes.contains(child_node)) {
                        element.m_outputColumns.clear();
                        element.m_outputColumns.addAll(child_node.m_outputColumns);
                        dirtyPlanNodes.add(element);
                        if (debug) LOG.debug(String.format("Updated %s with %d output columns to match child node %s output", element, element.getOutputColumnCount(), child_node));
                    }
                }
            }
        }.traverse(rootNode);
        if (trace) LOG.trace("Finished propagateProjections");
//        if (debug) LOG.debug("Optimized PlanNodeTree:\n" + PlanNodeUtil.debug(rootNode));
    }

}
