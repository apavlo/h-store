package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
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
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.ExpressionType;
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

    protected static final PlanNodeType TO_IGNORE[] = { PlanNodeType.AGGREGATE, PlanNodeType.NESTLOOP, };

    /**
     * 
     */
    private static final Comparator<Column> COLUMN_COMPARATOR = new Comparator<Column>() {
        public int compare(Column c0, Column c1) {
            Integer i0 = c0.getIndex();
            assert (i0 != null) : "Missing index for " + c0;
            Integer i1 = c1.getIndex();
            assert (i1 != null) : "Missing index for " + c1;
            return (i0.compareTo(i1));
        }
    };

    /** DWU hack to cut out the top most projection **/
    private AbstractPlanNode new_root;

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
    protected final Map<Integer, Column> guid_column_xref = new HashMap<Integer, Column>();

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
        if (LOG.isTraceEnabled())
            LOG.trace("Marking " + node + " as dirty");
        this.dirtyPlanNodes.add(node);
    }

    private boolean isDirty(AbstractPlanNode node) {
        return (this.dirtyPlanNodes.contains(node));
    }

    private boolean areChildrenDirty(AbstractPlanNode node) {
        int ctr = 0;
        for (AbstractPlanNode child_node : node.getChildren()) {
            if (this.isDirty(child_node))
                ctr++;
        }
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("%s has %d dirty children", node, ctr));
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

    protected void addProjectionColumn(Set<PlanColumn> proj_columns, Integer col_id) {
        PlanColumn new_column = m_context.get(col_id);
        boolean exists = false;
        for (PlanColumn plan_col : proj_columns) {
            if (new_column.displayName().equals(plan_col.displayName())) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            proj_columns.add(new_column);
        }
    }

    protected void addProjectionColumn(Set<PlanColumn> proj_columns, Integer col_id, int offset) throws CloneNotSupportedException {
        PlanColumn orig_pc = m_context.get(col_id);
        assert (orig_pc.getExpression() instanceof TupleValueExpression);
        TupleValueExpression orig_tv_exp = (TupleValueExpression)orig_pc.getExpression();
        TupleValueExpression clone_tv_exp = (TupleValueExpression)orig_tv_exp.clone();
        clone_tv_exp.setColumnIndex(offset);
        PlanColumn new_col = m_context.getPlanColumn(clone_tv_exp, orig_pc.displayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
        boolean exists = false;
        for (PlanColumn plan_col : proj_columns) {
            if (new_col.displayName().equals(plan_col.displayName())) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            proj_columns.add(new_col);
        }
    }

    // ----------------------------------------------------------------------------
    // OPTIMIZATION METHODS
    // ----------------------------------------------------------------------------

    public AbstractPlanNode getNewRoot() {
        return new_root;
    }

    /**
     * Main entry point for the PlanOptimizer
     */
    public void optimize(String sql, AbstractPlanNode root) {
        final boolean trace = LOG.isTraceEnabled();
        this.sql = sql;
        // if (debug) LOG.debug("PlanNodeTree:\n" + PlanNodeUtil.debug(root));

        // check to see if the join nodes have the wrong offsets. If so fix them and propagate them.
        new PlanNodeTreeWalker(false) {
			
			@Override
			protected void callback(AbstractPlanNode element) {
				if (element instanceof NestLoopPlanNode || element instanceof NestLoopIndexPlanNode) {
					// make sure the column reference offsets of the output column are consecutive
					int offset_cnt = 0;
					for (int col_guid : element.m_outputColumns) {
						PlanColumn pc_col = m_context.get(col_guid);
						if (pc_col.getExpression().getExpressionType() == ExpressionType.VALUE_TUPLE) {
							// if the offset doesn't match, set it to be the correct offset
							if (((TupleValueExpression)pc_col.getExpression()).getColumnIndex() != offset_cnt) {
								((TupleValueExpression)pc_col.getExpression()).setColumnIndex(offset_cnt);
							}
							offset_cnt++;
						}
					}
				}
			}
		}.traverse(root);
        
        
        // Check if our tree contains anything that we want to ignore
        Set<PlanNodeType> types = PlanNodeUtil.getPlanNodeTypes(root);
        if (trace)
            LOG.trace("PlanNodeTypes: " + types);
        for (PlanNodeType t : TO_IGNORE) {
            if (types.contains(t)) {
                if (trace)
                    LOG.trace(String.format("Tree rooted at %s contains %s. Skipping optimization...", root, t));
                return;
            }
        } // FOR

        if (trace)
            LOG.trace("Attempting to optimize: " + sql);

        this.populateTableNodeInfo(root);
        this.process(root);
    }

    /**
     * Populates the two data structures with information on the planNodes and
     * Tables and their referenced columns
     */
    protected void populateTableNodeInfo(final AbstractPlanNode rootNode) {
        final boolean trace = LOG.isTraceEnabled();
        if (trace)
            LOG.trace("Populating Table Information"); // \n" +
        // PlanNodeUtil.debug(rootNode));

        // Traverse tree and build up our data structures that maps all nodes to
        // the columns they affect
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

    /** clears the internal data structures that stores the column info **/
    protected void clearColumnInfo() {
        orig_node_output.clear();
        tableColumns.clear();
        column_guid_xref.clear();
        planNodeColumns.clear();
        guid_column_xref.clear();
    }
    
    /**
     * @param node
     * @param is_root
     * @throws Exception
     */
    protected void extractColumnInfo(AbstractPlanNode node, boolean is_root) throws Exception {
        //System.out.println("current node type: " + node.getPlanNodeType() + " id: " + node.getPlanNodeId());
        
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();

        // Store the original output column information per node
        this.orig_node_output.put(node, new ArrayList<Integer>(node.m_outputColumns));

        // Get all of the AbstractExpression roots for this node
        final Set<AbstractExpression> exps = PlanNodeUtil.getExpressions(node);
        // If this is the root node, then include the output columns + also include output columns if its a projection or limit node
        if (is_root || node instanceof ProjectionPlanNode | node instanceof LimitPlanNode) {
            for (Integer col_guid : node.m_outputColumns) {
                PlanColumn col = m_context.get(col_guid);
                assert (col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null) {
                    exps.add(col.getExpression());
                    // root_column_expressions.addAll(ExpressionUtil.getExpressions(col.getExpression(),
                    // TupleValueExpression.class));
                }
            } // FOR
        }

        // PlanNode specific extractions

        // ---------------------------------------------------
        // AGGREGATE
        // ---------------------------------------------------
        if (node instanceof AggregatePlanNode) {
            AggregatePlanNode agg_node = (AggregatePlanNode) node;
            for (Integer col_guid : agg_node.getAggregateColumnGuids()) {
                PlanColumn col = m_context.get(col_guid);
                assert (col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null)
                    exps.add(col.getExpression());
            } // FOR
            for (Integer col_guid : agg_node.getGroupByColumnIds()) {
                PlanColumn col = m_context.get(col_guid);
                assert (col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null)
                    exps.add(col.getExpression());
            } // FOR
            // ---------------------------------------------------
            // ORDER BY
            // ---------------------------------------------------
        } else if (node instanceof OrderByPlanNode) {
            OrderByPlanNode orby_node = (OrderByPlanNode) node;
            for (Integer col_guid : orby_node.getSortColumnGuids()) {
                PlanColumn col = m_context.get(col_guid);
                assert (col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null)
                    exps.add(col.getExpression());
            } // FOR
        }

        if (trace)
            LOG.trace("Extracted " + exps.size() + " expressions for " + node);

        // Now go through our expressions and extract out the columns that are
        // referenced
        for (AbstractExpression exp : exps) {
            for (Column catalog_col : CatalogUtil.getReferencedColumns(m_catalogDb, exp)) {
                if (trace)
                    LOG.trace(node + " => " + CatalogUtil.getDisplayName(catalog_col));
                this.addTableColumn(catalog_col);
                this.addPlanNodeColumn(node, catalog_col);
            } // FOR
        } // FOR

        // Populate our map from Column objects to PlanColumn GUIDs
        for (Integer col_guid : node.m_outputColumns) {
            PlanColumn col = m_context.get(col_guid);
            assert (col != null) : "Invalid PlanColumn #" + col_guid;
            if (col.getExpression() != null) {
                Set<Column> catalog_cols = CatalogUtil.getReferencedColumns(m_catalogDb, col.getExpression());
                // If there is more than one column, then it's some sort of
                // compound expression
                // So we don't want to include in our mapping
                if (catalog_cols.size() == 1) {
                    this.addColumnMapping(CollectionUtil.getFirst(catalog_cols), col_guid);
                }
            }
        } // FOR

    }

    /**
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
        assert (tables.size() == 1) : scan_node + ": " + tables;
        Table catalog_tbl = CollectionUtil.getFirst(tables);

        Set<Column> output_columns = this.tableColumns.get(catalog_tbl);

        // Stop if there is no column information.
        // XXX: Is this a bad thing?
        if (output_columns == null) {
            if (trace)
                LOG.warn("No column information for " + catalog_tbl);
            return (false);
            // Only create the projection if the number of columns we need to
            // output is less
            // then the total number of columns for the table
        } else if (output_columns.size() == catalog_tbl.getColumns().size()) {
            if (trace)
                LOG.warn("All columns needed in query. No need for inline projection on " + catalog_tbl);
            return (false);
        }

        // Create new projection and add in all of the columns that our table
        // will ever need
        ProjectionPlanNode proj_node = new ProjectionPlanNode(m_context, PlanAssembler.getNextPlanNodeId());
        if (debug)
            LOG.debug(String.format("Adding inline Projection for %s with %d columns. Original table has %d columns", catalog_tbl.getName(), output_columns.size(), catalog_tbl.getColumns().size()));
        int idx = 0;
        for (Column catalog_col : output_columns) {
            // Get the old GUID from the original output columns
            int orig_idx = catalog_col.getIndex();
            int orig_guid = scan_node.getOutputColumnGUID(orig_idx);
            PlanColumn orig_col = m_context.get(orig_guid);
            assert (orig_col != null);
            proj_node.appendOutputColumn(orig_col);

            // Set<Integer> guids = column_guid_xref.get(catalog_col);
            // assert(guids != null && guids.isEmpty() == false) :
            // "No PlanColumn GUID for " +
            // CatalogUtil.getDisplayName(catalog_col);
            // Integer col_guid = CollectionUtil.getFirst(guids);
            //            
            //
            // // Always try make a new PlanColumn and update the
            // TupleValueExpresion index
            // // This ensures that we always get the ordering correct
            // TupleValueExpression clone_exp =
            // (TupleValueExpression)orig_col.getExpression().clone();
            // clone_exp.setColumnIndex(idx);
            // Storage storage = (catalog_tbl.getIsreplicated() ?
            // Storage.kReplicated : Storage.kPartitioned);
            // PlanColumn new_col = m_context.getPlanColumn(clone_exp,
            // orig_col.displayName(), orig_col.getSortOrder(), storage);
            // assert(new_col != null);
            // proj_node.appendOutputColumn(new_col);
            this.addColumnMapping(catalog_col, orig_col.guid());
            idx++;
        } // FOR
        if (trace)
            LOG.trace("New Projection Output Columns:\n" + PlanNodeUtil.debugNode(proj_node));

        // Add projection inline to scan node
        scan_node.addInlinePlanNode(proj_node);
        assert (proj_node.isInline());

        // Then make sure that we update it's output columns to match the inline
        // output
        scan_node.m_outputColumns.clear();
        scan_node.m_outputColumns.addAll(proj_node.m_outputColumns);

        // add element to the "dirty" list
        this.markDirty(scan_node);
        if (trace)
            LOG.trace(String.format("Added inline %s with %d columns to leaf node %s", proj_node, proj_node.getOutputColumnCount(), scan_node));
        return (true);
    }

    /**
     * @param node
     * @return
     */
    protected boolean updateDistinctColumns(DistinctPlanNode node) {
        final boolean trace = LOG.isTraceEnabled();

        // We really have one child here
        assert (node.getChildCount() == 1) : node;
        AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);

        // Find the offset of our distinct column in our output. That will
        // tell us where to get the guid in the input table information
        int orig_guid = node.getDistinctColumnGuid();
        PlanColumn orig_pc = m_context.get(orig_guid);
        assert (orig_pc != null);

        node.setOutputColumns(child_node.m_outputColumns);

//        PlanColumn new_pc = null;
//        int new_idx = 0;
//        for (Integer guid : node.m_outputColumns) {
//            PlanColumn pc = m_context.get(guid);
//            assert (pc != null);
//            if (pc.equals(orig_pc, true, true)) {
//                if (trace)
//                    LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
//                new_pc = pc;
//                break;
//            }
//            new_idx++;
//        } // FOR
//        assert (new_pc != null);
//
//        
//        
//        // Now we can update output columns and set the distinct column to be
//        // the guid
//      node.setDistinctColumnGuid(new_pc.guid());

        for (Integer guid : node.m_outputColumns) {
            node.setDistinctColumnGuid(guid);            
        } // FOR

        markDirty(node);
//        if (LOG.isDebugEnabled())
//            LOG.debug(String.format("Updated %s with proper distinct column guid: ORIG[%d] => NEW[%d]", node, orig_guid, new_pc.guid()));

        return (true);
    }

    /**
     * Update OrderBy columns
     * 
     * @param node
     * @return
     */
    protected boolean updateOrderByColumns(OrderByPlanNode node) {
        final boolean trace = LOG.isTraceEnabled();

        // We really have one child here
        assert (node.getChildCount() == 1) : node;
        AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);

        node.setOutputColumns(child_node.m_outputColumns);
        updateOutputOffsets(node);

        for (int i = 0, cnt = node.getSortColumnGuids().size(); i < cnt; i++) {
            int orig_guid = node.getSortColumnGuids().get(i);
            PlanColumn orig_pc = m_context.get(orig_guid);
            assert (orig_pc != null);

            PlanColumn new_pc = null;
            int new_idx = 0;
            for (Integer guid : node.m_outputColumns) {
                PlanColumn pc = m_context.get(guid);
                assert (pc != null);
                if (pc.equals(orig_pc, true, true)) {
                    if (trace)
                        LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
                    new_pc = pc;
                    break;
                }
                new_idx++;
            } // FOR
            if (new_pc == null) {
                LOG.error(sql);
                LOG.error(String.format("[%02d] Failed to find %s", i, orig_pc));
            }
            assert (new_pc != null);
            node.getSortColumnGuids().set(i, new_pc.guid());
        } // FOR

        this.markDirty(node);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Updated %s with proper orderby column guid", node));

        return (true);
    }

    /**
     * Update AggregatePlanNode columns
     * 
     * @param node
     * @return
     */
    protected boolean updateAggregateColumns(AggregatePlanNode node) {
        final boolean trace = LOG.isTraceEnabled();

        // We really have one child here
        assert (node.getChildCount() == 1) : node;
        AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);

        for (int i = 0, cnt = node.getAggregateColumnGuids().size(); i < cnt; i++) {
            Integer orig_guid = node.getAggregateColumnGuids().get(i);
            PlanColumn orig_pc = m_context.get(orig_guid);
            assert (orig_pc != null);

            PlanColumn new_pc = null;
            int new_idx = 0;
            for (Integer guid : child_node.m_outputColumns) {
                PlanColumn pc = m_context.get(guid);
                if (pc.getStorage().equals(Storage.kTemporary)) {
                    new_pc = pc;
                    break;
                } else {
                    assert (pc != null);
                    if (pc.equals(orig_pc, true, true)) {
                        if (trace)
                            LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
                        new_pc = pc;
                        break;
                    }                    
                }
                new_idx++;
            } // FOR
            if (new_pc == null) {
                LOG.error(String.format("Couldn't find %d => %s\n", new_idx, new_pc));
                LOG.error(PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
            }
            assert (new_pc != null);
            node.getAggregateColumnGuids().set(i, new_pc.guid());
        } // FOR

        // Need to update output column guids for GROUP BYs...
        for (int i = 0, cnt = node.getGroupByColumnIds().size(); i < cnt; i++) {
            Integer orig_guid = node.getGroupByColumnIds().get(i);
            PlanColumn orig_pc = m_context.get(orig_guid);
            assert (orig_pc != null);

            PlanColumn new_pc = null;
            int new_idx = 0;
            for (Integer guid : child_node.m_outputColumns) {
                PlanColumn pc = m_context.get(guid);
                if (pc.getStorage().equals(Storage.kTemporary)) {
                    new_pc = pc;
                    break;
                } else {
                    assert (pc != null);
                    if (pc.equals(orig_pc, true, true)) {
                        if (trace)
                            LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
                        new_pc = pc;
                        break;
                    }                    
                }
                new_idx++;
            } // FOR
            assert (new_pc != null);
            node.getGroupByColumnIds().set(i, new_pc.guid());
        } // FOR

        // System.err.println(this.sql);
        // System.err.println("AGGREGATE_OUTPUT_COLUMNS: " +
        // agg_node.getAggregateOutputColumns());
        // System.err.println("AGGREGATE_OUTPUT_COLUMN_GUIDS: " +
        // agg_node.getAggregateColumnGuids());
        // System.err.println("AGGREGATE_OUTPUT_COLUMN_NAMES: " +
        // agg_node.getAggregateColumnNames());
        // System.err.println("AGGREGATE_OUTPUT_COLUMN_TYPES: " +
        // agg_node.getAggregateTypes());
        // System.err.println("ORIG_CHILD_OUTPUT: " + orig_child_output);
        // System.err.println("NEW_CHILD_OUTPUT: " +
        // child_node.m_outputColumns);
        // System.err.println(PlanNodeUtil.debug(PlanNodeUtil.getRoot(agg_node)));

        markDirty(node);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Updated %s with %d proper aggregate column guids", node, node.getAggregateColumnGuids().size()));
        return (true);
    }

    /**
     * @param node
     * @return
     * @throws Exception
     */
    protected boolean updateProjectionColumns(ProjectionPlanNode node) {
        final boolean debug = LOG.isDebugEnabled();

        assert (node.getChildCount() == 1) : node;
        final AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);
        final List<Integer> orig_child_guids = this.orig_node_output.get(child_node);

        for (int i = 0, cnt = node.getOutputColumnCount(); i < cnt; i++) {
            // Check to make sure that the offset in the tuple value expression
            // matches
            int orig_guid = node.getOutputColumnGUID(i);
            PlanColumn orig_pc = m_context.get(orig_guid);
            assert (orig_pc != null);

            // Fix all of the offsets in the ExpressionTree
            // We have to clone it so that we don't mess up anybody else that
            // may be referencing the same PlanColumn
            AbstractExpression new_exp = null;
            try {
                new_exp = (AbstractExpression) orig_pc.getExpression().clone();
            } catch (Exception ex) {
                LOG.fatal("Unable to clone " + orig_pc, ex);
                System.exit(1);
            }

            new ExpressionTreeWalker() {
                @Override
                protected void callback(AbstractExpression exp_element) {
                    if (exp_element instanceof TupleValueExpression) {
                        TupleValueExpression tv_exp = (TupleValueExpression) exp_element;
                        int orig_idx = tv_exp.getColumnIndex();
                        PlanColumn orig_child_pc = m_context.get(orig_child_guids.get(orig_idx));
                        assert (orig_child_pc != null);

                        PlanColumn new_child_pc = null;
                        int new_idx = 0;
                        for (Integer orig_child_guid : child_node.m_outputColumns) {
                            new_child_pc = m_context.get(orig_child_guid);
                            if (orig_child_pc.equals(new_child_pc, true, true)) {
                                break;
                            }
                            new_child_pc = null;
                            new_idx++;
                        } // FOR
                        assert (new_child_pc != null);
                        tv_exp.setColumnIndex(new_idx);
                    }
                }
            }.traverse(new_exp);

            // Always try make a new PlanColumn and update the
            // TupleValueExpresion index
            // This ensures that we always get the ordering correct
            PlanColumn new_col = m_context.getPlanColumn(new_exp, orig_pc.displayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
            assert (new_col != null);
            node.m_outputColumns.set(i, new_col.guid());
        } // FOR
        this.markDirty(node);
        if (debug)
            LOG.debug(String.format("Updated %s with %d output columns offsets", node, node.getOutputColumnCount()));
        return (true);
    }

    /**
     * @param node
     * @return
     * @throws Exception
     */
    protected boolean updateOutputOffsets(AbstractPlanNode node) {
        final boolean debug = LOG.isDebugEnabled();

        for (int i = 0, cnt = node.getOutputColumnCount(); i < cnt; i++) {
            // Check to make sure that the offset in the tuple value expression
            // matches
            int orig_guid = node.getOutputColumnGUID(i);
            PlanColumn orig_pc = m_context.get(orig_guid);
            assert (orig_pc != null);

            AbstractExpression orig_pc_exp = orig_pc.getExpression();
            if (!(orig_pc_exp instanceof TupleValueExpression)) {
                TupleValueExpression new_exp = new TupleValueExpression();
                new_exp.setColumnIndex(i);
                new_exp.setColumnAlias(orig_pc.displayName());
                new_exp.setValueType(VoltType.STRING);
                PlanColumn new_col = m_context.getPlanColumn(new_exp, orig_pc.displayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                assert (new_col != null);
                node.m_outputColumns.set(i, new_col.guid());
            } else  {
                // Always try make a new PlanColumn and update the
                // TupleValueExpresion index
                // This ensures that we always get the ordering correct
                TupleValueExpression orig_exp = (TupleValueExpression) orig_pc.getExpression();
                int orig_idx = orig_exp.getColumnIndex();

                if (orig_idx != i) {
                    TupleValueExpression clone_exp = null;
                    try {
                        clone_exp = (TupleValueExpression) orig_pc.getExpression().clone();
                    } catch (Exception ex) {
                        LOG.fatal("Unable to clone " + orig_pc, ex);
                        System.exit(1);
                    }
                    clone_exp.setColumnIndex(i);
                    PlanColumn new_col = m_context.getPlanColumn(clone_exp, orig_pc.displayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                    assert (new_col != null);
                    // DWU: set this to the orig plan column guid
                    node.m_outputColumns.set(i, orig_pc.guid());
                    //node.m_outputColumns.set(i, new_col.guid());
                }
            } // FOR                
            }
        this.markDirty(node);
        if (debug)
            LOG.debug(String.format("Updated %s with %d output columns offsets", node, node.getOutputColumnCount()));
        return (true);
    }
    
    /**
     * @param node
     * @return
     */
    protected boolean updateJoinsColumns(AbstractJoinPlanNode node) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();

        // There's always going to be two input tables. One is always going to
        // come
        // from a child node, while the second may come from a child node *or*
        // directly from
        // a table being scanned. Therefore, we need to first figure out the
        // original size
        // of the first input table and then use that to adjust the offsets of
        // the new tables
        AbstractPlanNode outer_node = node.getChild(0);
        assert (outer_node != null);
        List<Integer> outer_new_input_guids = outer_node.m_outputColumns;
        if (debug)
            LOG.debug("Calculating OUTER offsets from child node: " + outer_node);

        // List of PlanColumn GUIDs for the new output list
        List<Integer> new_output_guids = new ArrayList<Integer>();
        SortedMap<Integer, Integer> sorted_new_output_guids = new TreeMap<Integer, Integer>();

        // Go and build a map from original offsets to the new offsets that need
        // to be stored
        // for the TupleValueExpressions (and possible TupleAddressExpression)
        final Map<Integer, Integer> offset_xref = new HashMap<Integer, Integer>();
        List<Integer> outer_orig_input_guids = PlanOptimizer.this.orig_node_output.get(outer_node);
        assert (outer_orig_input_guids != null);
        StringBuilder sb = new StringBuilder();
        for (int orig_idx = 0, cnt = outer_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
            int orig_col_guid = outer_orig_input_guids.get(orig_idx);
            PlanColumn orig_pc = m_context.get(orig_col_guid);

            // Figure out what the new PlanColumn GUID is for this column
            // It may be the case that we need to make a new one because the
            // underlying expession has the wrong offsets
            PlanColumn new_pc = null;
            Integer new_idx = null;

            // Find the new index of this same PlanColumn guid
            new_idx = outer_new_input_guids.indexOf(orig_col_guid);
            if (new_idx != -1) {
                new_pc = m_context.get(orig_col_guid);
                //new_output_guids.add(orig_col_guid);
                sorted_new_output_guids.put(new_idx, orig_col_guid);
                if (debug)
                    LOG.debug(String.format("OUTER OFFSET %d => %d", orig_idx, new_idx));

                // Check whether we even have this column. We'll compare
                // everything but the Expression
            } else {
                new_idx = 0;
                for (Integer guid : outer_new_input_guids) {
                    PlanColumn pc = m_context.get(guid);
                    assert (pc != null);
                    if (pc.equals(orig_pc, true, true)) {
                        if (trace)
                            LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW:  %s", orig_idx, orig_pc, pc));
                        new_pc = pc;
                        break;
                    }
                    new_idx++;
                } // FOR

                // If we have this PlanColumn, then we need to clone it and set
                // the new column index
                // Make sure that we replace update outer_new_input_guids
                if (new_pc != null) {
                    assert (new_idx != -1);
                    TupleValueExpression clone_exp = (TupleValueExpression) orig_pc.getExpression().clone();
                    clone_exp.setColumnIndex(new_idx);
                    PlanColumn new_col = m_context.getPlanColumn(clone_exp, orig_pc.displayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                    assert (new_col != null);
                    outer_new_input_guids.set(new_idx, new_col.guid());
                    //new_output_guids.add(new_col.guid());
                    sorted_new_output_guids.put(new_idx, new_col.guid());
                    if (debug)
                        LOG.debug(String.format("OUTER OFFSET %d => %d [new_guid=%d]", orig_idx, new_idx, new_col.guid()));
                } else {
                    new_idx = null;
                }
            }

            if (new_idx != null) {
                assert (offset_xref.containsKey(orig_idx) == false) : orig_idx + " ==> " + offset_xref;
                offset_xref.put(orig_idx, new_idx);
            } else {
                String msg = String.format("[%02d] Failed to find new offset for OUTER %s", orig_idx, orig_pc);
                sb.append(msg).append("\n");
                if (debug)
                    LOG.warn(msg);
            }
        } // FOR
        if (trace)
            LOG.trace("Original Outer Input GUIDs: " + outer_orig_input_guids);
        if (trace)
            LOG.trace("New Outer Input GUIDs:      " + outer_new_input_guids);
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
            // LOG.info("Trace Information:\n" + sb);
            LOG.error("Unexpected Query Plan\n" + sql + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
        }
        assert (outer_new_input_guids.size() == offset_xref.size()) : sql + " outer_new_input_guids size: " + outer_new_input_guids.size() + " offset_xref size: " + offset_xref.size();

        // add the sorted columns into new_columns list
        Iterator iterator = sorted_new_output_guids.keySet().iterator();
        while (iterator.hasNext()) {
            new_output_guids.add(sorted_new_output_guids.get((Integer)iterator.next()));
        }        
        
        // For the inner table, we always have to offset ourselves based on the
        // size
        // of the new outer table
        int offset = outer_new_input_guids.size();
        
        // Whether we are in a NestLoop join or not
        boolean is_nestloop = false;

        AbstractPlanNode inner_node = null;

        // These are the set of expressions for the join clause that we need to
        // fix their offsets for
        final Set<AbstractExpression> expressions_to_fix = PlanNodeUtil.getExpressions(node);

        // --------------------------------------------
        // NEST LOOP
        // --------------------------------------------
        if (node.getChildCount() > 1) {
            assert (node instanceof NestLoopPlanNode);
            is_nestloop = true;

            inner_node = node.getChild(1);
            if (debug)
                LOG.debug("Calculating INNER offsets from child node: " + inner_node);

            List<Integer> inner_orig_input_guids = PlanOptimizer.this.orig_node_output.get(inner_node);
            assert (inner_orig_input_guids != null);
            List<Integer> inner_new_input_guids = inner_node.m_outputColumns;

            for (int orig_idx = 0, cnt = inner_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
                int col_guid = inner_orig_input_guids.get(orig_idx);

                // Find the new index of this same PlanColumn guid
                int new_idx = inner_new_input_guids.indexOf(col_guid);
                if (new_idx != -1) {
                    int offset_orig_idx = outer_orig_input_guids.size() + orig_idx;
                    int offset_new_idx = offset + new_idx;
                    if (trace)
                        LOG.trace(String.format("INNER NODE OFFSET %d => %d", offset_orig_idx, offset_new_idx));
                    assert (offset_xref.containsKey(offset_orig_idx) == false) : orig_idx + " ==> " + offset_xref;
                    offset_xref.put(offset_orig_idx, offset_new_idx);
                  new_output_guids.add(col_guid);
//                  sorted_new_output_guids.put(new_idx, col_guid);
                } else {
                    PlanColumn pc = m_context.get(col_guid);
                    LOG.warn("Failed to find new offset for INNER " + pc);
                }
            } // FOR
            if (trace)
                LOG.trace("Original Inner Input GUIDs: " + inner_orig_input_guids);
            if (trace)
                LOG.trace("New Inner Input GUIDs:      " + inner_new_input_guids);

            // ---------------------------------------------------
            // NEST LOOP INDEX
            // ---------------------------------------------------
        } else {
            // Otherwise, just grab all of the columns for the target table in
            // the inline scan
            assert (node instanceof NestLoopIndexPlanNode);
            IndexScanPlanNode idx_node = node.getInlinePlanNode(PlanNodeType.INDEXSCAN);
            
            assert (idx_node != null);
            inner_node = idx_node;

            Table catalog_tbl = null;
            try {
                catalog_tbl = CollectionUtil.getFirst(CatalogUtil.getReferencedTablesNonRecursive(m_catalogDb, idx_node));
            } catch (Exception ex) {
                LOG.fatal(ex);
                System.exit(1);
            }
            assert (catalog_tbl != null);
            if (debug)
                LOG.debug("Calculating INNER offsets from INLINE Scan: " + catalog_tbl);

            for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
                int i = catalog_col.getIndex();
                int offset_orig_idx = outer_orig_input_guids.size() + i;
                int offset_new_idx = offset + i;
                if (trace)
                    LOG.trace(String.format("INNER INLINE OFFSET %d => %d", offset_orig_idx, offset_new_idx));
                offset_xref.put(offset_orig_idx, offset_new_idx);

                // Since we're going in order, we know what column is at this
                // position.
                // That means we can grab the catalog object and convert it to a
                // PlanColumn GUID
                // Always try make a new PlanColumn and update the
                // TupleValueExpresion index
                // This ensures that we always get the ordering correct
                //int orig_guid = idx_node.getOutputColumnGUID(offset_orig_idx);
                int orig_guid = CollectionUtil.getFirst(column_guid_xref.get(catalog_col));
                assert (orig_guid != -1);
                PlanColumn orig_pc = m_context.get(orig_guid);
                assert (orig_pc != null);

//                PlanColumn new_pc = null;
//                int new_idx = 0;
//                for (Integer guid : idx_node.m_outputColumns) {
//                    PlanColumn pc = m_context.get(guid);
//                    assert (pc != null);
//                    if (pc.equals(orig_pc, true, true)) {
//                        if (trace)
//                            LOG.trace(String.format("[%02d] Found inline output PlanColumn match:\nORIG: %s\nNEW:  %s", new_idx, orig_pc, pc));
//                        new_pc = pc;
//                        break;
//                    }
//                    new_idx++;
//                } // FOR
//                assert (new_pc != null);

                idx_node.m_outputColumns.set(i, orig_pc.guid());
              new_output_guids.add(orig_pc.guid());
//              sorted_new_output_guids.put(i,orig_pc.guid());
                // TupleValueExpression clone_exp =
                // (TupleValueExpression)orig_col.getExpression().clone();
                // clone_exp.setColumnIndex(offset_new_idx);
                // Storage storage = (catalog_tbl.getIsreplicated() ?
                // Storage.kReplicated : Storage.kPartitioned);
                // PlanColumn new_col = m_context.getPlanColumn(clone_exp,
                // orig_col.displayName(), orig_col.getSortOrder(), storage);
                // assert(new_col != null);

            } // FOR
            
            // We also need to fix all of the search key expressions used in the
            // inline scan
            expressions_to_fix.addAll(PlanNodeUtil.getExpressions(idx_node));
            //System.out.println("expressions_to_fix: " + expressions_to_fix);
        }
        if (trace)
            LOG.trace("Output Xref Offsets:      " + offset_xref);
        if (trace)
            LOG.trace("New Output Columns GUIDS: " + sorted_new_output_guids);

        // Get all of the AbstractExpression roots for this node
        // Now fix the offsets for everyone
        for (AbstractExpression exp : expressions_to_fix) {
            new ExpressionTreeWalker() {
                @Override
                protected void callback(AbstractExpression exp_element) {
                    if (exp_element instanceof TupleValueExpression) {
                        TupleValueExpression tv_exp = (TupleValueExpression) exp_element;
                        int orig_idx = tv_exp.getColumnIndex();

                        // If we're in a NestLoopJoin (and not a
                        // NestLoopIndexJoin), then what we need to
                        // do is take the original offset (which points to a
                        // column in the original inner input), and s

                        Integer new_idx = offset_xref.get(orig_idx);
                        if (new_idx == null)
                            LOG.debug(m_context.debug());
                        assert (new_idx != null) : "Missing Offset: " + ExpressionUtil.debug(tv_exp);
                        if (debug)
                            LOG.debug(String.format("Changing %s.%s [%d ==> %d]", tv_exp.getTableName(), tv_exp.getColumnName(), orig_idx, new_idx));
                        if (orig_idx != new_idx) {
                            tv_exp.setColumnIndex(new_idx);
                        }

                    }
                }
            }.traverse(exp);
        }

        // Then update the output columns to reflect the change
        node.setOutputColumns(new_output_guids);
        for (int new_idx = 0, cnt = node.m_outputColumns.size(); new_idx < cnt; new_idx++) {
            Integer col_guid = node.m_outputColumns.get(new_idx);
            PlanColumn pc = m_context.get(col_guid);

            // Look at what our offset used versus what it is needs to be
            // If it's different, then we need to make a new PlanColumn.
            // Note that we will clone TupleValueExpression so that we do not
            // mess with
            // other PlanColumns
            // Assume that AbstractExpression is always a TupleValueExpression
            TupleValueExpression tv_exp = (TupleValueExpression) pc.getExpression();
            assert (tv_exp != null);
            int orig_idx = tv_exp.getColumnIndex();
            // assert(new_idx == offset_xref.get(orig_idx)) :
            // String.format("Offset Mismatch [orig_idx=%d] =>  [%d] != [%d]:\noffset_xref = %s\n%s",
            // orig_idx, new_idx, offset_xref.get(orig_idx), offset_xref,
            // PlanNodeUtil.debugNode(element));
            if (orig_idx != new_idx) {
                TupleValueExpression clone_exp = null;
                try {
                    clone_exp = (TupleValueExpression) tv_exp.clone();
                } catch (Exception ex) {
                    LOG.fatal(ex);
                    System.exit(1);
                }
                assert (clone_exp != null);
                
                // compare with child's output columns to see whether orig_idx or new_idx is correct
                assert (node.getChildCount() == 1);
                ArrayList<Integer> child_output = node.getChild(0).m_outputColumns;
                if (orig_idx < child_output.size() && pc.guid() == child_output.get(orig_idx)) {
                    clone_exp.setColumnIndex(orig_idx);
                } else {
                    clone_exp.setColumnIndex(new_idx);                    
                }
                PlanColumn new_pc = m_context.getPlanColumn(clone_exp, pc.displayName(), pc.getSortOrder(), pc.getStorage());
                assert (new_pc != null);
                node.m_outputColumns.set(new_idx, new_pc.guid());
            }
            if (trace)
                LOG.trace(String.format("OUTPUT[%d] => %s", new_idx, m_context.get(node.m_outputColumns.get(new_idx))));
        } // FOR

        // IMPORTANT: If the inner_node is inline (meaning it was a
        // NestLoopIndex), then we need to also update
        // its output columns to match our new ones. This is necessary because
        // the nestloopindexexecutor will
        // generate its output table from the inline node and not the actual
        // output columns
        if (inner_node.isInline()) {
            assert (inner_node instanceof IndexScanPlanNode);
            inner_node.setOutputColumns(node.m_outputColumns);
            if (trace)
                LOG.trace("Updated INNER inline " + inner_node + " output columns");
        }

        // if (debug) LOG.debug("PlanNodeTree:\n" +
        // PlanNodeUtil.debug(rootNode));
        // LOG.debug(PlanNodeUtil.debugNode(element));

        this.markDirty(node);

        return (true);
    }

    protected void updateColumnInfo(AbstractPlanNode node) {
        this.clearColumnInfo();
        this.populateTableNodeInfo(node);
    }
    /**
     * @param rootNode
     */
    protected void process(final AbstractPlanNode rootNode) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();

        final int total_depth = PlanNodeUtil.getDepth(rootNode);

//      System.out.println("UNOPTIMIZED TREE: ");
//      System.out.println(PlanNodeUtil.debug(rootNode));
//      System.out.println("\n");
        
        if (debug)
            LOG.debug("Optimizing PlanNode Tree [total_depth=" + total_depth + "]");
        // if (debug) LOG.debug("PlanNodeTree:\n" +
        // PlanNodeUtil.debug(rootNode));

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
                    AbstractScanPlanNode scan_node = (AbstractScanPlanNode) element;
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
                    AbstractJoinPlanNode join_node = (AbstractJoinPlanNode) element;
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
                    if (areChildrenDirty(element) && updateDistinctColumns((DistinctPlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                    // ---------------------------------------------------
                    // AGGREGATE
                    // ---------------------------------------------------
                } else if (element instanceof AggregatePlanNode) {
                    if (areChildrenDirty(element) && updateAggregateColumns((AggregatePlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                    // ---------------------------------------------------
                    // ORDER BY
                    // ---------------------------------------------------
                } else if (element instanceof OrderByPlanNode) {
                    if (areChildrenDirty(element) && updateOrderByColumns((OrderByPlanNode) element) == false) {
                        this.stop();
                        return;
                    }

                    // ---------------------------------------------------
                    // PROJECTION
                    // ---------------------------------------------------
                } else if (element instanceof ProjectionPlanNode) {
                    if (areChildrenDirty(element) && updateProjectionColumns((ProjectionPlanNode) element) == false) {
                        this.stop();
                        return;
                    }

                    // ---------------------------------------------------
                    // SEND/RECEIVE/LIMIT
                    // ---------------------------------------------------
                } else if (element instanceof SendPlanNode || element instanceof ReceivePlanNode || element instanceof LimitPlanNode) {
                    // I think we should always call this to ensure that our
                    // offsets are ok
                    // This might be because we don't call whatever that
                    // bastardized
                    // AbstractPlanNode.updateOutputColumns() that messes
                    // everything up for us
                    if (element instanceof LimitPlanNode || areChildrenDirty(element)) {
                        assert (element.getChildCount() == 1) : element;
                        AbstractPlanNode child_node = element.getChild(0);
                        assert (child_node != null);
                        element.setOutputColumns(child_node.m_outputColumns);
                        updateOutputOffsets(element);
                    }
                }
                return;
            }
        }.traverse(rootNode);
        /** END OF INLINE PROJECTION OPTIMIZATION

        /** START OF ADDING PROJECTION TO JOINS OPTIMIZATION **/
        // walk the tree a third time and build the Map between nestedloopjoin
        // nodes and Set of Table Names map element id to set of columns
        final Map<Integer, Set<String>> join_tbl_mapping = new HashMap<Integer, Set<String>>();
        final ArrayList<ProjectionPlanNode> projection_plan_nodes = new ArrayList<ProjectionPlanNode>();
        final Set<String> ref_join_tbls = new HashSet<String>();
        final SortedMap<Integer, AbstractPlanNode> join_node_index = new TreeMap<Integer, AbstractPlanNode>();
        final Map<AbstractPlanNode, Map<String, Integer>> join_outputs = new HashMap<AbstractPlanNode, Map<String,Integer>>();
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element instanceof AbstractScanPlanNode) {
                    ref_join_tbls.add(((AbstractScanPlanNode) element).getTargetTableName());
                } else if (element instanceof AbstractJoinPlanNode) {
                    // get target table of inline scan
                    assert (element.getInlinePlanNodeCount() == 1) : "Join has incorrect number of inline nodes";
                    AbstractScanPlanNode inline_scan_node = (AbstractScanPlanNode) CollectionUtil.getFirst(element.getInlinePlanNodes().values());
                    ref_join_tbls.add(inline_scan_node.getTargetTableName());
                    /** need temp set to put into hashmap! **/
                    HashSet<String> temp_set = (HashSet<String>) ((HashSet<String>) ref_join_tbls).clone();
                    join_tbl_mapping.put(element.getPlanNodeId(), temp_set);
                    // add to join index map which depth is the index
                    join_node_index.put(this.getDepth(), element);
                    Map<String, Integer> single_join_node_output = new HashMap<String, Integer>();
                    for (int i = 0; i < element.getOutputColumnCount(); i++) {
                        int guid = element.getOutputColumnGUID(i);
                        PlanColumn pc = m_context.get(guid);
                        single_join_node_output.put(pc.displayName(), i);
                    }
                    join_outputs.put(element, single_join_node_output);
                }
            }
        }.traverse(rootNode);
        /** Finished building the map between join nodes and reference tables **/

        new_root = rootNode;
        if (join_tbl_mapping.size() > 0) {
//            if (join_tbl_mapping.size() == 2) {
//                System.out.println();
//            }
            assert (join_node_index.size() == join_tbl_mapping.size()) : "join_node_index and join_tbl_mapping have different number of elements!";
            // Add projection when right above joins
            // clear the "dirty" nodes lists
            dirtyPlanNodes.clear();

            updateColumnInfo(rootNode);
            new PlanNodeTreeWalker(false) {
                @Override
                protected void callback(AbstractPlanNode element) {
                    if (element instanceof NestLoopIndexPlanNode && element.getParent(0) instanceof SendPlanNode) {
                        assert (join_node_index.size() == join_tbl_mapping.size()) : "Join data structures don't have the same size!!!";
                        assert (join_tbl_mapping.get(element.getPlanNodeId()) != null) : "Element : " + element.getPlanNodeId() + " does NOT exist in join map!!!";
                        final Set<String> current_tbls_in_join = join_tbl_mapping.get(element.getPlanNodeId());
                        final Set<PlanColumn> join_columns = new LinkedHashSet<PlanColumn>();
                        // traverse the tree from bottom up from the current nestloop index
                        final int outer_depth = this.getDepth();
                        final SortedMap<Integer, PlanColumn> proj_column_order = new TreeMap<Integer, PlanColumn>();
                        /**
                         * Adds a projection column as a parent of the the
                         * current join node. (Sticks it between the send and
                         * join)
                         **/
                        final boolean top_join = (join_node_index.get(join_node_index.firstKey()) == element);
                        new PlanNodeTreeWalker(true) {
                            @Override
                            protected void callback(AbstractPlanNode inner_element) {
                                int inner_depth = this.getDepth();
                                if (inner_depth < outer_depth) {
                                    // only interested in projections and index scans
                                    if (inner_element instanceof ProjectionPlanNode || inner_element instanceof IndexScanPlanNode) {
                                        Set<Column> col_set = planNodeColumns.get(inner_element);
                                        assert (col_set != null) : "Null column set for inner element: " + inner_element + "\n" + sql;
                                        //Map<String, Integer> current_join_output = new HashMap<String, Integer>();
                                        // Check whether any output columns have
                                        // operator, aggregator and project
                                        // those columns now!
                                        // iterate through columns and build the
                                        // projection columns
                                        if (top_join) {
                                            for (Integer output_guid : inner_element.m_outputColumns) {
                                                PlanColumn plan_col = m_context.get(output_guid);
                                                for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                                    TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                                    if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                                        addProjectionColumn(join_columns, output_guid);
                                                    }
                                                }
                                            }
                                        }
                                        else {
                                            for (Column col : col_set) {
                                                Integer col_guid = CollectionUtil.getFirst(column_guid_xref.get(col));
                                                PlanColumn plan_col = m_context.get(col_guid);
                                                for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                                    TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                                    if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                                        addProjectionColumn(join_columns, col_guid);
                                                    }
                                                }
                                            }                                            
                                        }
                                    } else if (inner_element instanceof AggregatePlanNode) {
                                        Set<Column> col_set = planNodeColumns.get(inner_element);
                                        for (Column col : col_set) {
                                            Integer col_guid = CollectionUtil.getFirst(column_guid_xref.get(col));
                                            PlanColumn plan_col = m_context.get(col_guid);
                                            for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                                TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                                if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                                    addProjectionColumn(join_columns, col_guid);
                                                }
                                            }
                                        }                                                                                	
                                    }
                                }
                            }
                        }.traverse(PlanNodeUtil.getRoot(element));
                        /** END OF "CONSTRUCTING" THE PROJECTION PLAN NODE **/
                        // Add a projection above the current nestloopindex plan node
                        AbstractPlanNode temp_parent = element.getParent(0);
                        // clear old parents
                        element.clearParents();
                        temp_parent.clearChildren();
                        ProjectionPlanNode projectionNode = new ProjectionPlanNode(m_context, PlanAssembler.getNextPlanNodeId());
                        projectionNode.m_outputColumns.clear();

//                        if (join_node_index.get(join_node_index.firstKey()) == element) {
//                            assert (proj_column_order != null);
//                            Iterator<Integer> order_iterator = proj_column_order.keySet().iterator();
//                            while (order_iterator.hasNext()) {
//                                projectionNode.appendOutputColumn(proj_column_order.get(order_iterator.next()));                                        
//                            }
//                        } else {
                            AbstractExpression orig_col_exp = null;
                            int orig_guid = -1;
                            PlanColumn orig_plan_col = null;
                            for (PlanColumn plan_col : join_columns) {
                                boolean exists = false;
                                for (Integer guid : element.m_outputColumns) {
                                    PlanColumn output_plan_column = m_context.get(guid);
                                    if (plan_col.equals(output_plan_column, true, true)) {
                                        orig_plan_col = plan_col;
                                        orig_col_exp = output_plan_column.getExpression();
                                        orig_guid = guid;
                                        exists = true;
                                        break;
                                    }
                                }
                                if (!exists) {
                                    LOG.warn("Trouble plan column name: " + plan_col.m_displayName);
                                } else {
                                    assert (orig_col_exp != null);
                                    AbstractExpression new_col_exp = null;
                                    try {
                                        new_col_exp = (AbstractExpression) orig_col_exp.clone();
                                    } catch (CloneNotSupportedException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                    assert (new_col_exp != null);
                                    PlanColumn new_plan_col = null;
                                    if (new_col_exp instanceof TupleValueExpression) {
                                        new_plan_col = new PlanColumn(orig_guid, new_col_exp, ((TupleValueExpression)new_col_exp).getColumnName(), plan_col.getSortOrder(), plan_col.getStorage());                                    
                                        projectionNode.appendOutputColumn(new_plan_col);                                
                                    }
                                }
                            }                            
//                        }
                        
                        projectionNode.addAndLinkChild(element);
                        temp_parent.addAndLinkChild(projectionNode);
                        // add to list of projection nodes
                        projection_plan_nodes.add(projectionNode);
                        // mark projectionNode as dirty
                        dirtyPlanNodes.add(projectionNode);
                        join_columns.clear();
                        //this.stop();
                    }
                }
            }.traverse(PlanNodeUtil.getRoot(rootNode));

            /** KILL THE TOP MOST PROJECTION **/
            if (projection_plan_nodes.size() > 0) {
                new PlanNodeTreeWalker(false) {
                    @Override
                    protected void callback(AbstractPlanNode element) {
                        /** NOTE: we cannot assume that the projection is the top most node because of the case of the LIMIT case. 
                         * Limit nodes will always be the top most node so we need to check for that. **/
                        if (element.getParentCount() > 0 && element.getParent(0) instanceof ProjectionPlanNode && (PlanNodeUtil.getRoot(element) == element.getParent(0)) && !projection_plan_nodes.contains((ProjectionPlanNode) element.getParent(0))) {
                            ProjectionPlanNode proj_node = (ProjectionPlanNode) element.getParent(0);
                            assert (proj_node.getChildCount() == 1) : "Projection element expected 1 child but has " + proj_node.getChildCount() + " children!!!!";
                            // now currently at the child of the top most
                            // projection node
                            proj_node.removeFromGraph();
                            new_root = element;
                            this.stop();
                        } else if (element.getParentCount() > 0 && element.getParent(0) instanceof ProjectionPlanNode && (PlanNodeUtil.getRoot(element) != element.getParent(0)) && !projection_plan_nodes.contains((ProjectionPlanNode) element.getParent(0))) {
                            // this is the case where the top most node is not a projection
                            ProjectionPlanNode current_projection = (ProjectionPlanNode)element.getParent(0);
                            // make sure current projection has parent
                            assert (current_projection.getParentCount() > 0);
                            AbstractPlanNode projection_parent = current_projection.getParent(0);
                            element.clearParents();
                            projection_parent.clearChildren();
                            projection_parent.addAndLinkChild(element);
                            new_root = projection_parent;
                            this.stop();
                        }
                    }
                }.traverse(rootNode);
            }
            /** TOP MOST PROJECTION KILLED **/

            assert (new_root != null);
            updateColumnInfo(new_root);
            new PlanNodeTreeWalker(false) {
                @Override
                protected void callback(AbstractPlanNode element) {
                    // ---------------------------------------------------
                    // JOIN
                    // ---------------------------------------------------
                    if (element instanceof AbstractJoinPlanNode) {
                        AbstractJoinPlanNode join_node = (AbstractJoinPlanNode) element;
                        try {
                            if (areChildrenDirty(join_node) && updateJoinsColumns(join_node) == false) {
                                this.stop();
                                return;
                            }
                        } catch (Exception ex) {
                            LOG.fatal("Failed to update join columns in " + element, ex);
                            System.exit(1);
                            // ---------------------------------------------------
                            // ORDER BY
                            // ---------------------------------------------------
                        }
                    } else if (element instanceof OrderByPlanNode) {
                        if (areChildrenDirty(element) && updateOrderByColumns((OrderByPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    } 
                    
                    // ---------------------------------------------------
                    // AGGREGATE
                    // ---------------------------------------------------
                    else if (element instanceof AggregatePlanNode) {
                        if (areChildrenDirty(element) && updateAggregateColumns((AggregatePlanNode) element) == false) {
                            this.stop();
                            return;
                        }

                    } 
                    
                    // ---------------------------------------------------
                    // DISTINCT
                    // ---------------------------------------------------
                    else if (element instanceof DistinctPlanNode) {
                        if (areChildrenDirty(element) && updateDistinctColumns((DistinctPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                        // ---------------------------------------------------
                        // PROJECTION
                        // ---------------------------------------------------
                    }
                    else if (element instanceof ProjectionPlanNode) {
                        if (areChildrenDirty(element) && updateProjectionColumns((ProjectionPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    }

                    else if (element instanceof SendPlanNode || element instanceof ReceivePlanNode || element instanceof LimitPlanNode) {
                        // I think we should always call this to ensure that our
                        // offsets are ok
                        // This might be because we don't call whatever that
                        // bastardized
                        // AbstractPlanNode.updateOutputColumns() that messes
                        // everything up for us
                        if (element instanceof LimitPlanNode || areChildrenDirty(element)) {
                            assert (element.getChildCount() == 1) : element;
                            AbstractPlanNode child_node = element.getChild(0);
                            assert (child_node != null);
                            element.setOutputColumns(child_node.m_outputColumns);
                            updateOutputOffsets(element);
                        }
                    }
                }
            }.traverse(new_root);
//             System.out.println("NEW CURRENT TREE: ");
//             System.out.println(PlanNodeUtil.debug(new_root));
//             System.out.println();
            }
        /** END OF ADDING PROJECTION TO JOINS OPTIMIZATION **/

        if (debug)
            LOG.trace("Finished Optimizations!");
        // if (debug) LOG.debug("Optimized PlanNodeTree:\n" +
        // PlanNodeUtil.debug(rootNode));
    }

}