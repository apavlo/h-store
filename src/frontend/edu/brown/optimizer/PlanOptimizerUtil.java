package edu.brown.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanColumn;
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
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public abstract class PlanOptimizerUtil {
    private static final Logger LOG = Logger.getLogger(PlanOptimizerUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * 
     * @param state
     * @param orig_pc
     * @param output_cols
     * @return
     */
    protected static Pair<PlanColumn, Integer> findMatchingColumn(final PlanOptimizerState state, PlanColumn orig_pc, List<Integer> output_cols) {
        PlanColumn new_pc = null;
        int new_idx = 0;
        for (Integer new_guid : output_cols) {
            new_pc = state.plannerContext.get(new_guid);
            assert (new_pc != null) : "Unexpected PlanColumn #" + new_guid;

            if (new_pc.equals(orig_pc, true, true)) {
                if (trace.val)
                    LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW: %s", new_idx, orig_pc, new_pc));
                break;
            }
            new_pc = null;
            new_idx++;
        } // FOR
        return (new_pc != null ? Pair.of(new_pc, new_idx) : null);
    }

    /**
     * Populates the two data structures with information on the planNodes and
     * Tables and their referenced columns
     * @param state
     * @param rootNode
     */
    public static void populateTableNodeInfo(final PlanOptimizerState state, final AbstractPlanNode rootNode) {
        // Traverse tree and build up our data structures that maps all nodes to
        // the columns they affect
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                try {
                    extractColumnInfo(state, element, this.getDepth() == 0);
                } catch (Exception ex) {
                    if (debug.val)
                        LOG.fatal(PlanNodeUtil.debug(rootNode));
                    throw new RuntimeException("Failed to extract column information for " + element, ex);
                }
            }
        }.traverse(rootNode);
    }

    /**
     * Populate the mappings between AbstractPlanNodes and the tableNames, and
     * the element id(?) to set of columns
     * @param state
     * @param rootNode
     */
    public static void populateJoinTableInfo(final PlanOptimizerState state, final AbstractPlanNode rootNode) {
        final Set<String> join_tbls = new HashSet<String>();

        // Traverse from the bottom up and figure out what tables are referenced
        // in each AbstractJoinPlanNode
        for (AbstractPlanNode leaf : PlanNodeUtil.getLeafPlanNodes(rootNode)) {
            new PlanNodeTreeWalker(false, true) {
                @Override
                protected void callback(AbstractPlanNode element) {
                    // ---------------------------------------------------
                    // AbstractScanPlanNode
                    // ---------------------------------------------------
                    if (element instanceof AbstractScanPlanNode) {
                        join_tbls.add(((AbstractScanPlanNode) element).getTargetTableName());
                    }
                    // ---------------------------------------------------
                    // AbstractJoinPlanNode
                    // ---------------------------------------------------
                    else if (element instanceof AbstractJoinPlanNode) {
                        if (debug.val)
                            LOG.debug("Updating the list of tables joined at " + element);

                        // We don't NestLoopPlanNode for now
                        assert ((element instanceof NestLoopPlanNode) == false);

                        // Get target table of inline scan
                        Collection<AbstractScanPlanNode> inline_nodes = element.getInlinePlanNodes(AbstractScanPlanNode.class);
                        assert (inline_nodes.isEmpty() == false);
                        AbstractScanPlanNode inline_scan_node = CollectionUtil.first(inline_nodes);
                        assert (inline_scan_node != null);
                        join_tbls.add(inline_scan_node.getTargetTableName());

                        // Add all of the tables that we've seen at this point
                        // in the tree
                        state.join_tbl_mapping.put(element, new HashSet<String>(join_tbls));

                        // Add to join index map which depth is the index
                        state.join_node_index.put(this.getDepth(), (AbstractJoinPlanNode) element);
                        Map<String, Integer> single_join_node_output = new HashMap<String, Integer>();
                        for (int i = 0; i < element.getOutputColumnGUIDCount(); i++) {
                            int guid = element.getOutputColumnGUID(i);
                            PlanColumn pc = state.plannerContext.get(guid);
                            single_join_node_output.put(pc.getDisplayName(), i);
                        } // FOR
                        state.join_outputs.put((AbstractJoinPlanNode) element, single_join_node_output);
                    }

                }
            }.traverse(leaf);
        } // FOR
    }

    /**
     * 
     * @param state
     * @param node
     * @param is_root
     * @throws Exception
     */
    protected static void extractColumnInfo(final PlanOptimizerState state, final AbstractPlanNode node, final boolean is_root) throws Exception {
        if (trace.val)
            LOG.trace("Extracting Column Info for " + node);

        // Store the original output column information per node
        if (state.orig_node_output.containsKey(node) == false) {
            if (trace.val)
                LOG.trace("Storing original PlanNode output information for " + node);
            state.orig_node_output.put(node, new ArrayList<Integer>(node.getOutputColumnGUIDs()));
        }

        // Get all of the AbstractExpression roots for this node
        final Collection<AbstractExpression> exps = PlanNodeUtil.getExpressionsForPlanNode(node);
        // If this is the root node, then include the output columns + also
        // include output columns if its a projection or limit node
        if (is_root || node instanceof ProjectionPlanNode | node instanceof LimitPlanNode) {
            for (Integer col_guid : node.getOutputColumnGUIDs()) {
                PlanColumn col = state.plannerContext.get(col_guid);
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
                PlanColumn col = state.plannerContext.get(col_guid);
                assert (col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null)
                    exps.add(col.getExpression());
            } // FOR
            for (Integer col_guid : agg_node.getGroupByColumnGuids()) {
                PlanColumn col = state.plannerContext.get(col_guid);
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
                PlanColumn col = state.plannerContext.get(col_guid);
                assert (col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null)
                    exps.add(col.getExpression());
            } // FOR
        }

        if (debug.val)
            LOG.debug("Extracted " + exps.size() + " expressions from " + node);

        // Now go through our expressions and extract out the columns that are
        // referenced
        StringBuilder sb = new StringBuilder();
        for (AbstractExpression exp : exps) {
            for (Column catalog_col : ExpressionUtil.getReferencedColumns(state.catalog_db, exp)) {
                if (trace.val)
                    sb.append(String.format("\n%s => %s", node, catalog_col.fullName()));
                state.addTableColumn(catalog_col);
                state.addPlanNodeColumn(node, catalog_col);
            } // FOR
        } // FOR
        if (trace.val && sb.length() > 0)
            LOG.trace("Extracted Column References:" + sb);

        // Populate our map from Column objects to PlanColumn GUIDs
        for (Integer col_guid : node.getOutputColumnGUIDs()) {
            PlanColumn col = state.plannerContext.get(col_guid);
            assert (col != null) : "Invalid PlanColumn #" + col_guid;
            if (col.getExpression() != null) {
                Collection<Column> catalog_cols = ExpressionUtil.getReferencedColumns(state.catalog_db, col.getExpression());
                // If there is more than one column, then it's some sort of
                // compound expression
                // So we don't want to include in our mapping
                if (catalog_cols.size() == 1) {
                    state.addColumnMapping(CollectionUtil.first(catalog_cols), col_guid);
                }
            }
        } // FOR

    }

    // ------------------------------------------------------------
    // QUERY PLAN HELPERS
    // ------------------------------------------------------------

    /**
     * Walk up the tree in reverse so that we get all the Column offsets right
     * @param state
     * @param rootNode
     * @param force
     * @return
     */
    public static boolean updateAllColumns(final PlanOptimizerState state, final AbstractPlanNode rootNode, final boolean force) {
        for (AbstractPlanNode leafNode : PlanNodeUtil.getLeafPlanNodes(rootNode)) {
            new PlanNodeTreeWalker(false, true) {
                @Override
                protected void callback(AbstractPlanNode element) {
                    if (trace.val)
                        LOG.trace("CURRENT:\n" + PlanNodeUtil.debugNode(element));

                    // ---------------------------------------------------
                    // JOIN
                    // ---------------------------------------------------
                    if (element instanceof AbstractJoinPlanNode) {
                        if ((state.areChildrenDirty(element) || force) &&
                            PlanOptimizerUtil.updateJoinsColumns(state, (AbstractJoinPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    // ---------------------------------------------------
                    // ORDER BY
                    // ---------------------------------------------------
                    } else if (element instanceof OrderByPlanNode) {
                        if ((state.areChildrenDirty(element) || force) &&
                            PlanOptimizerUtil.updateOrderByColumns(state, (OrderByPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    }
                    // ---------------------------------------------------
                    // AGGREGATE
                    // ---------------------------------------------------
                    else if (element instanceof AggregatePlanNode) {
                        if ((state.areChildrenDirty(element) || force) &&
                            PlanOptimizerUtil.updateAggregateColumns(state, (AggregatePlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    }
                    // ---------------------------------------------------
                    // DISTINCT
                    // ---------------------------------------------------
                    else if (element instanceof DistinctPlanNode) {
                        if ((state.areChildrenDirty(element) || force) &&
                            PlanOptimizerUtil.updateDistinctColumns(state, (DistinctPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    }
                    // ---------------------------------------------------
                    // PROJECTION
                    // ---------------------------------------------------
                    else if (element instanceof ProjectionPlanNode) {
                        if ((state.areChildrenDirty(element) || force) &&
                            PlanOptimizerUtil.updateProjectionColumns(state, (ProjectionPlanNode) element) == false) {
                            this.stop();
                            return;
                        }
                    }
                    // ---------------------------------------------------
                    // SEND + RECIEVE + LIMIT
                    // ---------------------------------------------------
                    else if (element instanceof SendPlanNode || element instanceof ReceivePlanNode || element instanceof LimitPlanNode) {
                        // I think we should always call this to ensure that our
                        // offsets are ok
                        // This might be because we don't call whatever that
                        // bastardized
                        // AbstractPlanNode.updateOutputColumns() that messes
                        // everything up for us
                        if (element instanceof LimitPlanNode || state.areChildrenDirty(element)) {
                            if (element.getChildPlanNodeCount() != 1) {
                                LOG.warn("Invalid PlanNode Tree:\n" + PlanNodeUtil.debug(rootNode));
                            }
                            assert (element.getChildPlanNodeCount() == 1) : String.format("%s has %d children when it should have one: %s", element, element.getChildPlanNodeCount(),
                                    element.getChildren());
                            AbstractPlanNode child_node = element.getChild(0);
                            assert (child_node != null);
                            element.setOutputColumns(child_node.getOutputColumnGUIDs());
                            PlanOptimizerUtil.updateOutputOffsets(state, element);
                            if (trace.val)
                                LOG.trace("Set Output Columns for " + element + "\n" + PlanNodeUtil.debugNode(element));
                        }
                    }
                }
            }.traverse(leafNode);
        }
        return (true);
    }

    /**
     * Update DISTINCT columns
     * @param state
     * @param node
     * @return
     */
    public static boolean updateDistinctColumns(final PlanOptimizerState state, DistinctPlanNode node) {
        // We really have one child here
        assert (node.getChildPlanNodeCount() == 1) : node;
        AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);

        // Find the offset of our distinct column in our output. That will
        // tell us where to get the guid in the input table information
        int orig_guid = node.getDistinctColumnGuid();
        PlanColumn orig_pc = state.plannerContext.get(orig_guid);
        assert (orig_pc != null);

        node.setOutputColumns(child_node.getOutputColumnGUIDs());

        // PlanColumn new_pc = null;
        // int new_idx = 0;
        // for (Integer guid : node.getOutputColumnGUIDs()) {
        // PlanColumn pc = state.m_context.get(guid);
        // assert (pc != null);
        // if (pc.equals(orig_pc, true, true)) {
        // if (trace.val)
        // LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW: %s",
        // new_idx, orig_pc, pc));
        // new_pc = pc;
        // break;
        // }
        // new_idx++;
        // } // FOR
        // assert (new_pc != null);
        //
        //
        //
        // // Now we can update output columns and set the distinct column to be
        // // the guid
        // node.setDistinctColumnGuid(new_pc.guid());

        PlanColumn found = null;
        for (Integer new_guid : node.getOutputColumnGUIDs()) {
            PlanColumn new_pc = state.plannerContext.get(new_guid);
            assert (new_pc != null);
            if (new_pc.equals(orig_pc, true, true)) {
                found = new_pc;
                break;
            }
        } // FOR
        assert(found != null) :
            "Failed to find DistinctColumn " + orig_pc + " in " + node + " output columns";
        node.setDistinctColumnGuid(found.guid());

        state.markDirty(node);
        if (debug.val)
            LOG.debug(String.format("Updated %s with proper distinct column guid: ORIG[%d] => NEW[%d]",
                                    node, orig_guid, found.guid()));

        return (true);
    }

    /**
     * Update OrderBy columns
     * @param state
     * @param node
     * @return
     */
    public static boolean updateOrderByColumns(final PlanOptimizerState state, OrderByPlanNode node) {
        if (debug.val)
            LOG.debug("Updating Sort Columns for " + node);

        // We really have one child here
        assert (node.getChildPlanNodeCount() == 1) : node;
        AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);

        node.setOutputColumns(child_node.getOutputColumnGUIDs());
        // updateOutputOffsets(state, node);

        // Look at each of the SortColumns and make sure that it references a
        // PlanColumn
        // that is in our child node's output PlanColumns
        for (int i = 0, cnt = node.getSortColumnGuids().size(); i < cnt; i++) {
            int orig_guid = node.getSortColumnGuids().get(i);
            PlanColumn orig_pc = state.plannerContext.get(orig_guid);
            assert (orig_pc != null);
            if (trace.val)
                LOG.trace("Looking for matching PlanColumn: " + orig_pc);

            // We can't use the offset of original sort PlanColumn because the
            // number of output
            // columns in our child may have changed. So we need to loop through
            // and find the one
            // that references the same value. This will probably not work if
            // they are trying to do
            // a sort on an aggregate output value...
            Pair<PlanColumn, Integer> p = findMatchingColumn(state, orig_pc, node.getOutputColumnGUIDs());
            PlanColumn new_pc = p.getFirst();
            assert (new_pc != null);
            node.getSortColumnGuids().set(i, new_pc.guid());
            // // XXX: Can we just loop through all our PlanColumns and find the
            // one we want?
            // if (new_pc == null) {
            // for (PlanColumn pc : state.plannerContext.getAllPlanColumns()) {
            // if (pc.equals(orig_pc, true, true)) {
            // new_pc = pc;
            // }
            // } // FOR
            // }
            // if (new_pc == null) {
            // LOG.error(String.format("[%02d] Failed to find %s", i, orig_pc));
            // if (trace.val) LOG.error("PlannerContext Dump:\n" +
            // state.plannerContext.debug());
            // }
            // assert (new_pc != null);
            // if (trace.val) LOG.trace(String.format("[%02d] %s", i,
            // new_pc));
            // node.getSortColumnGuids().set(i, new_pc.guid());
        } // FOR

        state.markDirty(node);
        if (debug.val)
            LOG.debug(String.format("Updated %s with proper orderby column guid", node));

        return (true);
    }

    /**
     * Update AggregatePlanNode columns
     * @param state
     * @param node
     * @return
     */
    public static boolean updateAggregateColumns(final PlanOptimizerState state, AggregatePlanNode node) {
        // We really have one child here
        assert (node.getChildPlanNodeCount() == 1) : node;
        AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);

        for (int i = 0, cnt = node.getAggregateColumnGuids().size(); i < cnt; i++) {
            Integer orig_guid = node.getAggregateColumnGuids().get(i);
            PlanColumn orig_pc = state.plannerContext.get(orig_guid);
            assert (orig_pc != null);

            PlanColumn new_pc = null;
            int new_idx = 0;
            for (Integer guid : child_node.getOutputColumnGUIDs()) {
                PlanColumn pc = state.plannerContext.get(guid);
                assert (pc != null);
                if (pc.equals(orig_pc, true, true)) {
                    if (trace.val)
                        LOG.trace(String.format("[%02d] Found non-expression PlanColumn match:\nORIG: %s\nNEW: %s", new_idx, orig_pc, pc));
                    new_pc = pc;
                    break;
                }
                new_idx++;
            } // FOR
            if (new_pc == null) {
                LOG.error(String.format("Failed to find %d => %s\n", new_idx, new_pc));
                LOG.error(PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
            }
            assert (new_pc != null) :
                String.format("Failed to find %s at offset %d for %s", orig_pc, i, node);
            node.getAggregateColumnGuids().set(i, new_pc.guid());
        } // FOR

        // Need to update output column guids for GROUP BYs...
        for (int i = 0, cnt = node.getGroupByColumnGuids().size(); i < cnt; i++) {
            Integer orig_guid = node.getGroupByColumnGuids().get(i);
            PlanColumn orig_pc = state.plannerContext.get(orig_guid);
            assert (orig_pc != null);

            Pair<PlanColumn, Integer> p = findMatchingColumn(state, orig_pc, child_node.getOutputColumnGUIDs());
            if (p == null) {
                LOG.error(String.format("Failed to find %s's output %s from child node %s", node, orig_pc, child_node));
                LOG.error(PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
            }
            assert (p != null) : String.format("Failed to find %s's output %s from child node %s", node, orig_pc, child_node);
            PlanColumn new_pc = p.getFirst();
            assert (new_pc != null);
            if (debug.val)
                LOG.debug(String.format("[%02d] Setting %s GroupByColumnGuid to %s", i, node, new_pc));
            node.getGroupByColumnGuids().set(i, new_pc.guid());
        } // FOR

        for (int i = 0, cnt = node.getOutputColumnGUIDs().size(); i < cnt; i++) {
            Integer orig_guid = node.getOutputColumnGUIDs().get(i);
            PlanColumn orig_pc = state.plannerContext.get(orig_guid);
            assert (orig_pc != null);

            // XXX: We might need to do something different if this part of our
            // aggregate output
            if (node.getAggregateOutputColumns().contains(i) == false) {
                Pair<PlanColumn, Integer> p = findMatchingColumn(state, orig_pc, child_node.getOutputColumnGUIDs());
                PlanColumn new_pc = p.getFirst();
                assert (new_pc != null);
                if (debug.val)
                    LOG.debug(String.format("[%02d] Setting %s OutputColumnGUID to %s", i, node, new_pc));
                node.getOutputColumnGUIDs().set(i, new_pc.guid());
            }
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
        // child_node.getOutputColumnGUIDs());
        // System.err.println(PlanNodeUtil.debug(PlanNodeUtil.getRoot(agg_node)));

        state.markDirty(node);
        if (debug.val)
            LOG.debug(String.format("Updated %s with %d proper aggregate column guids", node, node.getAggregateColumnGuids().size()));
        return (true);
    }

    /**
     * 
     * @param state
     * @param node
     * @return
     */
    public static boolean updateProjectionColumns(final PlanOptimizerState state, final ProjectionPlanNode node) {
        assert (node.getChildPlanNodeCount() == 1) : node;
        final AbstractPlanNode child_node = node.getChild(0);
        assert (child_node != null);
        final List<Integer> orig_child_guids = state.orig_node_output.get(child_node);

        for (int i = 0, cnt = node.getOutputColumnGUIDCount(); i < cnt; i++) {
            // Check to make sure that the offset in the tuple value expression
            // matches
            int orig_guid = node.getOutputColumnGUID(i);
            PlanColumn orig_pc = state.plannerContext.get(orig_guid);
            assert (orig_pc != null);

            // Fix all of the offsets in the ExpressionTree
            // We have to clone it so that we don't mess up anybody else that
            // may be referencing the same PlanColumn
            AbstractExpression new_exp = null;
            try {
                new_exp = (AbstractExpression) orig_pc.getExpression().clone();
            } catch (Exception ex) {
                throw new RuntimeException("Unable to clone " + orig_pc, ex);
            }

            try {
                new ExpressionTreeWalker() {
                    @Override
                    protected void callback(AbstractExpression exp_element) {
                        if (exp_element instanceof TupleValueExpression) {
                            TupleValueExpression tv_exp = (TupleValueExpression) exp_element;
                            int orig_idx = tv_exp.getColumnIndex();
                            PlanColumn orig_child_pc = null;

                            // If this is referencing a column that we don't
                            // have a direct link to
                            // then we will see if we can match one based on its
                            // name
                            if (orig_idx >= orig_child_guids.size()) {
                                for (Integer orig_child_guid : child_node.getOutputColumnGUIDs()) {
                                    orig_child_pc = state.plannerContext.get(orig_child_guid);
                                    if (orig_child_pc.getExpression() instanceof TupleValueExpression) {
                                        TupleValueExpression orig_child_tve = (TupleValueExpression) orig_child_pc.getExpression();
                                        if (tv_exp.getTableName().equals(orig_child_tve.getTableName()) && tv_exp.getColumnAlias().equals(orig_child_tve.getColumnAlias())) {
                                            break;
                                        }
                                        orig_child_pc = null;
                                    }
                                } // FOR
                            } else {
                                orig_child_pc = state.plannerContext.get(orig_child_guids.get(orig_idx));
                            }
                            assert (orig_child_pc != null);

                            PlanColumn new_child_pc = null;
                            int new_idx = 0;
                            for (Integer orig_child_guid : child_node.getOutputColumnGUIDs()) {
                                new_child_pc = state.plannerContext.get(orig_child_guid);
                                if (orig_child_pc.equals(new_child_pc, true, true)) {
                                    break;
                                }
                                new_child_pc = null;
                                new_idx++;
                            } // FOR
                            if (new_child_pc == null)
                                LOG.warn("Problems up ahead:\n" + state + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
                            assert (new_child_pc != null) : String.format("Failed to find matching output column %s in %s", orig_child_pc, node);
                            tv_exp.setColumnIndex(new_idx);
                        }
                    }
                }.traverse(new_exp);
            } catch (Throwable ex) {
                System.err.println(PlanNodeUtil.debug(node));
                throw new RuntimeException(ex);
            }

            // Always try make a new PlanColumn and update the
            // TupleValueExpresion index
            // This ensures that we always get the ordering correct
            PlanColumn new_col = state.plannerContext.getPlanColumn(new_exp, orig_pc.getDisplayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
            assert (new_col != null);
            node.getOutputColumnGUIDs().set(i, new_col.guid());
        } // FOR
        state.markDirty(node);
        if (debug.val)
            LOG.debug(String.format("Updated %s with %d output columns offsets", node, node.getOutputColumnGUIDCount()));
        return (true);
    }

    /**
     * 
     * @param state
     * @param node
     * @return
     */
    public static boolean updateOutputOffsets(final PlanOptimizerState state, AbstractPlanNode node) {
        for (int i = 0, cnt = node.getOutputColumnGUIDCount(); i < cnt; i++) {
            // Check to make sure that the offset in the tuple value expression
            // matches
            int orig_guid = node.getOutputColumnGUID(i);
            PlanColumn orig_pc = state.plannerContext.get(orig_guid);
            assert (orig_pc != null);

            AbstractExpression orig_pc_exp = orig_pc.getExpression();
            if (!(orig_pc_exp instanceof TupleValueExpression)) {
                TupleValueExpression new_exp = new TupleValueExpression();
                new_exp.setColumnIndex(i);
                new_exp.setColumnAlias(orig_pc.getDisplayName());
                new_exp.setValueType(VoltType.STRING);
                PlanColumn new_col = state.plannerContext.getPlanColumn(new_exp, orig_pc.getDisplayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                assert (new_col != null);
                node.getOutputColumnGUIDs().set(i, new_col.guid());
            } else {
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
                        throw new RuntimeException(ex);
                    }
                    clone_exp.setColumnIndex(i);
                    PlanColumn new_col = state.plannerContext.getPlanColumn(clone_exp, orig_pc.getDisplayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                    assert (new_col != null);
                    node.getOutputColumnGUIDs().set(i, new_col.guid());
                }
            }
        } // FOR
        state.markDirty(node);
        if (debug.val)
            LOG.debug(String.format("Updated %s with %d output columns offsets", node, node.getOutputColumnGUIDCount()));
        return (true);
    }

    /**
     * @param node
     * @return
     */
    public static boolean updateJoinsColumns(final PlanOptimizerState state, final AbstractJoinPlanNode node) {

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
        final List<Integer> outer_output_guids = outer_node.getOutputColumnGUIDs();
        if (debug.val)
            LOG.debug("Calculating OUTER offsets from child node: " + outer_node);

        // Mapping from the index in the new output list to the original
        // PlanColumn guid
        final SortedMap<Integer, Integer> new_sorted_output_guids = new TreeMap<Integer, Integer>();

        // Mapping from original index to the new index
        final Map<Integer, Integer> offset_xref = new HashMap<Integer, Integer>();

        // Build a map from original offsets to the new offsets that need to be
        // stored
        // for the TupleValueExpressions (and possible TupleAddressExpression)
        final List<Integer> outer_orig_input_guids = state.orig_node_output.get(outer_node);
        assert (outer_orig_input_guids != null);
        StringBuilder sb = new StringBuilder();
        for (int orig_idx = 0, cnt = outer_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
            int orig_col_guid = outer_orig_input_guids.get(orig_idx);
            PlanColumn orig_pc = state.plannerContext.get(orig_col_guid);

            // Figure out what the new PlanColumn GUID is for this column
            // It may be the case that we need to make a new one because the
            // underlying expression has the wrong offsets
            // Find the new index of this same PlanColumn guid in the outer
            // table's output columns
            Integer new_idx = outer_output_guids.indexOf(orig_col_guid);

            // If this column is not in the outer table's output columns
            if (new_idx != -1) {
                // PlanColumn new_pc = state.plannerContext.get(orig_col_guid);
                // new_output_guids.add(orig_col_guid);
                new_sorted_output_guids.put(new_idx, orig_col_guid);
                if (debug.val)
                    LOG.debug(String.format("[%02d] Remapped PlanColumn to new offset %02d", orig_idx, new_idx));
            }
            // Check whether we even have this column. We'll compare everything
            // but the Expression
            else {
                Pair<PlanColumn, Integer> p = findMatchingColumn(state, orig_pc, outer_output_guids);

                // If we have this PlanColumn, then we need to clone it and set
                // the new column index
                // Make sure that we replace update outer_new_input_guids
                if (p != null) {
                    // PlanColumn new_pc = p.getFirst();
                    assert (p.getFirst() != null);
                    new_idx = p.getSecond();

                    TupleValueExpression clone_exp = null;
                    try {
                        clone_exp = (TupleValueExpression) orig_pc.getExpression().clone();
                    } catch (CloneNotSupportedException ex) {
                        throw new RuntimeException(ex);
                    }
                    clone_exp.setColumnIndex(new_idx);
                    PlanColumn new_col = state.plannerContext.getPlanColumn(clone_exp, orig_pc.getDisplayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
                    assert (new_col != null);
                    outer_output_guids.set(new_idx, new_col.guid());
                    // new_output_guids.add(new_col.guid());
                    new_sorted_output_guids.put(new_idx, new_col.guid());
                    if (debug.val)
                        LOG.debug(String.format("OUTER OFFSET %d => %d [new_guid=%d]", orig_idx, new_idx, new_col.guid()));
                }
                // If we don't have this PlanColumn, that means that it isn't
                // being passed up from the
                // outer table and therefore don't want it anymore in our output
                else {
                    new_idx = null;
                }
            }

            if (new_idx != null) {
                assert (offset_xref.containsKey(orig_idx) == false) : orig_idx + " ==> " + offset_xref;
                offset_xref.put(orig_idx, new_idx);
            }
            // Just because we couldn't find the offset doesn't mean it's a bad
            // thing
            // It might be because we projected those columns out down below in
            // the tree
            // and therefore we don't need to worry about them anymore.
            else {
                String msg = String.format("[%02d] Failed to find new offset for OUTER %s", orig_idx, orig_pc);
                sb.append(msg).append("\n");
                if (debug.val)
                    LOG.warn(msg);
            }
        } // FOR
        if (trace.val) {
            LOG.trace("Original Outer Input GUIDs: " + outer_orig_input_guids);
            LOG.trace("New Outer Input GUIDs: " + outer_output_guids);
        }
        if (outer_output_guids.size() != offset_xref.size()) {
            LOG.error("Outer Node: " + outer_node);

            String temp = "";
            for (int i = 0; i < outer_orig_input_guids.size(); i++) {
                PlanColumn pc = state.plannerContext.get(outer_orig_input_guids.get(i));
                temp += String.format("[%02d] %s\n", i, pc);
                temp += ExpressionUtil.debug(pc.getExpression()) + "\n--------\n";
            }
            temp += "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
            LOG.error("Original Outer Input GUIDs: " + outer_orig_input_guids + "\n" + temp);

            temp = "";
            for (int i = 0; i < outer_output_guids.size(); i++) {
                PlanColumn pc = state.plannerContext.get(outer_output_guids.get(i));
                temp += String.format("[%02d] %s\n", i, pc);
                temp += ExpressionUtil.debug(pc.getExpression()) + "\n--------\n";
            }
            LOG.error("New Outer Input GUIDs: " + outer_output_guids + "\n" + temp);

            LOG.error("Output Xref Offsets: " + offset_xref);
            // LOG.info("Trace Information:\n" + sb);
            LOG.error("Unexpected Query Plan\n" + PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
        }
        assert (outer_output_guids.size() == offset_xref.size()) : "outer_new_input_guids size: " + outer_output_guids.size() + " offset_xref size: " + offset_xref.size();

        // add the sorted columns into new_columns list
        final List<Integer> new_output_guids = new ArrayList<Integer>(new_sorted_output_guids.values());

        // For the inner table, we always have to offset ourselves based on the
        // size of the new outer table
        int offset = outer_output_guids.size();

        AbstractPlanNode inner_node = null;

        // These are the set of expressions for the join clause that we need to
        // fix their offsets for
        final Collection<AbstractExpression> expressions_to_fix = PlanNodeUtil.getExpressionsForPlanNode(node);

        // --------------------------------------------
        // NEST LOOP
        // --------------------------------------------
        if (node.getChildPlanNodeCount() > 1) {
            assert (node instanceof NestLoopPlanNode);
            inner_node = node.getChild(1);
            if (debug.val)
                LOG.debug("Calculating INNER offsets from child node: " + inner_node);

            List<Integer> inner_orig_input_guids = state.orig_node_output.get(inner_node);
            assert (inner_orig_input_guids != null);
            List<Integer> inner_new_input_guids = inner_node.getOutputColumnGUIDs();

            for (int orig_idx = 0, cnt = inner_orig_input_guids.size(); orig_idx < cnt; orig_idx++) {
                int col_guid = inner_orig_input_guids.get(orig_idx);

                // Find the new index of this same PlanColumn guid
                int new_idx = inner_new_input_guids.indexOf(col_guid);
                if (new_idx != -1) {
                    int offset_orig_idx = outer_orig_input_guids.size() + orig_idx;
                    int offset_new_idx = offset + new_idx;
                    if (trace.val)
                        LOG.trace(String.format("INNER NODE OFFSET %d => %d", offset_orig_idx, offset_new_idx));
                    assert (offset_xref.containsKey(offset_orig_idx) == false) : orig_idx + " ==> " + offset_xref;
                    offset_xref.put(offset_orig_idx, offset_new_idx);
                    new_output_guids.add(col_guid);
                    // sorted_new_output_guids.put(new_idx, col_guid);
                } else {
                    PlanColumn pc = state.plannerContext.get(col_guid);
                    LOG.warn("Failed to find new offset for INNER " + pc);
                }
            } // FOR
            if (trace.val)
                LOG.trace("Original Inner Input GUIDs: " + inner_orig_input_guids);
            if (trace.val)
                LOG.trace("New Inner Input GUIDs: " + inner_new_input_guids);

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
                catalog_tbl = CollectionUtil.first(CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, idx_node));
            } catch (Exception ex) {
                LOG.fatal(ex);
                throw new RuntimeException(ex);
            }
            assert (catalog_tbl != null);
            if (debug.val)
                LOG.debug("Calculating INNER offsets from INLINE Scan: " + catalog_tbl);

            for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
                int i = catalog_col.getIndex();
                int offset_orig_idx = outer_orig_input_guids.size() + i;
                int offset_new_idx = offset + i;
                if (trace.val)
                    LOG.trace(String.format("INNER INLINE OFFSET %d => %d", offset_orig_idx, offset_new_idx));
                offset_xref.put(offset_orig_idx, offset_new_idx);

                // Since we're going in order, we know what column is at this
                // position.
                // That means we can grab the catalog object and convert it to a
                // PlanColumn GUID
                // Always try make a new PlanColumn and update the
                // TupleValueExpresion index
                // This ensures that we always get the ordering correct
                // int orig_guid =
                // idx_node.getOutputColumnGUID(offset_orig_idx);
                int orig_guid = CollectionUtil.first(state.column_guid_xref.get(catalog_col));
                assert (orig_guid != -1);
                PlanColumn orig_pc = state.plannerContext.get(orig_guid);
                assert (orig_pc != null);

                // PlanColumn new_pc = null;
                // int new_idx = 0;
                // for (Integer guid : idx_node.getOutputColumnGUIDs()) {
                // PlanColumn pc = state.m_context.get(guid);
                // assert (pc != null);
                // if (pc.equals(orig_pc, true, true)) {
                // if (trace.val)
                // LOG.trace(String.format("[%02d] Found inline output PlanColumn match:\nORIG: %s\nNEW: %s",
                // new_idx, orig_pc, pc));
                // new_pc = pc;
                // break;
                // }
                // new_idx++;
                // } // FOR
                // assert (new_pc != null);

                idx_node.getOutputColumnGUIDs().set(i, orig_pc.guid());
                new_output_guids.add(orig_pc.guid());
                // sorted_new_output_guids.put(i,orig_pc.guid());
                // TupleValueExpression clone_exp =
                // (TupleValueExpression)orig_col.getExpression().clone();
                // clone_exp.setColumnIndex(offset_new_idx);
                // Storage storage = (catalog_tbl.getIsreplicated() ?
                // Storage.kReplicated : Storage.kPartitioned);
                // PlanColumn new_col = state.m_context.getPlanColumn(clone_exp,
                // orig_col.displayName(), orig_col.getSortOrder(), storage);
                // assert(new_col != null);

            } // FOR

            // We also need to fix all of the search key expressions used in the
            // inline scan
            expressions_to_fix.addAll(PlanNodeUtil.getExpressionsForPlanNode(idx_node));
            // System.out.println("expressions_to_fix: " + expressions_to_fix);
        }
        if (debug.val) {
            LOG.debug("Output Xref Offsets: " + offset_xref);
            LOG.debug("New Output Columns GUIDS: " + new_sorted_output_guids);
        }

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
                        if (new_idx == null) {
                            LOG.debug(PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)));
                            LOG.debug(state.plannerContext.debug());
                        }
                        assert (new_idx != null) : String.format("Missing New Offset of Original Offset %02d:\n%s", orig_idx, ExpressionUtil.debug(tv_exp));
                        if (orig_idx != new_idx) {
                            if (debug.val)
                                LOG.debug(String.format("Changing offset for %s.%s [%d ==> %d]", tv_exp.getTableName(), tv_exp.getColumnName(), orig_idx, new_idx));
                            tv_exp.setColumnIndex(new_idx);
                        }

                    }
                }
            }.traverse(exp);
        }

        // Then update the output columns to reflect the change
        node.setOutputColumns(new_output_guids);
        for (int new_idx = 0, cnt = node.getOutputColumnGUIDs().size(); new_idx < cnt; new_idx++) {
            Integer col_guid = node.getOutputColumnGUIDs().get(new_idx);
            PlanColumn pc = state.plannerContext.get(col_guid);

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
            // String.format("Offset Mismatch [orig_idx=%d] => [%d] != [%d]:\noffset_xref = %s\n%s",
            // orig_idx, new_idx, offset_xref.get(orig_idx), offset_xref,
            // PlanNodeUtil.debugNode(element));
            if (orig_idx != new_idx) {
                TupleValueExpression clone_exp = null;
                try {
                    clone_exp = (TupleValueExpression) tv_exp.clone();
                } catch (Exception ex) {
                    LOG.fatal(ex);
                    throw new RuntimeException(ex);
                }
                assert (clone_exp != null);

                // compare with child's output columns to see whether orig_idx
                // or new_idx is correct
                assert (node.getChildPlanNodeCount() == 1);
                List<Integer> child_output = node.getChild(0).getOutputColumnGUIDs();
                if (orig_idx < child_output.size() && pc.guid() == child_output.get(orig_idx)) {
                    clone_exp.setColumnIndex(orig_idx);
                } else {
                    clone_exp.setColumnIndex(new_idx);
                }
                PlanColumn new_pc = state.plannerContext.getPlanColumn(clone_exp, pc.getDisplayName(), pc.getSortOrder(), pc.getStorage());
                assert (new_pc != null);
                node.getOutputColumnGUIDs().set(new_idx, new_pc.guid());
            }
            if (trace.val)
                LOG.trace(String.format("OUTPUT[%d] => %s", new_idx, state.plannerContext.get(node.getOutputColumnGUIDs().get(new_idx))));
        } // FOR

        // IMPORTANT: If the inner_node is inline (meaning it was a
        // NestLoopIndex), then we need to also update
        // its output columns to match our new ones. This is necessary because
        // the nestloopindexexecutor will
        // generate its output table from the inline node and not the actual
        // output columns
        if (inner_node.isInline()) {
            assert (inner_node instanceof IndexScanPlanNode);
            inner_node.setOutputColumns(node.getOutputColumnGUIDs());
            if (trace.val)
                LOG.trace("Updated INNER inline " + inner_node + " output columns");
        }

        // if (debug.val) LOG.debug("PlanNodeTree:\n" +
        // PlanNodeUtil.debug(rootNode));
        // LOG.debug(PlanNodeUtil.debugNode(element));

        state.markDirty(node);

        return (true);
    }

    /**
     * Correct any offsets in join nodes
     *
     * @param root
     */
    public static void fixJoinColumnOffsets(final PlanOptimizerState state, AbstractPlanNode root) {
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element instanceof NestLoopPlanNode || element instanceof NestLoopIndexPlanNode) {
                    // Make sure the column reference offsets of the output
                    // column are consecutive
                    // If it doesn't match, then we'll have to make a new
                    // PlanColumn
                    for (int i = 0, cnt = element.getOutputColumnGUIDCount(); i < cnt; i++) {
                        Integer col_guid = element.getOutputColumnGUID(i);
                        PlanColumn pc_col = state.plannerContext.get(col_guid);
                        assert (pc_col != null) : "Missing output column " + i + " for " + element;
                        AbstractExpression exp = pc_col.getExpression();

                        if (exp.getExpressionType() == ExpressionType.VALUE_TUPLE && ((TupleValueExpression) exp).getColumnIndex() != i) {
                            // NOTE: You can't just update the
                            // TupleValueExpression because other nodes might be
                            // referencing it. We have to clone the expression
                            // tree, update the offset and then register
                            // the PlanColumn
                            TupleValueExpression clone_exp = null;
                            try {
                                clone_exp = (TupleValueExpression) exp.clone();
                            } catch (CloneNotSupportedException ex) {
                                LOG.fatal("Unexpected error", ex);
                                throw new RuntimeException(ex);
                            }
                            assert (clone_exp != null);
                            clone_exp.setColumnIndex(i);

                            PlanColumn new_col = state.plannerContext.getPlanColumn(clone_exp, pc_col.getDisplayName(), pc_col.getSortOrder(), pc_col.getStorage());
                            assert (new_col != null);
                            assert (new_col != pc_col);
                            element.getOutputColumnGUIDs().set(i, new_col.guid());
                            if (trace.val)
                                LOG.trace(String.format("Updated %s Output Column at position %d: %s", element, i, new_col));
                        }
                    } // FOR
                }
            }
        }.traverse(root);
    }

    /**
     * Extract all the PlanColumns that we are going to need in the query plan
     * tree above the given node.
     *
     * @param node
     * @param tables
     * @return
     */
    public static Set<PlanColumn> extractReferencedColumns(final PlanOptimizerState state, final AbstractPlanNode node) {
        if (debug.val)
            LOG.debug("Extracting referenced column set for " + node);

        // Walk up the tree from the current node and figure out what columns
        // that we need from it are
        // referenced. This will tell us how many we can actually project out at
        // this point
        final Collection<Integer> col_guids = new ListOrderedSet<Integer>();
        new PlanNodeTreeWalker(true, true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // If this is the same node that we're examining, then we can
                // skip it
                // Otherwise, anything that this guy references but nobody else
                // does
                // will incorrectly get included in the projection
                if (element == node)
                    return;

                if (trace.val)
                    LOG.trace("Examining " + element + " :: " + this.getVisitPath());
                int ctr = 0;

                // ---------------------------------------------------
                // ProjectionPlanNode
                // AbstractScanPlanNode
                // AggregatePlanNode
                // ---------------------------------------------------
                if (element instanceof ProjectionPlanNode || element instanceof AbstractScanPlanNode || element instanceof AggregatePlanNode) {

                    // This is set can actually be null because we can
                    // parallelize certain operations on each node
                    // so that we don't have to send the entire data set back to
                    // the base partition
                    Collection<Column> col_set = state.getPlanNodeColumns(element);

                    // Check whether we're the top-most join, or that we don't
                    // have any referenced columns
                    if (col_set == null) {
                        ctr += element.getOutputColumnGUIDCount();
                        col_guids.addAll(element.getOutputColumnGUIDs());
                    } else {
                        for (Column col : col_set) {
                            col_guids.add(CollectionUtil.first(state.column_guid_xref.get(col)));
                            ctr++;
                        } // FOR
                    }
                }
                // ---------------------------------------------------
                // DistinctPlanNode
                // ---------------------------------------------------
                else if (element instanceof DistinctPlanNode) {
                    ctr++;
                    col_guids.add(((DistinctPlanNode) element).getDistinctColumnGuid());
                }
                // ---------------------------------------------------
                // OrderByPlanNode
                // ---------------------------------------------------
                else if (element instanceof OrderByPlanNode) {
                    ctr += ((OrderByPlanNode) element).getSortColumnGuids().size();
                    col_guids.addAll(((OrderByPlanNode) element).getSortColumnGuids());
                }

                if (debug.val && ctr > 0)
                    LOG.debug(String.format("%s -> Found %d PlanColumns referenced in %s", node, ctr, element));
            }
        }.traverse(node);
        if (debug.val)
            LOG.debug(String.format("Referenced PlanColumns for %s: %s", node, col_guids));

        // Now extract the TupleValueExpression and get the PlanColumn that we
        // really want

        // For each of the PlanColumn Guids that were referenced up above, check
        // to see whether
        // it references a column that our PlanNode knows about in its output
        // columns
        Set<PlanColumn> ref_columns = new ListOrderedSet<PlanColumn>();
        for (Integer col_guid : col_guids) {
            PlanColumn above_pc = state.plannerContext.get(col_guid);

            // Check whether we have anything similar to it in our output
            // If we do, then we want to... do something... I don't know what...
            Pair<PlanColumn, Integer> p = findMatchingColumn(state, above_pc, node.getOutputColumnGUIDs());
            if (p != null) {
                // Now look to see whether we already have a reference to a
                // similar PlanColumn
                // in our set of referenced columns
                boolean exists = false;
                for (PlanColumn existing : ref_columns) {
                    if (above_pc.equals(existing, false, true)) {
                        exists = true;
                        break;
                    }
                } // FOR
                  // We didn't find it in our existing set, so we'll want to add
                  // it
                if (exists == false) {
                    ref_columns.add(above_pc);
                    if (trace.val)
                        LOG.trace(String.format("Added PlanColumn #%d to list of referenced columns. [%s]", col_guid, above_pc.getDisplayName()));
                } else if (trace.val) {
                    LOG.trace("Skipped PlanColumn #" + col_guid + " because it already exists.");
                }
            }
        } // FOR

        return (ref_columns);
    }
}