package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.planner.PlanColumn;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.Pair;

import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.optimizer.PlanOptimizerUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public class ProjectionPushdownOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(ProjectionPushdownOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    private final Set<PlanColumn> new_output_cols = new ListOrderedSet<PlanColumn>();
    private final Set<Table> new_output_tables = new HashSet<Table>();
    private final AtomicBoolean modified = new AtomicBoolean(false);
    
    public ProjectionPushdownOptimization(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(final AbstractPlanNode root) {
        modified.set(false);
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // ---------------------------------------------------
                // ADD INLINE PROJECTION TO LEAF SCANS
                // ---------------------------------------------------
                if (element.getChildPlanNodeCount() == 0 && element instanceof AbstractScanPlanNode) {
                    if (element.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
                        if (debug.val)
                            LOG.debug("SKIP - " + element + " already has an inline ProjectionPlanNode");
                    } else {
                        try {
                            if (addProjection(element, true) == false) {
                                this.stop();
                                return;
                            }
                        } catch (Throwable ex) {
                            throw new RuntimeException("Failed to add projection for " + element, ex);
                        }
                        modified.set(true);
                    }
                }
                // ---------------------------------------------------
                // Distributed NestLoopIndexPlanNode
                // This is where the NestLoopIndexPlanNode immediately passes
                // its intermediate results to a SendPlanNode
                // ---------------------------------------------------
                else if (element instanceof NestLoopIndexPlanNode && element.getParent(0) instanceof SendPlanNode) {
                    assert (state.join_node_index.size() == state.join_tbl_mapping.size()) : "Join data structures don't have the same size";
                    if (state.join_tbl_mapping.get(element) == null)
                        LOG.error("BUSTED:\n" + state);
                    assert (state.join_tbl_mapping.get(element) != null) : element + " does NOT exist in join map";

                    try {
                        if (addProjection(element, false) == false) {
                            this.stop();
                            return;
                        }
                    } catch (Throwable ex) {
                        throw new RuntimeException("Failed to add projection for " + element, ex);
                    }
                    modified.set(true);
                }
            }
        }.traverse(root);
        return (Pair.of(modified.get(), root));
    }

    private boolean addProjection(final AbstractPlanNode node, boolean inline) {
        assert ((node instanceof ProjectionPlanNode) == false) : String.format("Trying to add a new Projection after " + node);

        // Look at the output columns of the given AbstractPlanNode and figure out
        // which of those columns we're going to need up above in the tree
        Set<PlanColumn> referenced = PlanOptimizerUtil.extractReferencedColumns(state, node);
        if (debug.val)
            LOG.debug(String.format("All referenced columns above %s:\n%s", node, StringUtil.join("\n", referenced)));

        // Look over the PlanColumns that we need above us, and add the ones that
        // are coming out of the parent. Those are the ones that we want to include
        new_output_cols.clear();
        new_output_tables.clear();
        for (Integer col_guid : node.getOutputColumnGUIDs()) {
            PlanColumn col = state.plannerContext.get(col_guid);
            assert (col != null) : "Invalid PlanColumn #" + col_guid;
            for (PlanColumn ref_col : referenced) {
                if (ref_col.equals(col, true, true)) {
                    new_output_cols.add(col);
                    new_output_tables.addAll(ExpressionUtil.getReferencedTables(state.catalog_db, col.getExpression()));
                    break;
                }
            }
        } // FOR
        if (new_output_cols.isEmpty())
            LOG.warn("BUSTED:\n" + PlanNodeUtil.debug(PlanNodeUtil.getRoot(node)) + "\n" + state);
        assert (new_output_cols.isEmpty() == false) : String.format("No new output columns for " + node);

        // Check whether the output PlanColumns all reference the same table,
        // and we have the same number of PlanColumns as that table has in the
        // catalog
        // This means that we don't need the projection because they're doing a
        // SELECT *
        if (new_output_tables.size() == 1) {
            Table catalog_tbl = CollectionUtil.first(new_output_tables);
            assert (catalog_tbl != null);
            // Only create the projection if the number of columns we need to
            // output is less then the total number of columns for the table
            if (new_output_cols.size() == catalog_tbl.getColumns().size()) {
                if (debug.val)
                    LOG.debug("SKIP - All columns needed in query. No need for inline projection on " + catalog_tbl);
                return (false);
            }
        }

        // Now create a new ProjectionPlanNode that outputs just those columns
        ProjectionPlanNode proj_node = new ProjectionPlanNode(state.plannerContext, PlanAssembler.getNextPlanNodeId());
        this.populateProjectionPlanNode(node, proj_node, new_output_cols);

        // Add a projection above this node
        // TODO: Add the ability to automatically check whether we can make
        // something inline
        if (inline) {
            // Add projection inline to scan node
            node.addInlinePlanNode(proj_node);
            assert (proj_node.isInline());

            // Then make sure that we update it's output columns to match the
            // inline output
            node.getOutputColumnGUIDs().clear();
            node.getOutputColumnGUIDs().addAll(proj_node.getOutputColumnGUIDs());
        } else {
            AbstractPlanNode parent = node.getParent(0);
            assert (parent != null);
            node.clearParents();
            parent.clearChildren();
            proj_node.addAndLinkChild(node);
            parent.addAndLinkChild(proj_node);
        }

        // Mark the new ProjectionPlanNode as dirty
        state.markDirty(proj_node);
        state.markDirty(node);

        if (debug.val)
            LOG.debug(String.format("PLANOPT - Added %s%s with %d columns for node %s", (inline ? "inline " : ""), proj_node, proj_node.getOutputColumnGUIDCount(), node));

        return (true);
    }

    private void populateProjectionPlanNode(AbstractPlanNode parent, ProjectionPlanNode proj_node, Collection<PlanColumn> output_columns) {
        if (debug.val)
            LOG.debug(String.format("Populating %s with %d output PlanColumns", proj_node, output_columns.size()));

        AbstractExpression orig_col_exp = null;
        for (PlanColumn plan_col : output_columns) {
            int orig_guid = -1;
            for (Integer guid : parent.getOutputColumnGUIDs()) {
                PlanColumn output_plan_column = state.plannerContext.get(guid);
                if (plan_col.equals(output_plan_column, true, true)) {
                    orig_col_exp = output_plan_column.getExpression();
                    orig_guid = guid;
                    break;
                }
            }
            if (orig_guid == -1) {
                LOG.warn(String.format("Failed to find PlanColumn %s in %s output columns?", plan_col, parent));
            } else {
                assert (orig_col_exp != null);
                AbstractExpression new_col_exp = null;
                try {
                    new_col_exp = (AbstractExpression) orig_col_exp.clone();
                } catch (CloneNotSupportedException ex) {
                    throw new RuntimeException(ex);
                }
                assert (new_col_exp != null);
                PlanColumn new_plan_col = null;
                if (new_col_exp instanceof TupleValueExpression) {
                    new_plan_col = new PlanColumn(orig_guid, new_col_exp, ((TupleValueExpression) new_col_exp).getColumnName(), plan_col.getSortOrder(), plan_col.getStorage());
                    proj_node.appendOutputColumn(new_plan_col);
                    if (debug.val)
                        LOG.debug("Added " + new_plan_col + " to " + proj_node);
                } else if (debug.val) {
                    LOG.debug("Skipped adding " + new_plan_col + " to " + proj_node + "?");
                }
            }
        } // FOR
    }

}
