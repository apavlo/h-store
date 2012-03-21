package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanColumn;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.Pair;

import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public class RemoveRedundantProjectionsOptimizations extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(RemoveRedundantProjectionsOptimizations.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());

    private AbstractPlanNode new_root;
    final AtomicBoolean modified = new AtomicBoolean(false);

    public RemoveRedundantProjectionsOptimizations(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(final AbstractPlanNode rootNode) {
        this.new_root = rootNode;
        this.modified.set(false);

        new PlanNodeTreeWalker(false) {
            protected void callback(AbstractPlanNode element) {

                // CASE #1
                // If we are a ScanNode that has an inline projection with
                // the same number of output columns as the scanned table
                // in the same order that are straight TupleValueExpression,
                // then
                // we know that we're just dumping out the table, so therefore
                // we don't need the projection at all!
                if (element instanceof AbstractScanPlanNode) {
                    AbstractScanPlanNode scan_node = (AbstractScanPlanNode) element;
                    ProjectionPlanNode proj_node = element.getInlinePlanNode(PlanNodeType.PROJECTION);
                    if (proj_node == null)
                        return;

                    String table_name = scan_node.getTargetTableName();
                    Table catalog_tbl = state.catalog_db.getTables().get(table_name);
                    assert (catalog_tbl != null) : "Unexpected table '" + table_name + "'";
                    if (catalog_tbl.getColumns().size() != proj_node.getOutputColumnGUIDCount())
                        return;

                    for (int i = 0, cnt = catalog_tbl.getColumns().size(); i < cnt; i++) {
                        int col_guid = proj_node.getOutputColumnGUID(i);
                        PlanColumn plan_col = state.plannerContext.get(col_guid);
                        assert (plan_col != null);

                        AbstractExpression col_exp = plan_col.getExpression();
                        assert (col_exp != null);
                        if ((col_exp instanceof TupleValueExpression) == false)
                            return;

                        Collection<Column> columns = ExpressionUtil.getReferencedColumns(state.catalog_db, col_exp);
                        assert (columns.size() == 1);
                        Column catalog_col = CollectionUtil.first(columns);
                        if (catalog_col.getIndex() != i)
                            return;
                    } // FOR

                    // If we're here, the new know that we can remove the
                    // projection
                    scan_node.removeInlinePlanNode(PlanNodeType.PROJECTION);
                    modified.set(true);

                    if (debug.get())
                        LOG.debug(String.format("PLANOPT - Removed redundant %s from %s\n%s", proj_node, scan_node, PlanNodeUtil.debug(rootNode)));
                }

                // CASE #2
                // We are ProjectionPlanNode that references the same columns as
                // one further below.
                // That means we are unnecessary and can be removed!
                if (element instanceof ProjectionPlanNode) {
                    // Find the first ProjectionPlanNode below us
                    ProjectionPlanNode next_proj = getFirstProjection(element);
                    assert (next_proj == null || next_proj != element);
                    if (next_proj == null) {
                        if (debug.get())
                            LOG.debug("SKIP - No other Projection found below " + element);
                        return;
                    }
                    // Check whether we have the same output columns
                    else if (element.getOutputColumnGUIDs().equals(next_proj.getOutputColumnGUIDs()) == false) {
                        if (debug.get())
                            LOG.debug(String.format("SKIP - %s and %s have different output columns", element, next_proj));
                        return;
                    }

                    // Ok so we need to remove it. But we need to check whether
                    // our child node
                    // will become the new root of the tree
                    if (element.getParentPlanNodeCount() == 0) {
                        assert (element.getChildPlanNodeCount() == 1) : "Projection element expected 1 child but has " + element.getChildPlanNodeCount() + " children";
                        new_root = element.getChild(0);
                        if (debug.get())
                            LOG.debug("PLANOPT - Promoted " + new_root + " as the new query plan root!");
                    }

                    // Off with it's head!
                    element.removeFromGraph();
                    modified.set(true);
                    if (debug.get())
                        LOG.debug("PLANOPT - Removed redundant " + element + " from query plan!");
                }
            }
        }.traverse(rootNode);

        assert (this.new_root != null);
        return (Pair.of(modified.get(), this.new_root));
    }

    private ProjectionPlanNode getFirstProjection(final AbstractPlanNode root) {
        final ProjectionPlanNode proj_node[] = { null };
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element != root && element instanceof ProjectionPlanNode) {
                    proj_node[0] = (ProjectionPlanNode) element;
                    this.stop();
                }
            }
        }.traverse(root);
        return (proj_node[0]);
    }

}
