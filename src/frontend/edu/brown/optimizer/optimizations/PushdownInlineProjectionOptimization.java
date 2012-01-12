package edu.brown.optimizer.optimizations;

import org.apache.log4j.Logger;
import org.voltdb.plannodes.*;

import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.optimizer.PlanOptimizerUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;

/**
 * Add inline ProjectionPlanNode to the bottom most scan node
 * @author pavlo
 */
public class PushdownInlineProjectionOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(PushdownInlineProjectionOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());

    public PushdownInlineProjectionOptimization(PlanOptimizerState state) {
        super(state);
    }
    
    @Override
    public AbstractPlanNode optimize(final AbstractPlanNode rootNode) {
        // We will only do this if we have a JOIN
        if (state.join_tbl_mapping.isEmpty()) {
            if (debug.get())
                LOG.debug("Not attempting to pushdown inline ProjectionPlanNode because there are no joins");
            return (rootNode);
        }
        
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // ---------------------------------------------------
                // LEAF SCANS
                // ---------------------------------------------------
                if (element.getChildPlanNodeCount() == 0 && element instanceof AbstractScanPlanNode) {
                    AbstractScanPlanNode scan_node = (AbstractScanPlanNode) element;
                    try {
                        if (PlanOptimizerUtil.addInlineProjection(state, scan_node) == false) {
                            this.stop();
                            return;
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to add inline projection to " + element, ex);
                    }
                }
                // ---------------------------------------------------
                // JOIN
                // ---------------------------------------------------
                else if (element instanceof AbstractJoinPlanNode) {
                    AbstractJoinPlanNode join_node = (AbstractJoinPlanNode) element;
                    try {
                        if (state.areChildrenDirty(join_node) &&
                            PlanOptimizerUtil.updateJoinsColumns(state, join_node) == false) {
                            this.stop();
                            return;
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to update join columns in " + element, ex);
                    }
                }
                // ---------------------------------------------------
                // DISTINCT
                // ---------------------------------------------------
                else if (element instanceof DistinctPlanNode) {
                    if (state.areChildrenDirty(element) &&
                        PlanOptimizerUtil.updateDistinctColumns(state, (DistinctPlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                }
                // ---------------------------------------------------
                // AGGREGATE
                // ---------------------------------------------------
                else if (element instanceof AggregatePlanNode) {
                    if (state.areChildrenDirty(element) &&
                        PlanOptimizerUtil.updateAggregateColumns(state, (AggregatePlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                }
                // ---------------------------------------------------
                // ORDER BY
                // ---------------------------------------------------
                else if (element instanceof OrderByPlanNode) {
                    if (state.areChildrenDirty(element) &&
                        PlanOptimizerUtil.updateOrderByColumns(state, (OrderByPlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                }
                // ---------------------------------------------------
                // PROJECTION
                // ---------------------------------------------------
                else if (element instanceof ProjectionPlanNode) {
                    if (state.areChildrenDirty(element) &&
                        PlanOptimizerUtil.updateProjectionColumns(state, (ProjectionPlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                }
                // ---------------------------------------------------
                // SEND/RECEIVE/LIMIT
                // ---------------------------------------------------
                else if (element instanceof SendPlanNode || element instanceof ReceivePlanNode || element instanceof LimitPlanNode) {
                    // I think we should always call this to ensure that our offsets are ok
                    // This might be because we don't call whatever that bastardized
                    // AbstractPlanNode.updateOutputColumns() that messes everything up for us
                    if (element instanceof LimitPlanNode || state.areChildrenDirty(element)) {
                        assert (element.getChildPlanNodeCount() == 1) : element;
                        AbstractPlanNode child_node = element.getChild(0);
                        assert (child_node != null);
                        element.setOutputColumns(child_node.getOutputColumnGUIDs());
                        PlanOptimizerUtil.updateOutputOffsets(state, element);
                    }
                }
            }
        }.traverse(rootNode);
        return (rootNode);
    }
}
