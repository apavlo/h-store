package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.planner.PlanColumn;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.Pair;

import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public class AggregatePushdownOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(AggregatePushdownOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();

    public AggregatePushdownOptimization(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(AbstractPlanNode rootNode) {
        Collection<HashAggregatePlanNode> nodes = PlanNodeUtil.getPlanNodes(rootNode, HashAggregatePlanNode.class);
        if (nodes.size() != 1) {
            if (debug.val) LOG.debug("SKIP - Not an aggregate query plan");
            return Pair.of(false, rootNode);
        }
        final HashAggregatePlanNode node = CollectionUtil.first(nodes);
        
        // Skip single-partition query plans
        if (PlanNodeUtil.isDistributedQuery(rootNode) == false) {
            if (debug.val) LOG.debug("SKIP - Not a distributed query plan");
            return (Pair.of(false, rootNode));
        }
// // Right now, Can't do averages
// for (ExpressionType et: node.getAggregateTypes()) {
// if (et.equals(ExpressionType.AGGREGATE_AVG)) {
// if (debug.val) LOG.debug("SKIP - Right now can't optimize AVG()");
// return (Pair.of(false, rootNode));
// }
// }
        
        // Get the AbstractScanPlanNode that is directly below us
        Collection<AbstractScanPlanNode> scans = PlanNodeUtil.getPlanNodes(node, AbstractScanPlanNode.class);
        if (debug.val) LOG.debug("<ScanPlanNodes>: "+ scans);
        if (scans.size() != 1) {
            if (debug.val)
                LOG.debug("SKIP - Multiple scans!");
            return (Pair.of(false, rootNode));
        }
        
        if (debug.val) LOG.debug("Trying to apply Aggregate pushdown optimization!");
        AbstractScanPlanNode scan_node = CollectionUtil.first(scans);
        assert (scan_node != null);
        
// // For some reason we have to do this??
// for (int col = 0, cnt = scan_node.getOutputColumnGUIDs().size(); col < cnt; col++) {
// int col_guid = scan_node.getOutputColumnGUIDs().get(col);
// assert (state.plannerContext.get(col_guid) != null) : "Failed [" + col_guid + "]";
// // PlanColumn retval = new PlanColumn(guid, expression, columnName,
// // sortOrder, storage);
// } // FOR
        
        // Skip if we're already directly after the scan (meaning no network traffic)
        if (scan_node.getParent(0).equals(node)) {
            if (debug.val)
                LOG.debug("SKIP - Aggregate does not need to be distributed");
            return (Pair.of(false, rootNode));
        }
        
        // Check if this is COUNT(DISTINCT) query
        // If it is then we can only pushdown the DISTINCT
        AbstractPlanNode clone_node = null;
        if (node.getAggregateTypes().contains(ExpressionType.AGGREGATE_COUNT)) {
            for (AbstractPlanNode child : node.getChildren()) {
                if (child.getClass().equals(DistinctPlanNode.class)) {
                    try {
                        clone_node = (AbstractPlanNode) child.clone(false, true);
                    } catch (CloneNotSupportedException ex) {
                        throw new RuntimeException(ex);
                    }
                    state.markDirty(clone_node);
                    break;
                }
            } // FOR
        }
        
        // Note that we don't want actually move the existing aggregate. We just
        // want to clone it and then attach it down below the SEND/RECIEVE so
        // that we calculate the aggregates in parallel
        if (clone_node == null) {
            clone_node = this.cloneAggregatePlanNode(node);
        }
        assert (clone_node != null);
        
        // But this means we have to also update the RECEIVE to only expect the
        // columns that the AggregateNode will be sending along
        ReceivePlanNode recv_node = null;
        if (clone_node instanceof DistinctPlanNode) {
            recv_node = (ReceivePlanNode) node.getChild(0).getChild(0);
        } else {
            recv_node = (ReceivePlanNode) node.getChild(0);
        }
        recv_node.getOutputColumnGUIDs().clear();
        recv_node.getOutputColumnGUIDs().addAll(clone_node.getOutputColumnGUIDs());
        state.markDirty(recv_node);

        assert (recv_node.getChild(0) instanceof SendPlanNode);
        SendPlanNode send_node = (SendPlanNode) recv_node.getChild(0);
        send_node.getOutputColumnGUIDs().clear();
        send_node.getOutputColumnGUIDs().addAll(clone_node.getOutputColumnGUIDs());
        send_node.addIntermediary(clone_node);
        state.markDirty(send_node);

        // 2011-12-08: We now need to correct the aggregate columns for the
        // original plan node
        if ((clone_node instanceof DistinctPlanNode) == false) {
            // If we have a AGGREGATE_WEIGHTED_AVG in our node, then we know that
            // we can skip the last column because that's the COUNT from the remote partition
            boolean has_weightedAvg = node.getAggregateTypes().contains(ExpressionType.AGGREGATE_WEIGHTED_AVG);
            node.getAggregateColumnGuids().clear();
            int num_cols = clone_node.getOutputColumnGUIDCount() - (has_weightedAvg ? 1 : 0);
            for (int i = 0; i < num_cols; i++) {
                Integer aggOutput = clone_node.getOutputColumnGUID(i);
                PlanColumn planCol = state.plannerContext.get(aggOutput);
                assert (planCol != null);
                AbstractExpression exp = planCol.getExpression();
                assert (exp != null);
                Collection<String> refTables = ExpressionUtil.getReferencedTableNames(exp);
                assert (refTables != null);
                if (refTables.size() == 1 && refTables.contains(PlanAssembler.AGGREGATE_TEMP_TABLE)) {
                    node.getAggregateColumnGuids().add(planCol.guid());
                }
            } // FOR
        }

        if (debug.val) {
            LOG.debug("Successfully applied optimization! Eat that John Hugg!");
            if (trace.val)
                LOG.trace("\n" + PlanNodeUtil.debug(rootNode));
        }

        return Pair.of(true, rootNode);
    }
    
    /**
*
* @param node
* @return
*/
    protected HashAggregatePlanNode cloneAggregatePlanNode(final HashAggregatePlanNode node) {
        HashAggregatePlanNode clone_agg = null;
        try {
            clone_agg = (HashAggregatePlanNode) node.clone(false, true);
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
        state.markDirty(clone_agg);

        // Update the cloned AggregateNode to handle distributed averages
        List<ExpressionType> clone_types = clone_agg.getAggregateTypes();
        
        // For now we'll always put a COUNT at the end of the AggregatePlanNode
        // This makes it easier for us to find it in the EE
        boolean has_count = false;
// boolean has_count = (clone_types.contains(ExpressionType.AGGREGATE_COUNT) ||
// clone_types.contains(ExpressionType.AGGREGATE_COUNT_STAR));

        int orig_cnt = clone_types.size();
        for (int i = 0; i < orig_cnt; i++) {
            ExpressionType cloneType = clone_types.get(i);
            // Ok, strap on your helmets boys, here's what we got going on here...
            // In order to do a distributed average, we need to send the average
            // AND the count (in order to compute the weight average at the base partition).
            // We need check whether we already have a count already in our list
            // If not, then we'll want to insert it here.
            if (cloneType == ExpressionType.AGGREGATE_AVG) {
                if (has_count == false) {
                    // But now because we add a new output column that we're going to use internally,
                    // we need to make sure that our output columns reflect this.
                    clone_types.add(ExpressionType.AGGREGATE_COUNT_STAR);
                    has_count = true;
                    
                    // Aggregate Input Column
                    // We just need to do it against the first column in the child's output
                    // Picking the column that we want to use doesn't matter even if there is a GROUP BY
                    clone_agg.getAggregateColumnGuids().add(node.getChild(0).getOutputColumnGUID(0));

                    // Aggregate Output Column
                    TupleValueExpression exp = new TupleValueExpression();
                    exp.setValueType(VoltType.BIGINT);
                    exp.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
                    exp.setTableName(PlanAssembler.AGGREGATE_TEMP_TABLE);
                    exp.setColumnName("");
                    exp.setColumnAlias("_DTXN_COUNT");
                    exp.setColumnIndex(clone_agg.getOutputColumnGUIDCount());
                    PlanColumn new_pc = state.plannerContext.getPlanColumn(exp, exp.getColumnAlias());
                    clone_agg.getAggregateOutputColumns().add(clone_agg.getOutputColumnGUIDCount());
                    clone_agg.getAggregateColumnNames().add(new_pc.getDisplayName());
                    clone_agg.getOutputColumnGUIDs().add(new_pc.guid());
                }
            }
        } // FOR
        
        // Now go through the original AggregateNode (the one at the top of tree)
        // and change the ExpressiontTypes for the aggregates to handle ahat we're
        // doing down below in the distributed query
        List<ExpressionType> exp_types = node.getAggregateTypes();
        exp_types.clear();
        for (int i = 0; i < orig_cnt; i++) {
            ExpressionType cloneType = clone_types.get(i);
            switch (cloneType) {
                case AGGREGATE_COUNT:
                case AGGREGATE_COUNT_STAR:
                case AGGREGATE_SUM:
                    exp_types.add(ExpressionType.AGGREGATE_SUM);
                    break;
                case AGGREGATE_MAX:
                case AGGREGATE_MIN:
                    exp_types.add(cloneType);
                    break;
                case AGGREGATE_AVG:
                    // This is a special internal marker that allows us to compute
                    // a weighted average from the count
                    exp_types.add(ExpressionType.AGGREGATE_WEIGHTED_AVG);
                    break;
                default:
                    throw new RuntimeException("Unexpected ExpressionType " + cloneType);
            } // SWITCH
        } // FOR
        
        // IMPORTANT: If we have GROUP BY columns, then we need to make sure
        // that those columns are always passed up the query tree at the pushed
        // down node, even if the final answer doesn't need it
        if (node.getGroupByColumnGuids().isEmpty() == false) {
            for (Integer guid : clone_agg.getGroupByColumnGuids()) {
                if (clone_agg.getOutputColumnGUIDs().contains(guid) == false) {
                    clone_agg.getOutputColumnGUIDs().add(guid);
                }
            } // FOR
        }

        assert(clone_agg.getGroupByColumnOffsets().size() == node.getGroupByColumnOffsets().size());
        assert(clone_agg.getGroupByColumnNames().size() == node.getGroupByColumnNames().size());
        assert(clone_agg.getGroupByColumnGuids().size() == node.getGroupByColumnGuids().size()) : clone_agg.getGroupByColumnGuids().size() + " not equal " + node.getGroupByColumnGuids().size();
        
        return (clone_agg);
    }

}