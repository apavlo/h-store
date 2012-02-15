package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.expressions.AbstractExpression;
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
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class AggregatePushdownOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(AggregatePushdownOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    
    public AggregatePushdownOptimization(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(AbstractPlanNode rootNode) {
        Collection<HashAggregatePlanNode> nodes = PlanNodeUtil.getPlanNodes(rootNode, HashAggregatePlanNode.class);
        if (nodes.size() != 1) return Pair.of(false, rootNode);
        final HashAggregatePlanNode aggNode = CollectionUtil.first(nodes);
        
        // String orig_root_debug2 = PlanNodeUtil.debug(root);
        if (debug.get()) LOG.debug("Trying to apply Aggregate pushdown optimization!");
        
        // Skip single-partition query plans
        if (PlanNodeUtil.isDistributedQuery(rootNode) == false) {
            if (debug.get()) LOG.debug("SKIP - Not a distributed query plan");
            return (Pair.of(false, rootNode));
        }
        // TODO: Can only do single aggregates
        if (aggNode.getAggregateTypes().size() != 1) {
            if (debug.get()) LOG.debug("SKIP - Multiple aggregates");
            return (Pair.of(false, rootNode));
        }
        // Can't do averages
        if (aggNode.getAggregateTypes().get(0) == ExpressionType.AGGREGATE_AVG) {
            if (debug.get()) LOG.debug("SKIP - Can't optimize AVG()");
            return (Pair.of(false, rootNode));
        }
        // Get the AbstractScanPlanNode that is directly below us
        Collection<AbstractScanPlanNode> scans = PlanNodeUtil.getPlanNodes(aggNode, AbstractScanPlanNode.class);
        if (scans.size() != 1) {
            if (debug.get()) LOG.debug("SKIP - Multiple scans!");
            return (Pair.of(false, rootNode));
        }
        
        // Check if this is count(distinct) query
        if (aggNode.getAggregateTypes().get(0) == ExpressionType.AGGREGATE_COUNT) {
            for (AbstractPlanNode child : aggNode.getChildren()) {
                if (child.getClass().equals(DistinctPlanNode.class)) {
                    final DistinctPlanNode distinct_plan_node = PlanNodeUtil.getPlanNodes(aggNode, DistinctPlanNode.class).iterator().next();
                    // write crap here to handle copying distinct above seqscan
                    new PlanNodeTreeWalker() {
                        @Override
                        protected void callback(AbstractPlanNode element) {
                            if (element instanceof SendPlanNode) {
                                AbstractPlanNode child = element.getChild(0);
                                element.clearChildren();
                                child.clearParents();
                                DistinctPlanNode distinct_clone = distinct_plan_node.produceCopyForTransformation();
                                distinct_clone.getOutputColumnGUIDs().clear();
                                distinct_clone.getOutputColumnGUIDs().addAll(child.getOutputColumnGUIDs());
                                distinct_clone.addAndLinkChild(child);
                                element.addAndLinkChild(distinct_clone);
                            }
                        }
                    }.traverse(aggNode);
                    //System.out.println(PlanNodeUtil.debug(aggNode));
                    break;
                }
            }                
        }
        
        AbstractScanPlanNode scan_node = CollectionUtil.first(scans);
        assert(scan_node != null);
        // For some reason we have to do this??
        for (int col = 0, cnt = scan_node.getOutputColumnGUIDs().size(); col < cnt; col++) {
            int col_guid = scan_node.getOutputColumnGUIDs().get(col);
            assert(state.plannerContext.get(col_guid) != null) : "Failed [" + col_guid + "]"; 
            // PlanColumn retval = new PlanColumn(guid, expression, columnName, sortOrder, storage);
        } // FOR

        // Skip if we're already directly after the scan (meaning no network traffic)
        if (scan_node.getParent(0).equals(aggNode)) {
            if (debug.get()) LOG.debug("SKIP - Aggregate does not need to be distributed");
            return (Pair.of(false, rootNode));
        }
        
        // Note that we don't want actually move the existing aggregate. We just want to clone it and then
        // attach it down below the SEND/RECIEVE so that we calculate the aggregate in parallel
        HashAggregatePlanNode clone_node = null; // new HashAggregatePlanNode(state.plannerContext, getNextPlanNodeId());
        try {
            clone_node = (HashAggregatePlanNode)aggNode.clone(false, true);
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
        state.markDirty(aggNode);

        // Set original AggregateNode to contain sum
        if (clone_node.getAggregateTypes().size() > 0) {
            List<ExpressionType> exp_types = aggNode.getAggregateTypes();
            exp_types.clear();
            
            ExpressionType origType = clone_node.getAggregateTypes().get(0);
            switch (origType) {
                case AGGREGATE_COUNT:
                case AGGREGATE_COUNT_STAR:
                case AGGREGATE_SUM:
                    exp_types.add(ExpressionType.AGGREGATE_SUM);
                    break;
                case AGGREGATE_MAX:
                case AGGREGATE_MIN:
                    exp_types.add(origType);
                    break;
                default:
                    throw new RuntimeException("Unexpected ExpressionType " + origType);
            } // SWITCH
        }
        
        assert(clone_node.getGroupByColumnOffsets().size() == aggNode.getGroupByColumnOffsets().size());
        assert(clone_node.getGroupByColumnNames().size() == aggNode.getGroupByColumnNames().size());
        assert(clone_node.getGroupByColumnGuids().size() == aggNode.getGroupByColumnGuids().size()) : clone_node.getGroupByColumnGuids().size() + " not equal " + aggNode.getGroupByColumnGuids().size();
        assert(clone_node.getAggregateTypes().size() == aggNode.getAggregateTypes().size());
        assert(clone_node.getAggregateColumnGuids().size() == aggNode.getAggregateColumnGuids().size());
        assert(clone_node.getAggregateColumnNames().size() == aggNode.getAggregateColumnNames().size());
        assert(clone_node.getAggregateOutputColumns().size() == aggNode.getAggregateOutputColumns().size());
        assert(clone_node.getOutputColumnGUIDs().size() == aggNode.getOutputColumnGUIDs().size());

        // But this means we have to also update the RECEIVE to only expect the columns that
        // the AggregateNode will be sending along
        assert(aggNode.getChild(0) instanceof ReceivePlanNode);
        ReceivePlanNode recv_node = (ReceivePlanNode)aggNode.getChild(0);
        recv_node.getOutputColumnGUIDs().clear();
        recv_node.getOutputColumnGUIDs().addAll(clone_node.getOutputColumnGUIDs());
        state.markDirty(recv_node);
        
        assert(recv_node.getChild(0) instanceof SendPlanNode);
        SendPlanNode send_node = (SendPlanNode)recv_node.getChild(0);
        send_node.getOutputColumnGUIDs().clear();
        send_node.getOutputColumnGUIDs().addAll(clone_node.getOutputColumnGUIDs());
        send_node.addIntermediary(clone_node);
        state.markDirty(send_node);
        
        // 2011-12-08: We now need to correct the aggregate columns for the original plan node
        aggNode.getAggregateColumnGuids().clear();
        for (Integer aggOutput : clone_node.getOutputColumnGUIDs()) {
            PlanColumn planCol = state.plannerContext.get(aggOutput);
            assert(planCol != null);
            AbstractExpression exp = planCol.getExpression();
            assert(exp != null);
            Collection<String> refTables = ExpressionUtil.getReferencedTableNames(exp);
            assert(refTables != null);
            if (refTables.size() == 1 && refTables.contains(PlanAssembler.AGGREGATE_TEMP_TABLE)) {
                aggNode.getAggregateColumnGuids().add(planCol.guid());
            }
        } // FOR
        
        if (debug.get()) {
            LOG.debug("Successfully applied optimization! Eat that John Hugg!");
            LOG.debug(PlanNodeUtil.debug(rootNode));
        }    
        
        return Pair.of(true, rootNode);
    }
    
    

}
