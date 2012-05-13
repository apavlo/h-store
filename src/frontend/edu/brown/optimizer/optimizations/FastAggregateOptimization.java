package edu.brown.optimizer.optimizations;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.utils.Pair;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.planner.PlanColumn;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.ExpressionType;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public class FastAggregateOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(FastAggregateOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());

    public FastAggregateOptimization(PlanOptimizerState state) {
        super(state);
        // TODO Auto-generated constructor stub
    }

    /**
     * Perform the optimization on the given PlanNode tree Returns a pair
     * containing the new root of the tree a boolean flag that signals whether
     * the tree was modified or not
     * 
     * @param rootNode
     * @return
     */
    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(AbstractPlanNode rootNode) {
        // TODO Auto-generated method stub
        // Skip single-partition query plans
        if (PlanNodeUtil.isDistributedQuery(rootNode) == false) {
            if (debug.get())
                LOG.debug("SKIP - Not a distributed query plan");
            return (Pair.of(false, rootNode));
        }

        Collection<HashAggregatePlanNode> nodes = PlanNodeUtil.getPlanNodes(rootNode, HashAggregatePlanNode.class);

        // ===================DEBUG====================
        if (debug.get()) LOG.debug("number of aggregate nodes:" + nodes.size());
        Iterator<HashAggregatePlanNode> iterator2 = nodes.iterator();
        while (iterator2.hasNext()) {
            HashAggregatePlanNode node1 = iterator2.next();
            // Check if this is COUNT query
            if (debug.get()){
                LOG.debug("Type of aggregate nodes:" + node1.getAggregateTypes().contains(ExpressionType.AGGREGATE_SUM));
                LOG.debug("Type of aggregate nodes:" + node1.getAggregateTypes().contains(ExpressionType.AGGREGATE_COUNT));
            }
            
        }
        // ===================DEBUG====================

        // Check if this is COUNT query
        Iterator<HashAggregatePlanNode> iterator = nodes.iterator();

        while (iterator.hasNext()) {
            HashAggregatePlanNode node1 = iterator.next();
            // Check if this is COUNT query
            // Check the type of aggregate and prevent the GroupBy operation
            if (node1.getAggregateTypes().contains(ExpressionType.AGGREGATE_SUM) && node1.getGroupByColumnNames().size()==0) {
                if (debug.get())  LOG.debug("have entered!");
                if (node1.getChild(0) instanceof ReceivePlanNode) {
                    ReceivePlanNode receiv_node = (ReceivePlanNode) (node1.getChild(0));
                    assert (receiv_node != null);
                    receiv_node.setFast(true);
                    if (debug.get())  LOG.debug("Set receivenode :" + receiv_node.getFast());
                    return Pair.of(true, rootNode); // need to modify
                }

            }
        }

        return Pair.of(false, rootNode);
    }

}