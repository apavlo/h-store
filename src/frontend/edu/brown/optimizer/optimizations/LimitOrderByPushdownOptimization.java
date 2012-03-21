package edu.brown.optimizer.optimizations;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.utils.Pair;

import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * Pushdown a LIMIT and ORDER BY to be before we send data over the network
 * 
 * @author pavlo
 */
public class LimitOrderByPushdownOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(LimitOrderByPushdownOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());

    public LimitOrderByPushdownOptimization(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(final AbstractPlanNode root) {
        // Check whether this PlanTree contains an OrderByPlanNode and a LimitPlanNode
        // If it does and there are no joins, then we should be able to push duplicates
        // down into the ScanPlanNode so that we can prune out as much as we can before
        // we send it over the wire. This is a basic a Merge-Sort operation
        Collection<OrderByPlanNode> orderby_nodes = PlanNodeUtil.getPlanNodes(root, OrderByPlanNode.class);
        Collection<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
        Collection<AbstractJoinPlanNode> join_nodes = PlanNodeUtil.getPlanNodes(root, AbstractJoinPlanNode.class);
        Collection<ReceivePlanNode> recv_nodes = PlanNodeUtil.getPlanNodes(root, ReceivePlanNode.class);
        if (orderby_nodes.size() != 1) {
            if (debug.get())
                LOG.debug("SKIP - Number of OrderByPlanNodes is " + orderby_nodes.size());
            return (Pair.of(false, root));
        } else if (limit_nodes.size() != 1) {
            if (debug.get())
                LOG.debug("SKIP - Number of LimitPlanNodes is " + limit_nodes.size());
            return (Pair.of(false, root));
        } else if (join_nodes.isEmpty() == false) {
            if (debug.get())
                LOG.debug("SKIP - Contains a JoinPlanNode " + join_nodes);
            return (Pair.of(false, root));
        } else if (recv_nodes.isEmpty()) {
            if (debug.get())
                LOG.debug("SKIP - Does not contain any ReceivePlanNodes");
            return (Pair.of(false, root));
        }

        OrderByPlanNode orderby_node = null;
        LimitPlanNode limit_node = null;
        try {
            orderby_node = (OrderByPlanNode) CollectionUtil.first(orderby_nodes).clone(false, true);
            limit_node = (LimitPlanNode) CollectionUtil.first(limit_nodes).clone(false, true);
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
        assert (orderby_node != null);
        assert (limit_node != null);

        if (debug.get()) {
            LOG.debug("ORDER BY: " + PlanNodeUtil.debug(orderby_node));
            LOG.debug("LIMIT:    " + PlanNodeUtil.debug(limit_node));
            LOG.debug(PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class));
        }
        AbstractScanPlanNode scan_node = CollectionUtil.first(PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class));
        assert (scan_node != null) : "Unexpected PlanTree:\n" + PlanNodeUtil.debug(root);
        SendPlanNode send_node = (SendPlanNode) scan_node.getParent(0);
        assert (send_node != null);

        send_node.addIntermediary(limit_node);
        limit_node.addIntermediary(orderby_node);
        orderby_node.clearChildren();
        scan_node.clearParents();
        orderby_node.addAndLinkChild(scan_node);

        // Need to make sure that the LIMIT has the proper output columns
        limit_node.setOutputColumns(orderby_node.getOutputColumnGUIDs());
        state.markDirty(orderby_node);
        state.markDirty(limit_node);

        if (debug.get())
            LOG.debug("PLANOPT - Added " + limit_node + "+" + orderby_node + " after " + scan_node);

        return (Pair.of(true, root));
    }

}