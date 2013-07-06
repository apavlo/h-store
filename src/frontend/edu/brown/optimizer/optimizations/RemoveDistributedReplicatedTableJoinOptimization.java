package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public class RemoveDistributedReplicatedTableJoinOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(RemoveDistributedReplicatedTableJoinOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public RemoveDistributedReplicatedTableJoinOptimization(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public Pair<Boolean, AbstractPlanNode> optimize(final AbstractPlanNode rootNode) {
        // Skip single-partition query plans
        if (PlanNodeUtil.isDistributedQuery(rootNode) == false) {
            if (debug.val)
                LOG.debug("SKIP - Not a distributed query plan");
            return (Pair.of(false, rootNode));
        }

        // Walk through the query plan tree and see if there is a join
        // where the outer table is replicated but it is being
        // scanned in a separate PlanFragment
        boolean modified = false;
        for (AbstractJoinPlanNode join_node : PlanNodeUtil.getPlanNodes(rootNode, AbstractJoinPlanNode.class)) {
            if (debug.val)
                LOG.debug("Examining " + join_node);

            // Check whether the chain below this node is
            // RECIEVE -> SEND -> SCAN
            assert (join_node.getChildPlanNodeCount() >= 1);
            AbstractPlanNode child = join_node.getChild(0);
            if (debug.val)
                LOG.debug(join_node + " -> " + child);
            if (child instanceof ReceivePlanNode) {
                ReceivePlanNode recv_node = (ReceivePlanNode) child;
                SendPlanNode send_node = (SendPlanNode) child.getChild(0);
                List<AbstractPlanNode> children = send_node.getChildren();
                assert (children != null);
                if (children.size() > 1)
                    continue;
                if (children.get(0).getChildPlanNodeCount() > 0)
                    continue;
                if ((children.get(0) instanceof AbstractScanPlanNode) == false)
                    continue;

                AbstractPlanNode leaf_node = children.get(0);
                assert (leaf_node instanceof AbstractScanPlanNode);
                Collection<Table> leaf_tables = CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, leaf_node);
                assert (leaf_tables.size() == 1);
                Table leaf_tbl = CollectionUtil.first(leaf_tables);

                Collection<Table> join_tables = CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, join_node);
                Table join_tbl = null;
                for (Table catalog_tbl : join_tables) {
                    if (catalog_tbl.equals(leaf_tbl) == false) {
                        join_tbl = catalog_tbl;
                        break;
                    }
                } // FOR
                assert (join_tbl != null);

                // If either table is replicated, then the leaf scan should be linked
                // directly with the Join PlanNode
                if (join_tbl.getIsreplicated() || leaf_tbl.getIsreplicated()) {
                    AbstractPlanNode parent = join_node.getParent(0);
                    assert (parent != null);
                    parent.clearChildren();
                    parent.addAndLinkChild(recv_node);
                    state.markDirty(parent);
                    state.markDirty(recv_node);

                    join_node.clearParents();
                    join_node.clearChildren();
                    leaf_node.clearParents();

                    join_node.addAndLinkChild(leaf_node);
                    state.markDirty(join_node);
                    state.markDirty(leaf_node);

                    // HACK: If the parent is a ProjectionPlanNode, then we'll want 
                    // to duplicate it so that we make sure that our original 
                    // SEND/RECIEVE nodes have the right offsets
                    if (parent instanceof ProjectionPlanNode) {
                        AbstractPlanNode parent_clone = null;
                        try {
                            parent_clone = (AbstractPlanNode) parent.clone(false, false);
                        } catch (CloneNotSupportedException ex) {
                            throw new RuntimeException(ex);
                        }
                        assert (parent_clone != null);
                        assert (parent != parent_clone);
                        parent_clone.addAndLinkChild(join_node);
                        send_node.clearChildren();
                        send_node.addAndLinkChild(parent_clone);
                    } else {
                        send_node.clearChildren();
                        send_node.addAndLinkChild(join_node);
                    }
                    state.markDirty(send_node);
                    modified = true;
                }
            }
        } // FOR

        return (Pair.of(modified, rootNode));
    }

}
