package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public class RemoveDistributedReplicatedTableJoinOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(RemoveDistributedReplicatedTableJoinOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    
    public RemoveDistributedReplicatedTableJoinOptimization(PlanOptimizerState state) {
        super(state);
    }

    @Override
    public AbstractPlanNode optimize(final AbstractPlanNode rootNode) {
        // Skip single-partition query plans
        if (PlanNodeUtil.getPlanNodeTypes(rootNode).contains(PlanNodeType.RECEIVE) == false) {
            if (debug.get()) LOG.debug("SKIP - Not a distributed query plan");
        }
        
        // Walk through the query plan tree and see if there is a join
        // where the outer table is replicated but it is being
        // scanned in a separate PlanFragment
        for (AbstractJoinPlanNode join_node : PlanNodeUtil.getPlanNodes(rootNode, AbstractJoinPlanNode.class)) {
            if (debug.get()) LOG.debug("Examining " + join_node);
            
            // Check whether the chain below this node is
            // RECIEVE -> SEND -> SCAN
            assert(join_node.getChildPlanNodeCount() >= 1);
            AbstractPlanNode child = join_node.getChild(0);
            if (debug.get()) LOG.debug(join_node + " -> " + child);
            if (child instanceof ReceivePlanNode) {
                ReceivePlanNode recv_node = (ReceivePlanNode)child;
                SendPlanNode send_node = (SendPlanNode)child.getChild(0);
                List<AbstractPlanNode> children = send_node.getChildren();
                assert(children != null);
                if (children.size() > 1) continue;
                if (children.get(0).getChildPlanNodeCount() > 0) continue;
                if ((children.get(0) instanceof AbstractScanPlanNode) == false) continue;
                
                AbstractPlanNode leaf_node = children.get(0);
                assert(leaf_node instanceof AbstractScanPlanNode);
                Collection<Table> leaf_tables = CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, leaf_node);
                assert(leaf_tables.size() == 1);
                Table leaf_tbl = CollectionUtil.first(leaf_tables);
                
                Collection<Table> join_tables = CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, join_node);
                Table join_tbl = null;
                for (Table catalog_tbl : join_tables) {
                    if (catalog_tbl.equals(leaf_tbl) == false) {
                        join_tbl = catalog_tbl;
                        break;
                    }
                } // FOR
                assert(join_tbl != null);
                
                // If either table is replicated, then the leaf scan should be linked
                // directly with the Join PlanNode
                if (join_tbl.getIsreplicated() || leaf_tbl.getIsreplicated()) {
                    AbstractPlanNode parent = join_node.getParent(0);
                    assert(parent != null);
                    parent.clearChildren();
                    parent.addAndLinkChild(recv_node);
                    
                    join_node.clearParents();
                    join_node.clearChildren();
                    leaf_node.clearParents();
                    
                    join_node.addAndLinkChild(leaf_node);
                    send_node.addAndLinkChild(join_node);
                }
            }
        } // FOR
        
        return (rootNode);
    }

}
