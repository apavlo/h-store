package org.voltdb.planner;

import java.util.Set;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

import edu.brown.BaseTestCase;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 */
public class TestPlanOptimizations extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
    }
    
    /**
     * testProjectionPushdown
     */
    public void testProjectionPushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getCustomer");
        
        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // First check that our single scan node has an inline Projection
        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        // FIXME assertEquals(1, scan_node.getInlinePlanNodes().size());
        // FIXME assertNotNull(scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION));
        
        // Now check to make sure there are no other Projections in the tree
        Set<ProjectionPlanNode> proj_nodes = PlanNodeUtil.getPlanNodes(root, ProjectionPlanNode.class);
        // FIXME assertEquals(0, proj_nodes.size());
    }
    
    /**
     * testAggregatePushdown
     */
    public void testAggregatePushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure(slev.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetStockCount");

        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // Check that our single scan node has a COUNT AggregatePlanNode above it.
        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        assertEquals(1, scan_node.getParentCount());
        // FIXME assertEquals(PlanNodeType.AGGREGATE, scan_node.getParent(0).getPlanNodeType());
        // FIXME AggregatePlanNode count_node = (AggregatePlanNode)scan_node.getParent(0);
        // FIXME assertNotNull(count_node);
        // FIXME assert(count_node.getAggregateTypes().contains(ExpressionType.AGGREGATE_COUNT));
        
        // Now check that we have a SUM AggregatePlanNode right before the root
        // This will sum up the counts from the different partitions and give us the total count
        assertEquals(1, root.getChildCount());
        // FIXME assertEquals(PlanNodeType.AGGREGATE, root.getChild(0).getPlanNodeType());
        // FIXME AggregatePlanNode sum_node= (AggregatePlanNode)root.getChild(0);
        // FIXME assert(sum_node.getAggregateTypes().contains(ExpressionType.AGGREGATE_SUM));
    }
    
}
