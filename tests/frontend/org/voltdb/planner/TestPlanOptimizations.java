package org.voltdb.planner;

import java.util.Set;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.QueryPlanUtil;
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
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
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
        Statement catalog_stmt = this.getStatement(catalog_proc, "getStockCount");

        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // TODO(pavlo)
    }
    
}
