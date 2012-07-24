package edu.brown.optimizer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 */
public class TestPlanOptimizerTPCC extends BasePlanOptimizerTestCase {

    @Override
    protected void setUp() throws Exception {
         super.setUp(ProjectType.TPCC);
    }
    
    /**
     * testProjectionPushdownDistinctOffset
     */
    public void testProjectionPushdownDistinctOffset() throws Exception {
        Procedure catalog_proc = this.getProcedure(slev.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetStockCount");
        
        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        
        // Make sure that the DistinctPlanNode's target column is OL_I_ID
        Collection<DistinctPlanNode> dist_nodes = PlanNodeUtil.getPlanNodes(root, DistinctPlanNode.class);
        assertEquals(1, dist_nodes.size());
        DistinctPlanNode dist_node = CollectionUtil.first(dist_nodes);
        assertNotNull(dist_node);
        
        int col_guid = dist_node.getDistinctColumnGuid();
        PlanColumn pc = PlannerContext.singleton().get(col_guid);
        assertNotNull(pc);
        assertEquals("OL_I_ID", pc.getDisplayName());
    }
    
    /**
     * testProjectionPushdown
     */
    public void testProjectionPushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getCustomer");
        
        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // First check that our single scan node has an inline Projection
        Collection<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
        assertNotNull(scan_node);
        assertEquals(1, scan_node.getInlinePlanNodes().size());
        assertNotNull(scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION));
        
        // Now check to make sure there are no other Projections in the tree
//        Set<ProjectionPlanNode> proj_nodes = PlanNodeUtil.getPlanNodes(root, ProjectionPlanNode.class);
//        assertEquals(0, proj_nodes.size());
    }
    
    
    /**
     * testInlineProjectionColumns
     */
    public void testInlineProjectionColums() throws Exception {
        Procedure catalog_proc = this.getProcedure(slev.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetStockCount");
        
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // Make sure the bottom-most IndexScan has an inline projection with the right columns
        Collection<IndexScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, IndexScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        IndexScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
        assertNotNull(scan_node);
        assertEquals(1, scan_node.getParentPlanNodeCount());
        assertEquals(0, scan_node.getChildPlanNodeCount());
        System.err.println(PlanNodeUtil.debug(root));
        assertEquals(1, scan_node.getInlinePlanNodeCount());
        
        final Map<String, Integer> col_offset_xref = new HashMap<String, Integer>();
        Table catalog_tbl = null;
        
        // STOCK
        if (scan_node.getTargetTableName().equals("STOCK")) {
            catalog_tbl = this.getTable("STOCK");
            for (String colName : new String[]{ "S_I_ID", "S_W_ID", "S_QUANTITY"}) {
                Column catalog_col = this.getColumn(catalog_tbl, colName);
                col_offset_xref.put(colName, catalog_col.getIndex());
            } // FOR
        }
        // ORDER_LINE
        else if (scan_node.getTargetTableName().equals("ORDER_LINE")) {
            catalog_tbl = this.getTable("ORDER_LINE");
            for (String colName : new String[]{ "OL_I_ID" }) {
                Column catalog_col = this.getColumn(catalog_tbl, colName);
                col_offset_xref.put(colName, catalog_col.getIndex());
            } // FOR
        }
        else {
            assert(false) : "Unexpected table '" + scan_node.getTargetTableName() + "'";
        }
        assertNotNull(catalog_tbl);
        assertFalse(col_offset_xref.isEmpty());
        assertEquals(catalog_tbl.getName(), scan_node.getTargetTableName());
        
        // The inline projection in the leaf ScanPlanNode should only output a 
        // single column (S_I_ID), since this is the only column used in the JOIN
        ProjectionPlanNode proj_node = scan_node.getInlinePlanNode(PlanNodeType.PROJECTION);
        assertNotNull(proj_node);
        assertEquals(1, proj_node.getOutputColumnGUIDCount());
        
        System.err.println(PlanNodeUtil.debug(scan_node));
        checkExpressionOffsets(proj_node, col_offset_xref);
        
    }
    
    /**
     * testAggregatePushdown
     */
    public void testAggregatePushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure(slev.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetStockCount");

        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // Check that our single scan node has a COUNT AggregatePlanNode above it.
        Collection<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
        assertNotNull(scan_node);
        assertEquals(1, scan_node.getParentPlanNodeCount());
        // FIXME assertEquals(PlanNodeType.AGGREGATE, scan_node.getParent(0).getPlanNodeType());
        // FIXME AggregatePlanNode count_node = (AggregatePlanNode)scan_node.getParent(0);
        // FIXME assertNotNull(count_node);
        // FIXME assert(count_node.getAggregateTypes().contains(ExpressionType.AGGREGATE_COUNT));
        
        // Now check that we have a SUM AggregatePlanNode right before the root
        // This will sum up the counts from the different partitions and give us the total count
        assertEquals(1, root.getChildPlanNodeCount());
        // FIXME assertEquals(PlanNodeType.AGGREGATE, root.getChild(0).getPlanNodeType());
        // FIXME AggregatePlanNode sum_node= (AggregatePlanNode)root.getChild(0);
        // FIXME assert(sum_node.getAggregateTypes().contains(ExpressionType.AGGREGATE_SUM));
    }
    
}
