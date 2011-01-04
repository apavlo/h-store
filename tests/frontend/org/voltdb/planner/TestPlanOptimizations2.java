package org.voltdb.planner;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
	
/**
 * 
 * @author pavlo
 */
public class TestPlanOptimizations2 extends BaseTestCase {

    private VoltProjectBuilder pb = new VoltProjectBuilder("test-planopt") {
        {
            File schema = new File(TestPlanOptimizations2.class.getResource("testopt-ddl.sql").getFile());
            assert(schema.exists()) : "Schema: " + schema;
            this.addSchema(schema.getAbsolutePath());
            
            this.addPartitionInfo("TABLEA", "A_ID");
            this.addPartitionInfo("TABLEB", "B_A_ID");
            
            this.addStmtProcedure("SingleProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
            this.addStmtProcedure("JoinProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA, TABLEB WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
        }
    };
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }
    
    /**
     * testSingleProjectionPushdown
     */
    @Test
    public void testSingleProjectionPushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        
        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // First check that our single scan node has an inline Projection
        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        // FIXME assertEquals(1, scan_node.getInlinePlanNodes().size());
        
        // Get the Projection and make sure it has valid output columns
        ProjectionPlanNode inline_proj = (ProjectionPlanNode)scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
        /* FIXME
        assertNotNull(inline_proj);
        for (int column_guid : inline_proj.m_outputColumns) {
            PlanColumn column = PlannerContext.singleton().get(column_guid);
//            System.err.println(String.format("[%02d] %s", column_guid, column));
//            System.err.println("==================");
//            System.err.println(PlannerContext.singleton().debug());
            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
            assertEquals(column_guid, column.guid());
        } // FOR
        */
        
        // Now check to make sure there are no other Projections in the tree
        Set<ProjectionPlanNode> proj_nodes = PlanNodeUtil.getPlanNodes(root, ProjectionPlanNode.class);
        // FIXME assertEquals(0, proj_nodes.size());
    }
    
    /**
     * testJoinProjectionPushdown
     */
    @Test
    public void testJoinProjectionPushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure("JoinProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        
        // Grab the root node of the multi-partition query plan tree for this Statement 
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
        assertNotNull(root);
        // System.err.println(PlanNodeUtil.debug(root));
        
        // print the tree starting at the root
//        new PlanNodeTreeWalker() {
//            @Override
//            protected void callback(AbstractPlanNode element) {
//            	System.out.println(element.getPlanNodeType().name() + " " + element.getPlanNodeId());
//            	if (element.getInlinePlanNodes().size() > 0)
//            	{
//            	    Iterator it = element.getInlinePlanNodes().entrySet().iterator();
//            	    while (it.hasNext()) {
//            	        Map.Entry pairs = (Map.Entry)it.next();
//            	        System.out.println(pairs.getKey() + " : # of output columns " + ((AbstractPlanNode)pairs.getValue()).m_outputColumns.size());
//            	        for (Integer col :((AbstractPlanNode)pairs.getValue()).m_outputColumns)
//            	        {
//            	        	System.out.print("Col: " + col + " ");
//            	        }
//            	    }
//            	}
//            	System.out.println();
//            	System.out.println();
//            }
//        }.traverse(root);

        System.out.println(PlanNodeUtil.debug(root));

        // At the very bottom of our tree should be a scan. Grab that and then check to see that 
        // it has an inline ProjectionPlanNode. We will then look to see whether all of the columns
        // we need to join are included. Note that we don't care which table is scanned first, as we can
        // dynamically figure things out for ourselves
        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        Table catalog_tbl = this.getTable(scan_node.getTargetTableName()); 
        
        assertEquals(1, scan_node.getInlinePlanNodes().size());
        ProjectionPlanNode inline_proj = (ProjectionPlanNode)scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION); 
        assertNotNull(inline_proj);
        
        // Validate output columns
        for (int column_guid : inline_proj.m_outputColumns) {
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            assertNotNull("Missing PlanColumn [guid=" + column_guid + "]", column);
            assertEquals(column_guid, column.guid());
        } // FOR
        
        System.out.println("Inline Projection Node: " + inline_proj.getPlanNodeId() + " output columns: " + inline_proj.m_outputColumns.size());
        Set<Column> proj_columns = null; 
        proj_columns = PlanNodeUtil.getOutputColumns(catalog_db, inline_proj);
        
        assertFalse(proj_columns.isEmpty());
        
        // Now find the join and get all of the columns from the first scanned table in the join operation
        Set<AbstractJoinPlanNode> join_nodes = PlanNodeUtil.getPlanNodes(root, AbstractJoinPlanNode.class);
        assertNotNull(join_nodes);
        assertEquals(1, join_nodes.size());
        AbstractJoinPlanNode join_node = CollectionUtil.getFirst(join_nodes);
        assertNotNull(join_node);
        
        // Remove the columns from the second table
        Set<Column> join_columns = CatalogUtil.getReferencedColumns(catalog_db, join_node);
        assertNotNull(join_columns);
        assertFalse(join_columns.isEmpty());
//        System.err.println(CatalogUtil.debug(join_columns));
        Iterator<Column> it = join_columns.iterator();
        while (it.hasNext()) {
            Column catalog_col = it.next();
            if (catalog_col.getParent().equals(catalog_tbl) == false) {
                it.remove();
            }
        } // WHILE
        assertFalse(join_columns.isEmpty());
        System.err.println("COLUMNS: " + CatalogUtil.debug(join_columns));
        
        System.err.println("Project COLUMNS: " + CatalogUtil.debug(proj_columns));
        // Ok so now we have the list of columns that are filtered out in the inline projection and the list of
        // columns that are used in the join from the first table. So we need to make sure that
        // every table that is in the join is in the projection
        for (Column catalog_col : join_columns) {
        	// dwu assert doesn't work, use assertTrue
            assertTrue(proj_columns.contains(catalog_col));
        } // FOR
        
        // Lastly, we need to look at the root SEND node and get its output columns, and make sure that they 
        // are also included in the bottom projection
        Set<Column> send_columns = PlanNodeUtil.getOutputColumns(catalog_db, root);
    	System.out.println("Send columns count: " + send_columns.size());
        assertFalse(send_columns.isEmpty());
        for (Column catalog_col : send_columns) {
            assert(proj_columns.contains(catalog_col)) : "Missing: " + CatalogUtil.getDisplayName(catalog_col);
        } // FOR
    }    
//    /**
//     * testAggregatePushdown
//     */
//    public void testAggregatePushdown() throws Exception {
//        Procedure catalog_proc = this.getProcedure(slev.class);
//        Statement catalog_stmt = this.getStatement(catalog_proc, "GetStockCount");
//
//        // Grab the root node of the multi-partition query plan tree for this Statement 
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        assertNotNull(root);
//        
//        // Check that our single scan node has a COUNT AggregatePlanNode above it.
//        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
//        assertEquals(1, scan_nodes.size());
//        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
//        assertNotNull(scan_node);
//        assertEquals(1, scan_node.getParentCount());
//        // FIXME assertEquals(PlanNodeType.AGGREGATE, scan_node.getParent(0).getPlanNodeType());
//        // FIXME AggregatePlanNode count_node = (AggregatePlanNode)scan_node.getParent(0);
//        // FIXME assertNotNull(count_node);
//        // FIXME assert(count_node.getAggregateTypes().contains(ExpressionType.AGGREGATE_COUNT));
//        
//        // Now check that we have a SUM AggregatePlanNode right before the root
//        // This will sum up the counts from the different partitions and give us the total count
//        assertEquals(1, root.getChildCount());
//        // FIXME assertEquals(PlanNodeType.AGGREGATE, root.getChild(0).getPlanNodeType());
//        // FIXME AggregatePlanNode sum_node= (AggregatePlanNode)root.getChild(0);
//        // FIXME assert(sum_node.getAggregateTypes().contains(ExpressionType.AGGREGATE_SUM));
//    }
    
}
