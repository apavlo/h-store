package org.voltdb.planner;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 */
public class TestPlanOptimizer3 extends BasePlanOptimizerTestCase {

    AbstractProjectBuilder pb = new PlanOptimizerTestProjectBuilder("planopt3") {
        {
            this.addStmtProcedure("DistinctAggregate",
                                  "SELECT COUNT(DISTINCT(TABLEB.B_ID)) AS DISTINCTNUMBER " +
                                  "FROM TABLEA, TABLEB " +
                                  "WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = ? AND TABLEB.B_ID < ?");
            this.addStmtProcedure("DistinctCount", "SELECT COUNT(DISTINCT(TABLEB.B_A_ID)) FROM TABLEB");
            this.addStmtProcedure("MaxGroup", "SELECT B_ID, Max(TABLEB.B_A_ID) FROM TABLEB GROUP BY B_ID");
            this.addStmtProcedure("Max", "SELECT Max(TABLEB.B_A_ID) FROM TABLEB");
            this.addStmtProcedure("Min", "SELECT Min(TABLEB.B_A_ID) FROM TABLEB");
            this.addStmtProcedure("Aggregate", "SELECT COUNT(TABLEB.B_A_ID) AS cnt FROM TABLEB");
            this.addStmtProcedure("Limit", "SELECT * FROM TABLEA WHERE TABLEA.A_ID > ? AND TABLEA.A_ID <= ? AND TABLEA.A_VALUE0 != ? LIMIT 15");
            this.addStmtProcedure("LimitJoin", "SELECT TABLEA.A_ID,TABLEB.B_ID FROM TABLEA, TABLEB WHERE TABLEA.A_ID > ? AND TABLEA.A_ID = TABLEB.B_A_ID LIMIT 15");
            this.addStmtProcedure("ThreeWayJoin",
                                  "SELECT TABLEA.A_VALUE0, TABLEB.B_VALUE0, ((TABLEC.C_VALUE0 + TABLEC.C_VALUE1) / TABLEB.B_A_ID) AS blah " +
                                  "FROM TABLEA, TABLEB, TABLEC " +
                                  "WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = TABLEC.C_B_A_ID AND TABLEA.A_VALUE3 = ? " +
                                  "  AND TABLEC.C_B_A_ID = ? AND TABLEC.C_VALUE0 != ? AND TABLEC.C_VALUE1 != ?");
            this.addStmtProcedure("SingleProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
            this.addStmtProcedure("JoinProjection",
                                  "SELECT TABLEA.A_ID, TABLEA.A_VALUE0, TABLEA.A_VALUE1, TABLEA.A_VALUE2, TABLEA.A_VALUE3, TABLEA.A_VALUE4 " +
                                  "FROM TABLEA,TABLEB " +
                                  "WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
            this.addStmtProcedure("AggregateColumnAddition", "SELECT AVG(TABLEC.C_VALUE0), C_B_A_ID FROM TABLEC WHERE TABLEC.C_ID = ? GROUP BY C_B_A_ID");
            this.addStmtProcedure("OrderBy", "SELECT TABLEC.C_B_A_ID FROM TABLEC ORDER BY TABLEC.C_B_A_ID, TABLEC.C_VALUE0");
            this.addStmtProcedure("GroupBy", "SELECT MAX(TABLEC.C_ID) FROM TABLEC GROUP BY TABLEC.C_B_A_ID, TABLEC.C_VALUE0");
        }
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }

    /**
     * testDistinct
     */
    @Test
    public void testDistinct() throws Exception {
        Procedure catalog_proc = this.getProcedure("DistinctAggregate");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        assertNotNull(root);
        //validateNodeColumnOffsets(root);
        //System.err.println(PlanNodeUtil.debug(root));
    }    

    /**
     * testCountDistinct
     */
    @Test
    public void testCountDistinct() throws Exception {
        Procedure catalog_proc = this.getProcedure("DistinctCount");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
//        validateNodeColumnOffsets(root);
        //System.err.println(PlanNodeUtil.debug(root));
    }
    
    /**
     * testMaxGroup
     */
    @Test
    public void testMaxGroup() throws Exception {
        Procedure catalog_proc = this.getProcedure("MaxGroup");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
//        validateNodeColumnOffsets(root);
//        System.err.println(PlanNodeUtil.debug(root));
    }

    /**
     * testMax
     */
    @Test
    public void testMax() throws Exception {
        Procedure catalog_proc = this.getProcedure("Max");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
//        validateNodeColumnOffsets(root);
//        System.err.println(PlanNodeUtil.debug(root));
    }
    
    /**
     * testMin
     */
    @Test
    public void testMin() throws Exception {
        Procedure catalog_proc = this.getProcedure("Min");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
//        validateNodeColumnOffsets(root);
//        System.err.println(PlanNodeUtil.debug(root));
    }

    /**
     * testAggregate
     */
    @Test
    public void testAggregate() throws Exception {
        Procedure catalog_proc = this.getProcedure("Aggregate");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
//        validateNodeColumnOffsets(root);
//        System.err.println(PlanNodeUtil.debug(root));
    }

    /**
     * testLimit
     */
    @Test
    public void testLimit() throws Exception {
        Procedure catalog_proc = this.getProcedure("Limit");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        //validateNodeColumnOffsets(root);
        assertNotNull(root);

        // First check that our single scan node has an limit node
        Collection<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
        assertEquals(1, limit_nodes.size());

        // Get the Limit nodes output columns and make sure their valid
        LimitPlanNode limit_node = CollectionUtil.getFirst(limit_nodes);
        assertNotNull(limit_node);
        for (int column_guid : limit_node.getOutputColumnGUIDs()) {
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            // System.err.println(String.format("[%02d] %s", column_guid,
            // column));
            // System.err.println("==================");
            // System.err.println(PlannerContext.singleton().debug());
            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
            assertEquals(column_guid, column.guid());
        } // FOR

        // System.err.println(PlanNodeUtil.debug(root));
    }

    /**
     * testLimitJoin
     */
    @Test
    public void testLimitJoin() throws Exception {
        Procedure catalog_proc = this.getProcedure("LimitJoin");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        //validateNodeColumnOffsets(root);
        assertNotNull(root);

        // First check that our single scan node has an limit node
        Collection<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
        assertEquals(1, limit_nodes.size());

        // Get the Limit nodes output columns and make sure their valid
        LimitPlanNode limit_node = CollectionUtil.getFirst(limit_nodes);
        assertNotNull(limit_node);
        for (int column_guid : limit_node.getOutputColumnGUIDs()) {
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            // System.err.println(String.format("[%02d] %s", column_guid,
            // column));
            // System.err.println("==================");
            // System.err.println(PlannerContext.singleton().debug());
            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
            assertEquals(column_guid, column.guid());
        } // FOR

        // System.err.println(PlanNodeUtil.debug(root));
    }

    /**
     * testThreeWayJoin
     */
    @Test
    public void testThreeWayJoin() throws Exception {
        Procedure catalog_proc = this.getProcedure("ThreeWayJoin");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);

        // System.err.println(PlanNodeUtil.debug(root));
        //validateNodeColumnOffsets(root);
        assertNotNull(root);
    }

    /**
     * testSingleProjectionPushdown
     */
    @Test
    public void testSingleProjectionPushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        assertNotNull(root);
        // //validateNodeColumnOffsets(root);
        // System.err.println(PlanNodeUtil.debug(root));
        // First check that our single scan node has an inline Projection
        Collection<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        assertEquals(1, scan_node.getInlinePlanNodes().size());

        // Get the Projection and make sure it has valid output columns
        ProjectionPlanNode inline_proj = (ProjectionPlanNode) scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
        assertNotNull(inline_proj);
        for (int column_guid : inline_proj.getOutputColumnGUIDs()) {
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            // System.err.println(String.format("[%02d] %s", column_guid,
            // column));
            // System.err.println("==================");
            // System.err.println(PlannerContext.singleton().debug());
            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
            assertEquals(column_guid, column.guid());
        } // FOR

        // // Now check to make sure there are no other Projections in the tree
        // Set<ProjectionPlanNode> proj_nodes =
        PlanNodeUtil.getPlanNodes(root, ProjectionPlanNode.class);
        // assertEquals(0, proj_nodes.size());
    }

    /**
     * testJoinProjectionPushdown
     */
    @Test
    public void testJoinProjectionPushdown() throws Exception {
        Procedure catalog_proc = this.getProcedure("JoinProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        assertNotNull(root);
        //validateNodeColumnOffsets(root);

        new PlanNodeTreeWalker() {

            @Override
            protected void callback(AbstractPlanNode element) {
                // System.out.println("element plannodetype: " +
                // element.getPlanNodeType() + " depth: " + this.getDepth());
            }
        }.traverse(root);
        //
        // System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
        //
        // System.err.println(PlanNodeUtil.debug(root));
        // //
        //
        // System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
        // //
        // // System.err.println("# of Fragments: " +
        // catalog_stmt.getMs_fragments().size());
        // // for (PlanFragment pf : catalog_stmt.getMs_fragments()) {
        // // System.err.println(pf.getName() + "\n" +
        // PlanNodeUtil.debug(QueryPlanUtil.deserializePlanFragment(pf)));
        // }

        // At the very bottom of our tree should be a scan. Grab that and then
        // check to see that it has an inline ProjectionPlanNode. We will then
        // look to see whether
        // all of the columns
        // we need to join are included. Note that we don't care which table is
        // scanned first, as we can
        // dynamically figure things out for ourselves
        Collection<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        Table catalog_tbl = this.getTable(scan_node.getTargetTableName());

        assertEquals(1, scan_node.getInlinePlanNodes().size());
        ProjectionPlanNode inline_proj = (ProjectionPlanNode) scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
        assertNotNull(inline_proj);

        // Validate output columns
        for (int column_guid : inline_proj.getOutputColumnGUIDs()) {
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            assertNotNull("Missing PlanColumn [guid=" + column_guid + "]", column);
            assertEquals(column_guid, column.guid());

            // Check that only columns from the scanned table are there
            String table_name = column.originTableName();
            assertNotNull(table_name);
            String column_name = column.originColumnName();
            assertNotNull(column_name);
            assertEquals(table_name + "." + column_name, catalog_tbl.getName(), table_name);
            assertNotNull(table_name + "." + column_name, catalog_tbl.getColumns().get(column_name));
        } // FOR

        Collection<Column> proj_columns = null;
        proj_columns = PlanNodeUtil.getOutputColumnsForPlanNode(catalog_db, inline_proj);
        assertFalse(proj_columns.isEmpty());

        // Now find the join and get all of the columns from the first scanned
        // table in the join operation
        Collection<AbstractJoinPlanNode> join_nodes = PlanNodeUtil.getPlanNodes(root, AbstractJoinPlanNode.class);
        assertNotNull(join_nodes);
        assertEquals(1, join_nodes.size());
        AbstractJoinPlanNode join_node = CollectionUtil.getFirst(join_nodes);
        assertNotNull(join_node);

        // Remove the columns from the second table
        Collection<Column> join_columns = CatalogUtil.getReferencedColumnsForPlanNode(catalog_db, join_node);
        assertNotNull(join_columns);
        assertFalse(join_columns.isEmpty());
        // System.err.println(CatalogUtil.debug(join_columns));
        Iterator<Column> it = join_columns.iterator();
        while (it.hasNext()) {
            Column catalog_col = it.next();
            if (catalog_col.getParent().equals(catalog_tbl) == false) {
                it.remove();
            }
        } // WHILE
        assertFalse(join_columns.isEmpty());
        // System.err.println("COLUMNS: " + CatalogUtil.debug(join_columns));

        // Ok so now we have the list of columns that are filtered out in the
        // inline projection and the list of
        // columns that are used in the join from the first table. So we need to
        // make sure that
        // every table that is in the join is in the projection
        for (Column catalog_col : join_columns) {
            assert (proj_columns.contains(catalog_col)) : "Missing: " + CatalogUtil.getDisplayName(catalog_col);
        } // FOR

        // Lastly, we need to look at the root SEND node and get its output
        // columns, and make sure that they
        // are also included in the bottom projection
        Collection<Column> send_columns = PlanNodeUtil.getOutputColumnsForPlanNode(catalog_db, root);
        assertFalse(send_columns.isEmpty());
        for (Column catalog_col : send_columns) {
            assert (proj_columns.contains(catalog_col)) : "Missing: " + CatalogUtil.getDisplayName(catalog_col);
        } // FOR
    }

    /**
     * testAggregateColumnAddition
     */
    @Test
    public void testAggregateColumnAddition() throws Exception {
        Procedure catalog_proc = this.getProcedure("AggregateColumnAddition");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
        //validateNodeColumnOffsets(root);
        // System.err.println(PlanNodeUtil.debug(root));
    }

    /**
     * testAggregateOrderBy
     */
    @Test
    public void testAggregateOrderBy() throws Exception {
        Procedure catalog_proc = this.getProcedure("OrderBy");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
        //System.err.println(PlanNodeUtil.debug(root));
//        validateNodeColumnOffsets(root);
    }

    /**
     * testAggregateGroupBy
     */
    @Test
    public void testAggregateGroupBy() throws Exception {   
        Procedure catalog_proc = this.getProcedure("GroupBy");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
//        validateNodeColumnOffsets(root);
        // System.err.println(PlanNodeUtil.debug(root));
    }
}