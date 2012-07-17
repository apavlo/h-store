package edu.brown.optimizer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
* @author pavlo
*/
public class TestPlanOptimizer extends BasePlanOptimizerTestCase {

    final Set<String> DEBUG = new HashSet<String>();
    {
        // DEBUG.add("DistinctAggregate");
        // DEBUG.add("MultipleAggregates");
        // DEBUG.add("JoinProjection");
        DEBUG.add("LimitNoWhere");
//        DEBUG.add("LimitOrderBy");
    }
    
    AbstractProjectBuilder pb = new PlanOptimizerTestProjectBuilder("planopt") {
        {
            this.addStmtProcedure("MultipleAggregates",
                                  "SELECT C_B_ID, SUM(C_VALUE0), SUM(C_VALUE1), " +
                                  " AVG(C_VALUE0), AVG(C_VALUE1) " +
                                  "FROM TABLEC GROUP BY C_B_ID");
            
            this.addStmtProcedure("DistinctAggregate",
                                  "SELECT COUNT(DISTINCT(TABLEB.B_ID)) AS DISTINCTNUMBER " +
                                  "FROM TABLEA, TABLEB " +
                                  "WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = ? AND TABLEB.B_ID < ?");
            
            this.addStmtProcedure("DistinctCount",
                                  "SELECT COUNT(DISTINCT(TABLEB.B_A_ID)) FROM TABLEB");
            
            this.addStmtProcedure("MaxGroupPassThrough",
                                  "SELECT B_ID, Max(TABLEB.B_A_ID) FROM TABLEB GROUP BY B_ID");
            
            this.addStmtProcedure("MaxMultiGroupBy",
                                  "SELECT MAX(TABLEC.C_ID) FROM TABLEC GROUP BY TABLEC.C_B_A_ID, TABLEC.C_VALUE0");
            
            this.addStmtProcedure("Max",
                                  "SELECT MAX(TABLEB.B_A_ID) FROM TABLEB");
            
            this.addStmtProcedure("Min",
                                  "SELECT MIN(TABLEB.B_A_ID) FROM TABLEB");
            
            this.addStmtProcedure("AggregateCount",
                                  "SELECT COUNT(TABLEB.B_A_ID) AS cnt, B_VALUE0 FROM TABLEB GROUP BY B_VALUE0");
            
            this.addStmtProcedure("LimitNoWhere",
                                  "SELECT * FROM TABLEA LIMIT 1");
            
            this.addStmtProcedure("Limit",
                                  "SELECT * FROM TABLEA WHERE TABLEA.A_ID > ? AND TABLEA.A_ID <= ? AND TABLEA.A_VALUE0 != ? LIMIT 15");
            
            this.addStmtProcedure("LimitJoin",
                                  "SELECT TABLEA.A_ID,TABLEB.B_ID FROM TABLEA, TABLEB WHERE TABLEA.A_ID > ? AND TABLEA.A_ID = TABLEB.B_A_ID LIMIT 15");
            
            this.addStmtProcedure("ThreeWayJoin",
                                  "SELECT TABLEA.A_VALUE0, TABLEB.B_VALUE0, ((TABLEC.C_VALUE0 + TABLEC.C_VALUE1) / TABLEB.B_A_ID) AS blah " +
                                  "FROM TABLEA, TABLEB, TABLEC " +
                                  "WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = TABLEC.C_B_A_ID AND TABLEA.A_VALUE3 = ? " +
                                  " AND TABLEC.C_B_A_ID = ? AND TABLEC.C_VALUE0 != ? AND TABLEC.C_VALUE1 != ?");
            
            this.addStmtProcedure("SingleProjection",
                                  "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
            
            this.addStmtProcedure("NonPartitioningProjection",
                                  "SELECT TABLEA.A_ID FROM TABLEA WHERE TABLEA.A_VALUE0 = ?");
            
            this.addStmtProcedure("JoinProjection",
                                  "SELECT TABLEA.A_ID, TABLEA.A_VALUE0, TABLEA.A_VALUE1, TABLEA.A_VALUE2, TABLEA.A_VALUE3, TABLEA.A_VALUE4 " +
                                  "FROM TABLEA, TABLEB " +
                                  "WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
            
            this.addStmtProcedure("AggregateColumnAddition",
                                  "SELECT AVG(TABLEC.C_VALUE0), C_B_A_ID " +
                                  " FROM TABLEC WHERE TABLEC.C_ID = ? GROUP BY C_B_A_ID");
            
            this.addStmtProcedure("OrderBy",
                                  "SELECT TABLEC.C_B_A_ID FROM TABLEC ORDER BY TABLEC.C_B_A_ID DESC, TABLEC.C_VALUE0 ASC");
            
            this.addStmtProcedure("LimitOrderBy",
                                  "SELECT C_ID FROM TABLEC ORDER BY C_B_A_ID LIMIT 1000");
            
            this.addStmtProcedure("SingleSelect",
                                  "SELECT A_ID FROM TABLEA");
            
            this.addStmtProcedure("TwoTableJoin",
                                  "SELECT B_ID, B_A_ID, B_VALUE0, C_ID, C_VALUE0 " +
                                  " FROM TABLEB, TABLEC " +
                                  " WHERE B_A_ID = ? AND B_ID = ? " +
                                  " AND B_A_ID = C_B_A_ID AND B_ID = C_B_ID " +
                                  " ORDER BY B_VALUE1 ASC LIMIT 25");
        }
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }
    
    private void check(Statement catalog_stmt) throws Exception {
        // Grab the root node of the multi-partition query plan tree for this Statement
        for (boolean dtxn : new boolean[]{ true, false }) {
            AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, dtxn);
            assertNotNull(root);
            if (DEBUG.contains(catalog_stmt.getParent().getName()))
                System.err.println(PlanNodeUtil.debug(root));
            BasePlanOptimizerTestCase.validate(root);
        } // FOR
    }
    
    /**
     * testMultipleAggregates
     */
    @Test
    public void testMultipleAggregates() throws Exception {
        Procedure catalog_proc = this.getProcedure("MultipleAggregates");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
    
    /**
     * testExtractReferencedColumns
     */
    @Test
    public void testExtractReferencedColumns() throws Exception {
        Procedure catalog_proc = this.getProcedure("DistinctCount");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        
        Collection<SeqScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, SeqScanPlanNode.class);
        SeqScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
        assertNotNull(scan_node);
        
        PlanOptimizerState state = new PlanOptimizerState(catalog_db, PlannerContext.singleton());
        Collection<PlanColumn> referenced = PlanOptimizerUtil.extractReferencedColumns(state, scan_node);
        assertNotNull(referenced);
        
        // Make sure all of the columns that we get back have a matching column in
        // the table scanned in the PlanNode
        Table catalog_tbl = this.getTable(scan_node.getTargetTableName());
// System.err.println(referenced);
        for (PlanColumn pc : referenced) {
            assertNotNull(pc);
            Collection<Column> columns = ExpressionUtil.getReferencedColumns(catalog_db, pc.getExpression());
            assertEquals(pc.toString(), 1, columns.size());
            Column catalog_col = CollectionUtil.first(columns);
            assertNotNull(pc.toString(), catalog_col);
            assertEquals(pc.toString(), catalog_tbl, catalog_col.getParent());
        } // FOR
    }

    /**
     * testDistinctAggregate
     */
    @Test
    public void testDistinctAggregate() throws Exception {
        Procedure catalog_proc = this.getProcedure("DistinctAggregate");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }

    /**
     * testDistinctCount
     */
    @Test
    public void testDistinctCount() throws Exception {
        Procedure catalog_proc = this.getProcedure("DistinctCount");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
    
    /**
     * testMaxGroupPassThrough
     */
    @Test
    public void testMaxGroupPassThrough() throws Exception {
        Procedure catalog_proc = this.getProcedure("MaxGroupPassThrough");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
    
    /**
     * testMaxMultiGroupBy
     */
    @Test
    public void testMaxMultiGroupBy() throws Exception {
        Procedure catalog_proc = this.getProcedure("MaxMultiGroupBy");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
    
    /**
     * testMax
    */
    @Test
    public void testMax() throws Exception {
        Procedure catalog_proc = this.getProcedure("Max");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
    
    /**
     * testMin
    */
    @Test
    public void testMin() throws Exception {
        Procedure catalog_proc = this.getProcedure("Min");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }

    /**
     * testAggregateCount
     */
    @Test
    public void testAggregateCount() throws Exception {
        Procedure catalog_proc = this.getProcedure("AggregateCount");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // We should have two AggregatePlanNodes.
        // Make sure that they have the same GroupByColumns
        Collection<AggregatePlanNode> agg_nodes = PlanNodeUtil.getPlanNodes(root, AggregatePlanNode.class);
        assertEquals(2, agg_nodes.size());
        
        AggregatePlanNode agg0 = CollectionUtil.get(agg_nodes, 0);
        assertNotNull(agg0);
        AggregatePlanNode agg1 = CollectionUtil.get(agg_nodes, 1);
        assertNotNull(agg1);
        assertNotSame(agg0, agg1);
        
// System.err.println(PlanNodeUtil.debug(root));
        
        assertEquals(agg0.getAggregateOutputColumns(), agg1.getAggregateOutputColumns());
        assertEquals(agg0.getGroupByColumnNames(), agg1.getGroupByColumnNames());
// assertEquals(agg0.getGroupByColumnGuids(), agg1.getGroupByColumnGuids());
    }

    /**
     * testLimitNoWhere
     */
    @Test
    public void testLimitNoWhere() throws Exception {
        Procedure catalog_proc = this.getProcedure("LimitNoWhere");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        //validateNodeColumnOffsets(root);
        assertNotNull(root);

        // We should have two LIMIT nodes:
        // (1) One that is inline on the SeqScan that executes at each partition 
        // (2) One that executes at the base partition
        Collection<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
        assertEquals(1, limit_nodes.size());

        Collection<SeqScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, SeqScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        LimitPlanNode inline_node = CollectionUtil.first(scan_nodes).getInlinePlanNode(PlanNodeType.LIMIT);
        assertNotNull(inline_node);
        
        // Get the Limit nodes output columns and make sure their valid
        for (LimitPlanNode limit_node : limit_nodes) {
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
        } // FOR
    }
    
    /**
     * testLimit
     */
    @Test
    public void testLimit() throws Exception {
        Procedure catalog_proc = this.getProcedure("Limit");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        //validateNodeColumnOffsets(root);
        assertNotNull(root);

        // First check that our single scan node has an limit node
        Collection<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
        assertEquals(1, limit_nodes.size());

        // Get the Limit nodes output columns and make sure their valid
        LimitPlanNode limit_node = CollectionUtil.first(limit_nodes);
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
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);

        // First check that our single scan node has an limit node
        Collection<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
        assertEquals(1, limit_nodes.size());

        // Get the Limit nodes output columns and make sure their valid
        LimitPlanNode limit_node = CollectionUtil.first(limit_nodes);
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
        this.check(catalog_stmt);
    }

    /**
     * testSingleProjection
     */
    @Test
    public void testSingleProjection() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        // First check that our single scan node has an inline Projection
        Collection<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
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

        // Now check to make sure there are no other Projections in the tree
        Collection<ProjectionPlanNode> proj_nodes = PlanNodeUtil.getPlanNodes(root, ProjectionPlanNode.class);
        assertEquals(0, proj_nodes.size());
    }
    
     /**
      * testNonPartitioningProjection
      */
     @Test
     public void testNonPartitioningProjection() throws Exception {   
         Procedure catalog_proc = this.getProcedure("NonPartitioningProjection");
         Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
         this.check(catalog_stmt);
     }
    

    /**
     * testJoinProjection
     */
    @Test
    public void testJoinProjection() throws Exception {
        Procedure catalog_proc = this.getProcedure("JoinProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        //validateNodeColumnOffsets(root);
        
        // Grab the single-partition root node and make sure that they have the same
        // output columns in their topmost send node
        AbstractPlanNode spRoot = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(spRoot);
        
        assertEquals(root.getOutputColumnGUIDCount(), spRoot.getOutputColumnGUIDCount());
        PlannerContext context = PlannerContext.singleton();
        assertNotNull(context);
        for (int i = 0, cnt = root.getOutputColumnGUIDCount(); i < cnt; i++) {
            Integer guid0 = root.getOutputColumnGUID(i);
            PlanColumn col0 = context.get(guid0);
            assertNotNull(col0);
            
            Integer guid1 = spRoot.getOutputColumnGUID(i);
            PlanColumn col1 = context.get(guid1);
            assertNotNull(col1);
            
            assertTrue(col0.equals(col1, false, true));
        } // FOR
        

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
        AbstractScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
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
        AbstractJoinPlanNode join_node = CollectionUtil.first(join_nodes);
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
        // columns, and make sure that they are also included in the bottom projection
        Collection<Column> send_columns = PlanNodeUtil.getOutputColumnsForPlanNode(catalog_db, root);
        assertFalse(send_columns.isEmpty());
        for (Column catalog_col : send_columns) {
            assert(proj_columns.contains(catalog_col)) :
                "Missing: " + CatalogUtil.getDisplayName(catalog_col);
        } // FOR
    }

    /**
     * testAggregateColumnAddition
     */
    @Test
    public void testAggregateColumnAddition() throws Exception {
        Procedure catalog_proc = this.getProcedure("AggregateColumnAddition");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }

    /**
     * testAggregateOrderBy
     */
    @Test
    public void testAggregateOrderBy() throws Exception {
        Procedure catalog_proc = this.getProcedure("OrderBy");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }

    /**
     * testLimitOrderBy
     */
    @Test
    public void testLimitOrderBy() throws Exception {
        Procedure catalog_proc = this.getProcedure("LimitOrderBy");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        
        // We should have two LIMITs and two ORDER BYs
        Class<?> planClasses[] = { LimitPlanNode.class, OrderByPlanNode.class };
        for (Class<?> c : planClasses) {
            @SuppressWarnings("unchecked")
            Collection<AbstractPlanNode> nodes = PlanNodeUtil.getPlanNodes(root, (Class<AbstractPlanNode>)c);
            assertNotNull(nodes);
            assertEquals(2, nodes.size());
            
            // Make sure each one only has one child!
            for (AbstractPlanNode node : nodes) {
                assertEquals(PlanNodeUtil.debug(node), 1, node.getChildPlanNodeCount());
            } // FOR
        } // FOR
    }
    
    /**
     * testSingleSelect
     */
    @Test
    public void testSingleSelect() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleSelect");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
     
    /**
     * testTwoTableJoin
     */
    @Test
    public void testTwoTableJoin() throws Exception {
        Procedure catalog_proc = this.getProcedure("TwoTableJoin");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        this.check(catalog_stmt);
    }
}