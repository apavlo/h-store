package org.voltdb.planner;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 */
public class TestPlanOptimizations2 extends BaseTestCase {

//    private VoltProjectBuilder pb = new VoltProjectBuilder("test-planopt") {
//        {
//            File schema = new File(TestPlanOptimizations2.class.getResource("testopt-ddl.sql").getFile());
//            assert (schema.exists()) : "Schema: " + schema;
//            this.addSchema(schema.getAbsolutePath());
//
//            this.addPartitionInfo("TABLEA", "A_ID");
//            this.addPartitionInfo("TABLEB", "B_A_ID");
//            this.addPartitionInfo("TABLEC", "C_A_ID");
//
//            this.addStmtProcedure("DistinctAggregate", "SELECT COUNT(DISTINCT(TABLEB.B_ID)) AS DISTINCTNUMBER FROM TABLEA, TABLEB WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = ? AND TABLEB.B_ID < ?");
//            this.addStmtProcedure("DistinctCount", "SELECT COUNT(DISTINCT(TABLEB.B_A_ID)) FROM TABLEB");
//            this.addStmtProcedure("MaxGroup", "SELECT B_ID, Max(TABLEB.B_A_ID) FROM TABLEB GROUP BY B_ID");
//            this.addStmtProcedure("Max", "SELECT Max(TABLEB.B_A_ID) FROM TABLEB");
//            this.addStmtProcedure("Min", "SELECT Min(TABLEB.B_A_ID) FROM TABLEB");
//            this.addStmtProcedure("Aggregate", "SELECT COUNT(TABLEB.B_A_ID) AS cnt FROM TABLEB");
//            this.addStmtProcedure("Limit", "SELECT * FROM TABLEA WHERE TABLEA.A_ID > ? AND TABLEA.A_ID <= ? AND TABLEA.A_VALUE0 != ? LIMIT 15");
//            this.addStmtProcedure("LimitJoin", "SELECT TABLEA.A_ID,TABLEB.B_ID FROM TABLEA, TABLEB WHERE TABLEA.A_ID > ? AND TABLEA.A_ID = TABLEB.B_A_ID LIMIT 15");
//	        this.addStmtProcedure("ThreeWayJoin", "SELECT TABLEA.A_VALUE0, TABLEB.B_VALUE0, ((TABLEC.C_VALUE0 + TABLEC.C_VALUE1) / TABLEB.B_A_ID) AS blah FROM TABLEA, TABLEB, TABLEC WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = TABLEC.C_A_ID AND TABLEA.A_VALUE3 = ? AND TABLEC.C_A_ID = ? AND TABLEC.C_VALUE0 != ? AND TABLEC.C_VALUE1 != ?");
//	        this.addStmtProcedure("ThreeWayJoin2", "SELECT TABLEA.A_VALUE0, TABLEB.B_VALUE0, TABLEC.C_VALUE0, TABLEC.C_VALUE1, TABLEB.B_A_ID FROM TABLEA, TABLEB, TABLEC WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = TABLEC.C_A_ID AND TABLEA.A_VALUE3 = ? AND TABLEC.C_A_ID = ? AND TABLEC.C_VALUE0 != ? AND TABLEC.C_VALUE1 != ?");
//            this.addStmtProcedure("SingleProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
//            this.addStmtProcedure("JoinProjection", "SELECT TABLEA.A_ID, TABLEA.A_VALUE0, TABLEA.A_VALUE1, TABLEA.A_VALUE2, TABLEA.A_VALUE3, TABLEA.A_VALUE4 FROM TABLEA,TABLEB WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
//            this.addStmtProcedure("AggregateColumnAddition", "SELECT AVG(TABLEC.C_VALUE0), C_A_ID FROM TABLEC WHERE TABLEC.C_ID = ? GROUP BY C_A_ID");
//            this.addStmtProcedure("OrderBy", "SELECT TABLEC.C_A_ID FROM TABLEC ORDER BY TABLEC.C_A_ID, TABLEC.C_VALUE0");
//            this.addStmtProcedure("GroupBy", "SELECT MAX(TABLEC.C_ID) FROM TABLEC GROUP BY TABLEC.C_A_ID, TABLEC.C_VALUE0");
//            this.addStmtProcedure("ProjectOrderBy", "select A_ID, B_A_ID, B_VALUE0, B_VALUE1, B_VALUE2, A_VALUE5 FROM TABLEA, TABLEB WHERE A_ID = B_A_ID ORDER BY A_VALUE5 ASC LIMIT 25");
//        }
//    };

    private VoltProjectBuilder pb = new VoltProjectBuilder("test-planopt(auction-mark)") {
        {
            File schema = new File(TestPlanOptimizations2.class.getResource("auction-mark-snapshot.sql").getFile());
            assert (schema.exists()) : "Schema: " + schema;
            this.addSchema(schema.getAbsolutePath());

            this.addPartitionInfo("ITEM", "i_id");
            this.addPartitionInfo("USER_WATCH", "uw_u_id");
            this.addPartitionInfo("CATEGORY", "c_id");

     		this.addStmtProcedure("TwoTableJoin", "SELECT uw_u_id, i_id, i_u_id, i_name, i_current_price, i_end_date, i_status, uw_created FROM USER_WATCH, ITEM WHERE uw_u_id = ?    AND uw_i_id = i_id AND uw_i_u_id = i_u_id  ORDER BY i_end_date ASC LIMIT 25");
        }
    };

    
    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }

    protected void checkColumnIndex(TupleValueExpression expr, Map<String, Integer> tbl_map) {
        // check column exists in the map
        assert (tbl_map.containsKey(expr.getColumnAlias())) : expr.getColumnAlias() + " does not exist in scanned table";
        // check column has correct index
        assert (expr.getColumnIndex() == tbl_map.get(expr.getColumnAlias())) : "Column : " + expr.getColumnAlias() + " has offset: " + expr.getColumnIndex() + " expected: "
                + tbl_map.get(expr.getColumnAlias());
    }

    protected void checkColumnIndex(Column col, Map<String, Integer> tbl_map) {
        // check column exists in the map
        assert (tbl_map.containsKey(col.getName())) : col.getName() + " does not exist in intermediate table";
        // check column has correct index
        assert (col.getIndex() == tbl_map.get(col.getName())) : "Column : " + col.getName() + " has offset: " + col.getIndex() + " expected: " + tbl_map.get(col.getName());
    }

    /** Given a ScanNode, builds a map mapping column names to indices. **/
    protected Map<String, Integer> buildTableMap(AbstractScanPlanNode node) {
        // build hashmap mapping column names to column index values
        final Map<String, Integer> tbl_col_index_map = new HashMap<String, Integer>();
        CatalogMap<Column> node_map = catalog_db.getTables().get(node.getTargetTableName()).getColumns();
        assert (node_map != null) : " Failed to retrieve columns for table: " + node.getTargetTableName();
        Column[] node_tbl_cols = new Column[node_map.size()];
        node_map.toArray(node_tbl_cols);
        for (Column col : node_tbl_cols) {
            tbl_col_index_map.put(col.getName(), col.getIndex());
        }
        return tbl_col_index_map;
    }

    /**
     * Given a plan node, checks to see all the expressions within the plan node
     * match up with the intermediate table
     **/
    protected void checkExpressionOffsets(AbstractPlanNode node, final Map<String, Integer> tbl_map) {
        // check all tuplevalueexpression offsets in the scannode
        // System.out.println("map: " + tbl_map);
        for (AbstractExpression exp : PlanNodeUtil.getExpressions(node)) {
            new ExpressionTreeWalker() {
                @Override
                protected void callback(AbstractExpression exp_element) {
                    if (exp_element instanceof TupleValueExpression) {
                        // System.out.println("element column: " +
                        // ((TupleValueExpression)exp_element).getColumnAlias()
                        // + " element index: " +
                        // ((TupleValueExpression)exp_element).getColumnIndex());
                        checkColumnIndex((TupleValueExpression) exp_element, tbl_map);
                    }
                }
            }.traverse(exp);
        }
    }

    /** Updates intermediate table for column offsets **/
    protected void updateIntermediateTblOffset(AbstractPlanNode node, final Map<String, Integer> intermediate_offset_tbl) {
        int offset_cnt = 0;
        intermediate_offset_tbl.clear();
        for (Integer col_guid : node.m_outputColumns) {
            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
            // TupleValueExpression tv_expr =
            // (TupleValueExpression)plan_col.getExpression();
            intermediate_offset_tbl.put(plan_col.displayName(), offset_cnt);
            offset_cnt++;
        }
    }

    /** Updates intermediate table for column GUIDs **/
    protected void updateIntermediateTblGUIDs(AbstractPlanNode node, final Map<String, Integer> intermediate_GUID_tbl) {
        int offset_cnt = 0;
        intermediate_GUID_tbl.clear();
        for (Integer col_guid : node.m_outputColumns) {
            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
            // TupleValueExpression tv_expr =
            // (TupleValueExpression)plan_col.getExpression();
            intermediate_GUID_tbl.put(plan_col.displayName(), plan_col.guid());
        }
    }

    protected void checkTableOffsets(AbstractPlanNode node, final Map<String, Integer> tbl_map) {
        /** Aggregates **/
        if (node instanceof AggregatePlanNode) {
            /** check aggregate column offsets **/
            for (Integer col_guid : ((AggregatePlanNode) node).getAggregateColumnGuids()) {
                PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                assert (plan_col.getExpression().getExpressionType().equals(ExpressionType.VALUE_TUPLE)) : " plan column expression type is: " + plan_col.getExpression().getExpressionType()
                        + " NOT TupleValueExpression";
                TupleValueExpression tv_exp = (TupleValueExpression) plan_col.getExpression();
                checkColumnIndex(tv_exp, tbl_map);
            }
            /** check output column offsets **/
            for (Integer col_guid : ((AggregatePlanNode) node).getAggregateOutputColumns()) {
                PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                assert (plan_col.getExpression().getExpressionType().equals(ExpressionType.VALUE_TUPLE)) : " plan column expression type is: " + plan_col.getExpression().getExpressionType()
                        + " NOT TupleValueExpression";
                TupleValueExpression tv_exp = (TupleValueExpression) plan_col.getExpression();
                checkColumnIndex(tv_exp, tbl_map);
            }
            /** check group by column offsets **/
            for (Integer col_guid : ((AggregatePlanNode) node).getGroupByColumns()) {
                PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                assert (plan_col.getExpression().getExpressionType().equals(ExpressionType.VALUE_TUPLE)) : " plan column expression type is: " + plan_col.getExpression().getExpressionType()
                        + " NOT TupleValueExpression";
                TupleValueExpression tv_exp = (TupleValueExpression) plan_col.getExpression();
                checkColumnIndex(tv_exp, tbl_map);
            }
            /** Order By's **/
        } else if (node instanceof OrderByPlanNode) {

        } else {
            // check the offsets of the output column
            // Set<TupleValueExpression> ExpressionUtil.getExpressions(node,
            // TupleValueExpression.class);
            for (Integer col_guid : node.m_outputColumns) {
                PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                new ExpressionTreeWalker() {
                    @SuppressWarnings("unchecked")
                    @Override
                    protected void callback(AbstractExpression element) {
                        if (element.getClass().equals(TupleValueExpression.class)) {
                            assert (element.getExpressionType().equals(ExpressionType.VALUE_TUPLE)) : "plan column expression type is: " + element.getExpressionType() + " NOT TupleValueExpression";
                            TupleValueExpression tv_exp = (TupleValueExpression) element;
                            checkColumnIndex(tv_exp, tbl_map);
                        }
                        return;
                    }
                }.traverse(plan_col.getExpression());
            }
        }
    }

    /**
     * make sure the output columns of a node exactly match its inline columns
     * (guid for guid)
     **/
    protected void checkMatchInline(AbstractPlanNode node, AbstractPlanNode inline_node) {
        for (int i = 0; i < node.m_outputColumns.size(); i++) {
            assert ((int) node.m_outputColumns.get(i) == (int) inline_node.m_outputColumns.get(i)) : "Node guid: " + node.m_outputColumns.get(i) + " doesn't match inline plan guid: "
                    + inline_node.m_outputColumns.get(i);
        }
    }

    /**
     * walk through the output columns of the current plan_node and compare the
     * GUIDs of the output columns of the child of the current plan_node
     **/
    protected void checkNodeColumnGUIDs(AbstractPlanNode plan_node, Map<String, Integer> intermediate_GUID_tbl) {
        for (int orig_guid : plan_node.m_outputColumns) {
            PlanColumn orig_pc = PlannerContext.singleton().get(orig_guid);
            boolean found = false;
            for (String int_name : intermediate_GUID_tbl.keySet()) {
                if (orig_pc.displayName().equals(int_name)) {
                    assert ((int) orig_guid == (int) intermediate_GUID_tbl.get(int_name)) : "Column name: " + int_name + " guid id: " + orig_guid + " doesn't match guid from child: "
                            + intermediate_GUID_tbl.get(int_name);
                }
            }
        }
    }

    /** walk the tree starting at the given root and validate **/
    protected void validateNodeColumnOffsets(AbstractPlanNode node) {
        final int total_depth = PlanNodeUtil.getDepth(node);

        // maintain data structure - (most recent "immediate" table)
        final Map<String, Integer> intermediate_offset_tbl = new HashMap<String, Integer>();
        // maintain column name to GUID
        final Map<String, Integer> intermediate_GUID_tbl = new HashMap<String, Integer>();

        new PlanNodeTreeWalker() {
            @Override
            protected void callback(AbstractPlanNode element) {
                // skip the column offset checking for the root send - the EE
                // doesn't care
                if (this.getDepth() != 0) {
                    /** Bottom Scan Node **/
                    if (this.getDepth() == total_depth && element instanceof AbstractScanPlanNode) {
                        // if its bottom most node (scan node), check offsets
                        // against
                        // the table being scanned
                        Map<String, Integer> target_tbl_map = buildTableMap((AbstractScanPlanNode) element);
                        checkExpressionOffsets(element, target_tbl_map);
                        checkTableOffsets(element, target_tbl_map);
                        // if inline nodes exist, check offsets of output
                        // columns match inline projection output columns
                        if (element.getInlinePlanNodes().size() > 0) {
                            // only 1 inline plan node - must be projection
                            assert (element.getInlinePlanNodes().size() == 1) : "More than 1 Inline Nodes in leaf Scan Node";
                            assert (element.getInlinePlanNode(PlanNodeType.PROJECTION) != null) : "Leaf scan node's inline node is not a projection";
                            // TO D0: compare inline projection columns with
                            // output columns of scan - should be identical
                            checkMatchInline(element, element.getInlinePlanNode(PlanNodeType.PROJECTION));
                        }
                        // update the intermediate table offsets + GUIDS - with
                        // output columns from the scan
                        updateIntermediateTblOffset(element, intermediate_offset_tbl);
                        updateIntermediateTblGUIDs(element, intermediate_GUID_tbl);
                    }
                    /** NestLoopIndex Node **/
                    else if (element instanceof NestLoopIndexPlanNode) {
                        // The only type of join we're currently handling. The
                        // join mashes the Receive node
                        // intermediate table with the inline index scan
                        // check the inline scan node's column offsets are based
                        // on the "mashing" of the intermediate table
                        // and the target scan table
                        assert (element.getInlinePlanNodes().size() == 1) : "More than 1 Inline Nodes in NestLoopIndex";
                        assert (element.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null || element.getInlinePlanNode(PlanNodeType.SEQSCAN) != null) : "No scan nodes exist in inline plan nodes";
                        AbstractScanPlanNode scan_node = (AbstractScanPlanNode) CollectionUtil.getFirst(element.getInlinePlanNodes().values());
                        // get all columns of the "target table" being scanned
                        // and append them to the current intermediate table
                        // + determine new offsets + determine new guids
                        Map<String, Integer> scan_node_map = buildTableMap(scan_node);
                        Integer intermediate_tbl_offset = intermediate_offset_tbl.size();
                        for (Map.Entry<String, Integer> col : scan_node_map.entrySet()) {
                            intermediate_offset_tbl.put(col.getKey(), intermediate_tbl_offset + col.getValue());
                        }
                        for (Integer col_guid : scan_node.m_outputColumns) {
                            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                            intermediate_GUID_tbl.put(plan_col.displayName(), plan_col.guid());
                        }
                        // check that expression column offsets + output column
                        // offsets match + GUIDs match with the original target
                        // table
                        checkExpressionOffsets(scan_node, intermediate_offset_tbl);
                        checkTableOffsets(scan_node, intermediate_offset_tbl);
                        checkMatchInline(element, scan_node);
                        checkNodeColumnGUIDs(element, intermediate_GUID_tbl);
                    }
                    /** Projection Node **/
                    else if (element instanceof ProjectionPlanNode) {
                        // check that expression column offsets + output column
                        // offsets match + GUIDs match with the original target
                        // table
                        checkExpressionOffsets(element, intermediate_offset_tbl);
                        checkTableOffsets(element, intermediate_offset_tbl);
                        checkNodeColumnGUIDs(element, intermediate_offset_tbl);
                        // update intermediate table (offset + guids)
                        updateIntermediateTblOffset(element, intermediate_offset_tbl);
                        updateIntermediateTblGUIDs(element, intermediate_GUID_tbl);
                    } else if (element instanceof AggregatePlanNode) {
                        // only want to check the expression here because the
                        // output will be different for aggregates
                        checkExpressionOffsets(element, intermediate_offset_tbl);
                        // update intermediate table to reflect output of
                        // aggregates
                        updateIntermediateTblOffset(element, intermediate_offset_tbl);
                    } else if (element instanceof OrderByPlanNode) {
                        // check sort column GUIDs against the intermediate
                        // table
                        for (int order_node_guid : ((OrderByPlanNode) element).getSortColumnGuids()) {
                            PlanColumn order_node_pc = PlannerContext.singleton().get(order_node_guid);
                            int int_order_node_guid = -1;
                            int_order_node_guid = intermediate_GUID_tbl.get(order_node_pc.displayName());
                            assert (int_order_node_guid != -1) : order_node_pc.displayName() + " doesn't exist in intermediate table";
                            assert (order_node_guid == int_order_node_guid) : order_node_pc.displayName() + " sort column guid: " + order_node_guid + " doesn't match: " + int_order_node_guid;
                        }
                        // only want to check the expression here because the
                        // output will be different for aggregates
                        checkExpressionOffsets(element, intermediate_offset_tbl);
                        // update intermediate table to reflect output of
                        // aggregates
                        updateIntermediateTblOffset(element, intermediate_offset_tbl);
                    }
                    /**
                     * Any other types of AbstractPlanNode (Send, Recieve,
                     * Limit, etc.)
                     **/
                    else {
                        checkExpressionOffsets(element, intermediate_offset_tbl);
                        checkTableOffsets(element, intermediate_offset_tbl);
                    }
                }
            }
        }.traverse(node);
    }

//    @Test
//    public void testDistinct() throws Exception {
//        Procedure catalog_proc = this.getProcedure("DistinctAggregate");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        //validateNodeColumnOffsets(root);
//        //System.err.println(PlanNodeUtil.debug(root));
//    }    
//    
//    @Test
//    public void testCountDistinct() throws Exception {
//        Procedure catalog_proc = this.getProcedure("DistinctCount");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
////        validateNodeColumnOffsets(root);
//        //System.err.println(PlanNodeUtil.debug(root));
//    }
    
    /** Other NON-max test cases **/
//    @Test
//    public void testMaxGroup() throws Exception {
//        Procedure catalog_proc = this.getProcedure("MaxGroup");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
////        validateNodeColumnOffsets(root);
////        System.err.println(PlanNodeUtil.debug(root));
//    }
//    
//    @Test
//    public void testMax() throws Exception {
//        Procedure catalog_proc = this.getProcedure("Max");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
////        validateNodeColumnOffsets(root);
////        System.err.println(PlanNodeUtil.debug(root));
//    }
//    
//    @Test
//    public void testMin() throws Exception {
//        Procedure catalog_proc = this.getProcedure("Min");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
////        validateNodeColumnOffsets(root);
////        System.err.println(PlanNodeUtil.debug(root));
//    }
//
//    @Test
//    public void testAggregate() throws Exception {
//        Procedure catalog_proc = this.getProcedure("Aggregate");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
////        validateNodeColumnOffsets(root);
////        System.err.println(PlanNodeUtil.debug(root));
//    }
//
//    @Test
//    public void testLimit() throws Exception {
//        Procedure catalog_proc = this.getProcedure("Limit");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        //validateNodeColumnOffsets(root);
//        assertNotNull(root);
//
//        // First check that our single scan node has an limit node
//        Set<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
//        assertEquals(1, limit_nodes.size());
//
//        // Get the Limit nodes output columns and make sure their valid
//        LimitPlanNode limit_node = CollectionUtil.getFirst(limit_nodes);
//        assertNotNull(limit_node);
//        for (int column_guid : limit_node.m_outputColumns) {
//            PlanColumn column = PlannerContext.singleton().get(column_guid);
//            // System.err.println(String.format("[%02d] %s", column_guid,
//            // column));
//            // System.err.println("==================");
//            // System.err.println(PlannerContext.singleton().debug());
//            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
//            assertEquals(column_guid, column.guid());
//        } // FOR
//
//        // System.err.println(PlanNodeUtil.debug(root));
//    }
//
//    @Test
//    public void testLimitJoin() throws Exception {
//        Procedure catalog_proc = this.getProcedure("LimitJoin");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        //validateNodeColumnOffsets(root);
//        assertNotNull(root);
//
//        // First check that our single scan node has an limit node
//        Set<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
//        assertEquals(1, limit_nodes.size());
//
//        // Get the Limit nodes output columns and make sure their valid
//        LimitPlanNode limit_node = CollectionUtil.getFirst(limit_nodes);
//        assertNotNull(limit_node);
//        for (int column_guid : limit_node.m_outputColumns) {
//            PlanColumn column = PlannerContext.singleton().get(column_guid);
//            // System.err.println(String.format("[%02d] %s", column_guid,
//            // column));
//            // System.err.println("==================");
//            // System.err.println(PlannerContext.singleton().debug());
//            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
//            assertEquals(column_guid, column.guid());
//        } // FOR
//
//        // System.err.println(PlanNodeUtil.debug(root));
//    }
//
//    @Test
//    public void testThreeWayJoin() throws Exception {
//        Procedure catalog_proc = this.getProcedure("ThreeWayJoin");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//
//        //System.err.println(PlanNodeUtil.debug(root));
//        //validateNodeColumnOffsets(root);
//        assertNotNull(root);
//    }
//
//  @Test
//  	public void testThreeWayJoin2() throws Exception {
//      Procedure catalog_proc = this.getProcedure("ThreeWayJoin2");
//      Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//      // Grab the root node of the multi-partition query plan tree for this
//      // Statement
//      AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//
//      //System.err.println(PlanNodeUtil.debug(root));
//      //validateNodeColumnOffsets(root);
//      assertNotNull(root);
//  }
//    
//    @Test
//    public void testSingleProjectionPushdown() throws Exception {
//        Procedure catalog_proc = this.getProcedure("SingleProjection");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        assertNotNull(root);
//        // //validateNodeColumnOffsets(root);
//        // System.err.println(PlanNodeUtil.debug(root));
//        // First check that our single scan node has an inline Projection
//        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
//        assertEquals(1, scan_nodes.size());
//        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
//        assertNotNull(scan_node);
//        assertEquals(1, scan_node.getInlinePlanNodes().size());
//
//        // Get the Projection and make sure it has valid output columns
//        ProjectionPlanNode inline_proj = (ProjectionPlanNode) scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
//        assertNotNull(inline_proj);
//        for (int column_guid : inline_proj.m_outputColumns) {
//            PlanColumn column = PlannerContext.singleton().get(column_guid);
//            // System.err.println(String.format("[%02d] %s", column_guid,
//            // column));
//            // System.err.println("==================");
//            // System.err.println(PlannerContext.singleton().debug());
//            assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
//            assertEquals(column_guid, column.guid());
//        } // FOR
//
//        // // Now check to make sure there are no other Projections in the tree
//        // Set<ProjectionPlanNode> proj_nodes =
//        PlanNodeUtil.getPlanNodes(root, ProjectionPlanNode.class);
//        // assertEquals(0, proj_nodes.size());
//    }
//
//    @Test
//    public void testJoinProjectionPushdown() throws Exception {
//        Procedure catalog_proc = this.getProcedure("JoinProjection");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        assertNotNull(root);
//        //validateNodeColumnOffsets(root);
//
//        new PlanNodeTreeWalker() {
//
//            @Override
//            protected void callback(AbstractPlanNode element) {
//                // System.out.println("element plannodetype: " +
//                // element.getPlanNodeType() + " depth: " + this.getDepth());
//            }
//        }.traverse(root);
//        //
//        // System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
//        //
//        // System.err.println(PlanNodeUtil.debug(root));
//        // //
//        //
//        // System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
//        // //
//        // // System.err.println("# of Fragments: " +
//        // catalog_stmt.getMs_fragments().size());
//        // // for (PlanFragment pf : catalog_stmt.getMs_fragments()) {
//        // // System.err.println(pf.getName() + "\n" +
//        // PlanNodeUtil.debug(QueryPlanUtil.deserializePlanFragment(pf)));
//        // }
//
//        // At the very bottom of our tree should be a scan. Grab that and then
//        // check to see that it has an inline ProjectionPlanNode. We will then
//        // look to see whether
//        // all of the columns
//        // we need to join are included. Note that we don't care which table is
//        // scanned first, as we can
//        // dynamically figure things out for ourselves
//        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
//        assertEquals(1, scan_nodes.size());
//        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
//        assertNotNull(scan_node);
//        Table catalog_tbl = this.getTable(scan_node.getTargetTableName());
//
//        assertEquals(1, scan_node.getInlinePlanNodes().size());
//        ProjectionPlanNode inline_proj = (ProjectionPlanNode) scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
//        assertNotNull(inline_proj);
//
//        // Validate output columns
//        for (int column_guid : inline_proj.m_outputColumns) {
//            PlanColumn column = PlannerContext.singleton().get(column_guid);
//            assertNotNull("Missing PlanColumn [guid=" + column_guid + "]", column);
//            assertEquals(column_guid, column.guid());
//
//            // Check that only columns from the scanned table are there
//            String table_name = column.originTableName();
//            assertNotNull(table_name);
//            String column_name = column.originColumnName();
//            assertNotNull(column_name);
//            assertEquals(table_name + "." + column_name, catalog_tbl.getName(), table_name);
//            assertNotNull(table_name + "." + column_name, catalog_tbl.getColumns().get(column_name));
//        } // FOR
//
//        Set<Column> proj_columns = null;
//        proj_columns = PlanNodeUtil.getOutputColumns(catalog_db, inline_proj);
//        assertFalse(proj_columns.isEmpty());
//
//        // Now find the join and get all of the columns from the first scanned
//        // table in the join operation
//        Set<AbstractJoinPlanNode> join_nodes = PlanNodeUtil.getPlanNodes(root, AbstractJoinPlanNode.class);
//        assertNotNull(join_nodes);
//        assertEquals(1, join_nodes.size());
//        AbstractJoinPlanNode join_node = CollectionUtil.getFirst(join_nodes);
//        assertNotNull(join_node);
//
//        // Remove the columns from the second table
//        Set<Column> join_columns = CatalogUtil.getReferencedColumns(catalog_db, join_node);
//        assertNotNull(join_columns);
//        assertFalse(join_columns.isEmpty());
//        // System.err.println(CatalogUtil.debug(join_columns));
//        Iterator<Column> it = join_columns.iterator();
//        while (it.hasNext()) {
//            Column catalog_col = it.next();
//            if (catalog_col.getParent().equals(catalog_tbl) == false) {
//                it.remove();
//            }
//        } // WHILE
//        assertFalse(join_columns.isEmpty());
//        // System.err.println("COLUMNS: " + CatalogUtil.debug(join_columns));
//
//        // Ok so now we have the list of columns that are filtered out in the
//        // inline projection and the list of
//        // columns that are used in the join from the first table. So we need to
//        // make sure that
//        // every table that is in the join is in the projection
//        for (Column catalog_col : join_columns) {
//            assert (proj_columns.contains(catalog_col)) : "Missing: " + CatalogUtil.getDisplayName(catalog_col);
//        } // FOR
//
//        // Lastly, we need to look at the root SEND node and get its output
//        // columns, and make sure that they
//        // are also included in the bottom projection
//        Set<Column> send_columns = PlanNodeUtil.getOutputColumns(catalog_db, root);
//        assertFalse(send_columns.isEmpty());
//        for (Column catalog_col : send_columns) {
//            assert (proj_columns.contains(catalog_col)) : "Missing: " + CatalogUtil.getDisplayName(catalog_col);
//        } // FOR
//    }
//
//    @Test
//    public void testAggregateColumnAddition() throws Exception {
//        Procedure catalog_proc = this.getProcedure("AggregateColumnAddition");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
//        //validateNodeColumnOffsets(root);
//        // System.err.println(PlanNodeUtil.debug(root));
//    }
//
//    @Test
//    public void testAggregateOrderBy() throws Exception {
//        Procedure catalog_proc = this.getProcedure("OrderBy");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
//        //System.err.println(PlanNodeUtil.debug(root));
//        //validateNodeColumnOffsets(root);
//    }
//
//    @Test
//    public void testAggregateGroupBy() throws Exception {   
//        Procedure catalog_proc = this.getProcedure("GroupBy");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
//        //validateNodeColumnOffsets(root);
//        //System.err.println(PlanNodeUtil.debug(root));
//    }
//
//    @Test
//    public void testProjectOrderBy() throws Exception {   
//        Procedure catalog_proc = this.getProcedure("ProjectOrderBy");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//
//        // Grab the root node of the multi-partition query plan tree for this
//        // Statement
//        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        assertNotNull(root);
//        //validateNodeColumnOffsets(root);
//        //System.err.println(PlanNodeUtil.debug(root));
//    }
    
	@Test
	public void testTwoTableJoin() throws Exception {
		Procedure catalog_proc = this.getProcedure("TwoTableJoin");
		Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

		// Grab the root node of the multi-partition query plan tree for this
		// Statement
		AbstractPlanNode root = QueryPlanUtil.deserializeStatement(
				catalog_stmt, true);
		assertNotNull(root);
		//validateNodeColumnOffsets(root);
		System.err.println(PlanNodeUtil.debug(root));
	}
    
    /** END **/
}