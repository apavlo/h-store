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
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 */
public class TestPlanOptimizations2 extends BaseTestCase {

    private VoltProjectBuilder pb = new VoltProjectBuilder("test-planopt") {
        {
            File schema = new File(TestPlanOptimizations2.class.getResource("testopt-ddl.sql").getFile());
            assert (schema.exists()) : "Schema: " + schema;
            this.addSchema(schema.getAbsolutePath());

            this.addPartitionInfo("TABLEA", "A_ID");
            this.addPartitionInfo("TABLEB", "B_A_ID");
            this.addPartitionInfo("TABLEC", "C_A_ID");

            this.addStmtProcedure("SingleProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
            this.addStmtProcedure("Limit", "SELECT * FROM TABLEA WHERE TABLEA.A_ID > ? AND TABLEA.A_ID <= ? AND TABLEA.A_VALUE0 != ? LIMIT 15");
            this.addStmtProcedure("JoinProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA, TABLEB WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
            // NESTLOOP this.addStmtProcedure("ThreeWayJoin",
            // "SELECT TABLEA.A_VALUE0, TABLEB.B_VALUE0, (TABLEC.C_VALUE0 + TABLEC.C_VALUE1) AS blah FROM TABLEA, TABLEB, TABLEC WHERE TABLEA.A_ID = TABLEB.B_ID AND TABLEA.A_ID = TABLEC.C_A_ID AND TABLEC.C_A_ID = ? AND TABLEC.C_VALUE0 != ?");
            this
                    .addStmtProcedure(
                            "ThreeWayJoin",
                            "SELECT TABLEA.A_VALUE0, TABLEB.B_VALUE0, (TABLEC.C_VALUE0 + TABLEC.C_VALUE1) AS blah FROM TABLEA, TABLEB, TABLEC WHERE TABLEA.A_ID = TABLEB.B_A_ID AND TABLEA.A_ID = TABLEC.C_A_ID AND TABLEC.C_A_ID = ? AND TABLEC.C_VALUE0 != ?");
            this.addStmtProcedure("Aggregate", "SELECT COUNT(TABLEB.B_A_ID) AS cnt FROM TABLEB WHERE TABLEB.B_ID = ?");
        }
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }

    protected void checkColumnIndex(TupleValueExpression expr, Map<String, Integer> tbl_map) {
        // check column exists in the map
        assert (tbl_map.containsKey(expr.getColumnName())) : expr.getColumnName() + " does not exist in scanned table";
        // check column has correct index
        assert (expr.getColumnIndex() == tbl_map.get(expr.getColumnName())) : "Column : " + expr.getColumnName() + " has offset: " + expr.getColumnIndex() + " expected: " + tbl_map.get(expr.getColumnName());
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

    /** Given a plan node, checks to see all the expressions within the plan node match up with the intermediate table **/
    protected void checkExpressionOffsets(AbstractPlanNode node,  final Map<String, Integer> tbl_map) {
        // check all tuplevalueexpression offsets in the scannode
        //System.out.println("map: " + tbl_map);
        for (AbstractExpression exp : PlanNodeUtil.getExpressions(node)) {
            new ExpressionTreeWalker() {
                @Override
                protected void callback(AbstractExpression exp_element) {
                    if (exp_element instanceof TupleValueExpression) {
                        //System.out.println("element column: " + ((TupleValueExpression)exp_element).getColumnName() + " element index: " + ((TupleValueExpression)exp_element).getColumnIndex());
                        checkColumnIndex((TupleValueExpression)exp_element, tbl_map);
                    }
                }
            }.traverse(exp);
        }        
    }

    protected void checkTableOffsets(AbstractPlanNode node,  final Map<String, Integer> tbl_map) {
        // check the offsets of the output column
        System.out.println("Table map: " + tbl_map + " node type: " + node.getPlanNodeType());
        for (Integer col_guid : node.m_outputColumns) {
            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
            assert (plan_col.getExpression().getExpressionType().equals(ExpressionType.VALUE_TUPLE)) : "plan column expression type is: " + plan_col.getExpression().getExpressionType() + " NOT TupleValueExpression";
            TupleValueExpression tv_exp = (TupleValueExpression)plan_col.getExpression();
            System.out.println("out Column Name: " + tv_exp.getColumnName() + " Column index: " + tv_exp.getColumnIndex());
            checkColumnIndex(tv_exp, tbl_map);
        }        
    }
    
    /** make sure the output columns of a node exactly match its inline columns (guid for guid) **/
    protected void checkMatchInline(AbstractPlanNode node, AbstractPlanNode inline_node) {
        for (int i = 0; i < node.m_outputColumns.size(); i++) {
            assert (node.m_outputColumns.get(i) == inline_node.m_outputColumns.get(i)) : "Node guid: " + node.m_outputColumns.get(i) + " doesn't match inline plan guid: " + inline_node.m_outputColumns.get(i);
        }
    }

    /** walk the tree starting at the given root and validate **/
    protected void validateNodeColumnOffsets(AbstractPlanNode node) {
        final int total_depth = PlanNodeUtil.getDepth(node);

        // maintain data structure - (most recent "immediate" table)
        final Map<String, Integer> intermediate_tbl = new HashMap<String, Integer>();
        
        new PlanNodeTreeWalker() {
            @Override
            protected void callback(AbstractPlanNode element) {

                /** Bottom Scan Node **/
                if (this.getDepth() == total_depth && element instanceof AbstractScanPlanNode) {
                    // if its bottom most node (scan node), check offsets against
                    // the table being scanned
                    Map<String, Integer> target_tbl_map = buildTableMap((AbstractScanPlanNode)element);
                    checkExpressionOffsets(element, target_tbl_map);
                    checkTableOffsets(element, target_tbl_map);
                    // if inline nodes exist, check offsets of output columns match inline projection output columns
                    if (element.getInlinePlanNodes().size() > 0) {
                        // only 1 inline plan node - must be projection
                        assert (element.getInlinePlanNodes().size() == 1) : "More than 1 Inline Nodes in leaf Scan Node";
                        assert (element.getInlinePlanNode(PlanNodeType.PROJECTION) != null) : "Leaf scan node's inline node is not a projection";
                        // TO D0: compare inline projection columns with output columns of scan - should be identical
                        checkMatchInline(element, element.getInlinePlanNode(PlanNodeType.PROJECTION));
                    }
                    // update the intermediate table - with output columns from the scan
                    for (Column col : PlanNodeUtil.getOutputColumns(catalog_db, element)) {
                        intermediate_tbl.put(col.getName(), col.getIndex());
                    }                    
                }
                /** NestLoopIndex Node **/
                else if (element instanceof NestLoopIndexPlanNode) {
                    // The only type of join we're currently handling. The join mashes the Receive node
                    // intermediate table with the inline index scan
                    System.out.println("First nestedloopindex, intermediate tables are: " + intermediate_tbl);
                    
                    // check the inline scan node's column offsets are based on the "mashing" of the intermediate table
                    // and the target scan table
                    assert (element.getInlinePlanNodes().size() == 1) : "More than 1 Inline Nodes in NestLoopIndex";
                    assert (element.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null || element.getInlinePlanNode(PlanNodeType.SEQSCAN) != null) : "No scan nodes exist in inline plan nodes";
                    AbstractScanPlanNode scan_node = (AbstractScanPlanNode)CollectionUtil.getFirst(element.getInlinePlanNodes().values());
                    // get all columns of the "target table" being scanned and append them to the current intermediate table
                    Map<String, Integer> scan_node_map = buildTableMap(scan_node);
                    Integer intermediate_tbl_offset = intermediate_tbl.size();
                    for (Map.Entry<String, Integer> col : scan_node_map.entrySet()) {
                        intermediate_tbl.put(col.getKey(), intermediate_tbl_offset + col.getValue());
                    }
                    // check that the expression column offsets match up with the "intermediate" table
                    checkExpressionOffsets(scan_node, intermediate_tbl);
                    // check that output column offsets match up with the original target table
                    checkTableOffsets(scan_node, intermediate_tbl);
                    checkMatchInline(element, scan_node);
                    System.out.println("finished checking first nest loop!!!");
                }
                /** Projection Node **/
                else if (element instanceof ProjectionPlanNode) {
                    // check the output columns of the projection against the "last pushed" intermediate table data structure
                    checkExpressionOffsets(element, intermediate_tbl);
                    checkTableOffsets(element, intermediate_tbl);
                    // update intermediate table
                    int offset_cnt = 0;
                    intermediate_tbl.clear();
                    for (Integer col_guid : element.m_outputColumns) {
                        PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                        TupleValueExpression tv_expr = (TupleValueExpression)plan_col.getExpression();
                        System.out.println("tuple value expr: " + tv_expr);
                        intermediate_tbl.put(tv_expr.getColumnName(), offset_cnt);
                        offset_cnt++;
                    }
                }
                
                /** Any other types of AbstractPlanNode (Send, Recieve, Limit, etc.) **/
                else {
                    checkExpressionOffsets(element, intermediate_tbl);
                    checkTableOffsets(element, intermediate_tbl);
                }

            }
        }.traverse(node);
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
     AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt,
     false);
     validateNodeColumnOffsets(root);
     assertNotNull(root);
    
     // First check that our single scan node has an limit node
     Set<LimitPlanNode> limit_nodes = PlanNodeUtil.getPlanNodes(root, LimitPlanNode.class);
     assertEquals(1, limit_nodes.size());
    
     // Get the Limit nodes output columns and make sure their valid
     LimitPlanNode limit_node = CollectionUtil.getFirst(limit_nodes);
     assertNotNull(limit_node);
     for (int column_guid : limit_node.m_outputColumns) {
     PlanColumn column = PlannerContext.singleton().get(column_guid);
     // System.err.println(String.format("[%02d] %s", column_guid,
     // column));
     // System.err.println("==================");
     // System.err.println(PlannerContext.singleton().debug());
     assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
     assertEquals(column_guid, column.guid());
     } // FOR
    
     //System.err.println(PlanNodeUtil.debug(root));
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
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
        // FIXME: validateNodeColumnOffsets(root);
        //System.err.println(PlanNodeUtil.debug(root));
        assertNotNull(root);
    }
    
        
     /**
     * testAggregate
     */
     @Test
     public void testAggregate() throws Exception {
     Procedure catalog_proc = this.getProcedure("Aggregate");
     Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
            
     // Grab the root node of the multi-partition query plan tree for this
     //Statement
     AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt,
     true);
     assertNotNull(root);
     //validateNodeColumnOffsets(root);
     //System.err.println(PlanNodeUtil.debug(root));
     }
        
     /**
     * testSingleProjectionPushdown
     */
     @Test
     public void testSingleProjectionPushdown() throws Exception {
     Procedure catalog_proc = this.getProcedure("SingleProjection");
     Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
            
     // Grab the root node of the multi-partition query plan tree for this
     //Statement
     AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt,
     false);
     assertNotNull(root);
     // FIXME: validateNodeColumnOffsets(root);
     //System.err.println(PlanNodeUtil.debug(root));    
     // First check that our single scan node has an inline Projection
     Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root,
     AbstractScanPlanNode.class);
     assertEquals(1, scan_nodes.size());
     AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
     assertNotNull(scan_node);
     assertEquals(1, scan_node.getInlinePlanNodes().size());
            
     // Get the Projection and make sure it has valid output columns
     ProjectionPlanNode inline_proj =
     (ProjectionPlanNode)scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
     assertNotNull(inline_proj);
     for (int column_guid : inline_proj.m_outputColumns) {
     PlanColumn column = PlannerContext.singleton().get(column_guid);
     // System.err.println(String.format("[%02d] %s", column_guid, column));
     // System.err.println("==================");
     // System.err.println(PlannerContext.singleton().debug());
     assertNotNull("Invalid PlanColumn [guid=" + column_guid + "]", column);
     assertEquals(column_guid, column.guid());
     } // FOR
    
     // // Now check to make sure there are no other Projections in the tree
     // Set<ProjectionPlanNode> proj_nodes = PlanNodeUtil.getPlanNodes(root,ProjectionPlanNode.class);
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
     //Statement
     AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt,
     false);
     assertNotNull(root);
     // FIXME: validateNodeColumnOffsets(root);
            
     new PlanNodeTreeWalker() {

         @Override
         protected void callback(AbstractPlanNode element) {
             //System.out.println("element plannodetype: " + element.getPlanNodeType() + " depth: " + this.getDepth());
         }
     }.traverse(root);
     //
     //System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
//            
      //System.err.println(PlanNodeUtil.debug(root));
//     //
//     System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
//     //
//     // System.err.println("# of Fragments: " +
//     catalog_stmt.getMs_fragments().size());
//     // for (PlanFragment pf : catalog_stmt.getMs_fragments()) {
//     // System.err.println(pf.getName() + "\n" +
//     PlanNodeUtil.debug(QueryPlanUtil.deserializePlanFragment(pf)));
     // }
            
            
     // At the very bottom of our tree should be a scan. Grab that and then
     //check to see that it has an inline ProjectionPlanNode. We will then look to see whether
     //all of the columns
     // we need to join are included. Note that we don't care which table is
     //scanned first, as we can
     // dynamically figure things out for ourselves
     Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root,
     AbstractScanPlanNode.class);
     assertEquals(1, scan_nodes.size());
     AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
     assertNotNull(scan_node);
     Table catalog_tbl = this.getTable(scan_node.getTargetTableName());
            
     assertEquals(1, scan_node.getInlinePlanNodes().size());
     ProjectionPlanNode inline_proj =
     (ProjectionPlanNode)scan_node.getInlinePlanNodes().get(PlanNodeType.PROJECTION);
     assertNotNull(inline_proj);
            
     // Validate output columns
     for (int column_guid : inline_proj.m_outputColumns) {
     PlanColumn column = PlannerContext.singleton().get(column_guid);
     assertNotNull("Missing PlanColumn [guid=" + column_guid + "]", column);
     assertEquals(column_guid, column.guid());
    
     // Check that only columns from the scanned table are there
     String table_name = column.originTableName();
     assertNotNull(table_name);
     String column_name = column.originColumnName();
     assertNotNull(column_name);
     assertEquals(table_name+"."+column_name, catalog_tbl.getName(),
     table_name);
     assertNotNull(table_name+"."+column_name,
     catalog_tbl.getColumns().get(column_name));
     } // FOR
            
     Set<Column> proj_columns = null;
     proj_columns = PlanNodeUtil.getOutputColumns(catalog_db, inline_proj);
     assertFalse(proj_columns.isEmpty());
            
     // Now find the join and get all of the columns from the first scanned table in the join operation
     Set<AbstractJoinPlanNode> join_nodes = PlanNodeUtil.getPlanNodes(root,
     AbstractJoinPlanNode.class);
     assertNotNull(join_nodes);
     assertEquals(1, join_nodes.size());
     AbstractJoinPlanNode join_node = CollectionUtil.getFirst(join_nodes);
     assertNotNull(join_node);
            
     // Remove the columns from the second table
     Set<Column> join_columns = CatalogUtil.getReferencedColumns(catalog_db,
     join_node);
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
     // columns that are used in the join from the first table. So we need to make sure that
     // every table that is in the join is in the projection
     for (Column catalog_col : join_columns) {
     assert(proj_columns.contains(catalog_col)) : "Missing: " +
     CatalogUtil.getDisplayName(catalog_col);
     } // FOR
            
     // Lastly, we need to look at the root SEND node and get its output
     // columns, and make sure that they
     // are also included in the bottom projection
     Set<Column> send_columns = PlanNodeUtil.getOutputColumns(catalog_db, root);
     assertFalse(send_columns.isEmpty());
     for (Column catalog_col : send_columns) {
     assert(proj_columns.contains(catalog_col)) : "Missing: " +
     CatalogUtil.getDisplayName(catalog_col);
     } // FOR
     }
}
