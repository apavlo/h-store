package edu.brown.optimizer;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

public abstract class BasePlanOptimizerTestCase extends BaseTestCase {
    
    public static class PlanOptimizerTestProjectBuilder extends AbstractProjectBuilder {
        
        public PlanOptimizerTestProjectBuilder(String name) {
            super("test-" + name, AbstractProjectBuilder.class, null, null);
            
            File schema = new File(BasePlanOptimizerTestCase.class.getResource("test-planopt-ddl.sql").getFile());
            assert(schema.exists()) : "Missing test schema file: " + schema;
            this.addSchema(schema);
            
            this.addTablePartitionInfo("TABLEA", "A_ID");
            this.addTablePartitionInfo("TABLEB", "B_A_ID");
            this.addTablePartitionInfo("TABLEC", "C_B_A_ID");
        }
    }
    
    
    @Override
    protected void setUp(AbstractProjectBuilder projectBuilder) throws Exception {
        super.setUp(projectBuilder);
    }
    
    public void checkColumnIndex(TupleValueExpression expr, Map<String, Integer> tbl_map) {
        // check column exists in the map
        assert (tbl_map.containsKey(expr.getColumnAlias())) : expr.getColumnAlias() + " does not exist in scanned table";
        // check column has correct index
        assert (expr.getColumnIndex() == tbl_map.get(expr.getColumnAlias())) : "Column : " + expr.getColumnAlias() + " has offset: " + expr.getColumnIndex() + " expected: "
                + tbl_map.get(expr.getColumnAlias());
    }

    public void checkColumnIndex(Column col, Map<String, Integer> tbl_map) {
        // check column exists in the map
        assert (tbl_map.containsKey(col.getName())) : col.getName() + " does not exist in intermediate table";
        // check column has correct index
        assert (col.getIndex() == tbl_map.get(col.getName())) : "Column : " + col.getName() + " has offset: " + col.getIndex() + " expected: " + tbl_map.get(col.getName());
    }

    /** Given a ScanNode, builds a map mapping column names to indices. **/
    public Map<String, Integer> buildTableMap(AbstractScanPlanNode node) {
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
    public void checkExpressionOffsets(AbstractPlanNode node, final Map<String, Integer> tbl_map) {
        // check all tuplevalueexpression offsets in the scannode
        // System.out.println("map: " + tbl_map);
        
        Collection<AbstractExpression> exps = CollectionUtils.union(PlanNodeUtil.getExpressionsForPlanNode(node),
                                                                    PlanNodeUtil.getOutputExpressionsForPlanNode(node));
        assert(exps.size() > 0) : "No Expressions: " + node;
        for (AbstractExpression exp : exps) {
            new ExpressionTreeWalker() {
                @Override
                protected void callback(AbstractExpression exp_element) {
                    if (exp_element instanceof TupleValueExpression) {
                         System.out.println("element column: " +
                         ((TupleValueExpression)exp_element).getColumnAlias()
                         + " element index: " +
                         ((TupleValueExpression)exp_element).getColumnIndex());
                        checkColumnIndex((TupleValueExpression) exp_element, tbl_map);
                    }
                }
            }.traverse(exp);
        }
    }

    /** Updates intermediate table for column offsets **/
    public void updateIntermediateTblOffset(AbstractPlanNode node, final Map<String, Integer> intermediate_offset_tbl) {
        int offset_cnt = 0;
        intermediate_offset_tbl.clear();
        for (Integer col_guid : node.getOutputColumnGUIDs()) {
            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
            // TupleValueExpression tv_expr =
            // (TupleValueExpression)plan_col.getExpression();
            intermediate_offset_tbl.put(plan_col.getDisplayName(), offset_cnt);
            offset_cnt++;
        }
    }

    /** Updates intermediate table for column GUIDs **/
    public void updateIntermediateTblGUIDs(AbstractPlanNode node, final Map<String, Integer> intermediate_GUID_tbl) {
//        int offset_cnt = 0;
        intermediate_GUID_tbl.clear();
        for (Integer col_guid : node.getOutputColumnGUIDs()) {
            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
            // TupleValueExpression tv_expr =
            // (TupleValueExpression)plan_col.getExpression();
            intermediate_GUID_tbl.put(plan_col.getDisplayName(), plan_col.guid());
        }
    }

    public void checkTableOffsets(AbstractPlanNode node, final Map<String, Integer> tbl_map) {
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
            for (Integer col_guid : ((AggregatePlanNode) node).getGroupByColumnOffsets()) {
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
            for (Integer col_guid : node.getOutputColumnGUIDs()) {
                PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                new ExpressionTreeWalker() {
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
    public void checkMatchInline(AbstractPlanNode node, AbstractPlanNode inline_node) {
        for (int i = 0; i < node.getOutputColumnGUIDs().size(); i++) {
            Integer guid0 = node.getOutputColumnGUIDs().get(i);
            assertNotNull(guid0);
            Integer guid1 = inline_node.getOutputColumnGUIDs().get(i);
            assertNotNull(guid1);
            assertEquals(node + " Node guid doesn't match inline plan guid", guid0, guid1); 
        }
    }

    /**
     * walk through the output columns of the current plan_node and compare the
     * GUIDs of the output columns of the child of the current plan_node
     **/
    public void checkNodeColumnGUIDs(AbstractPlanNode plan_node, Map<String, Integer> intermediate_GUID_tbl) {
        for (Integer orig_guid : plan_node.getOutputColumnGUIDs()) {
            PlanColumn orig_pc = PlannerContext.singleton().get(orig_guid);
            assertNotNull(orig_pc);
            
            // XXX boolean found = false;
            for (String int_name : intermediate_GUID_tbl.keySet()) {
                Integer new_guid = intermediate_GUID_tbl.get(int_name); 
                if (orig_pc.getDisplayName().equals(int_name)) {
                    assertEquals(plan_node + " Column name: " + int_name + " guid id: " + orig_guid + " doesn't match guid from child: ",
                                 orig_guid, new_guid);
                }
            }
        }
    }

    /** walk the tree starting at the given root and validate **/
    public void validateNodeColumnOffsets(AbstractPlanNode node) {
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
                        AbstractScanPlanNode scan_node = (AbstractScanPlanNode) CollectionUtil.first(element.getInlinePlanNodes().values());
                        // get all columns of the "target table" being scanned
                        // and append them to the current intermediate table
                        // + determine new offsets + determine new guids
                        Map<String, Integer> scan_node_map = buildTableMap(scan_node);
                        Integer intermediate_tbl_offset = intermediate_offset_tbl.size();
                        for (Map.Entry<String, Integer> col : scan_node_map.entrySet()) {
                            intermediate_offset_tbl.put(col.getKey(), intermediate_tbl_offset + col.getValue());
                        }
                        for (Integer col_guid : scan_node.getOutputColumnGUIDs()) {
                            PlanColumn plan_col = PlannerContext.singleton().get(col_guid);
                            intermediate_GUID_tbl.put(plan_col.getDisplayName(), plan_col.guid());
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
                            int_order_node_guid = intermediate_GUID_tbl.get(order_node_pc.getDisplayName());
                            assert (int_order_node_guid != -1) : order_node_pc.getDisplayName() + " doesn't exist in intermediate table";
                            assert (order_node_guid == int_order_node_guid) : order_node_pc.getDisplayName() + " sort column guid: " + order_node_guid + " doesn't match: " + int_order_node_guid;
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

    public static void validate(final AbstractPlanNode node) throws Exception {
            
//            System.err.println("Validating: " + node + " / " + node.getPlanNodeType());
            
            switch (node.getPlanNodeType()) {
                // Make sure that the output columns from this node match the output
                // columns of the scan node below us that is feeding into it 
    //            case NESTLOOPINDEX: {
    //                assert(root.getChildPlanNodeCount() == 1);
    //                AbstractPlanNode child = root.getChild(0);
    //                assert(child != null);
    //                if ((child instanceof SeqScanPlanNode) == false) break;
    //                
    //                PlannerContext plannerContext = PlannerContext.singleton();
    //                for (int i = 0, cnt = child.getOutputColumnGUIDCount(); i < cnt; i++) {
    //                    int child_guid  = child.getOutputColumnGUID(i);
    //                    PlanColumn child_col = plannerContext.get(child_guid);
    //                    assert(child_col != null);
    //                    
    //                    int root_guid = root.getOutputColumnGUID(i);
    //                    PlanColumn root_col = plannerContext.get(root_guid);
    //                    assert(root_col != null);
    //                    
    //                    if (child_guid != root_guid) {
    //                        throw new Exception(String.format("Output Column mismatch at position %d : %s != %s",
    //                                                          i, child_col, root_col));
    //                    }
    //                } // FOR
    //                break;
    //            }
                case DISTINCT: {
                    // Make sure the DISTINCT column is in the output columns
                    DistinctPlanNode cast_node = (DistinctPlanNode)node;
                    Integer distinct_col = cast_node.getDistinctColumnGuid();
                    assertTrue(String.format("%s is missing DISTINCT PlanColumn GUID %d in its output columns", cast_node, distinct_col),
                              cast_node.getOutputColumnGUIDs().contains(distinct_col));
                    
                    break;
                }
                case HASHAGGREGATE:
                case AGGREGATE: {
                    // Every PlanColumn referenced in this node must appear in its children's output
                    Collection<Integer> planCols = node.getOutputColumnGUIDs();
                    assert(planCols != null);
                    
//                    System.err.println(PlanNodeUtil.debugNode(node));
                    AggregatePlanNode cast_node = (AggregatePlanNode)node; 
                    assertEquals(cast_node.toString(), cast_node.getAggregateTypes().size(), cast_node.getAggregateColumnGuids().size());
                    assertEquals(cast_node.toString(), cast_node.getAggregateTypes().size(), cast_node.getAggregateColumnNames().size());
                    assertEquals(cast_node.toString(), cast_node.getAggregateTypes().size(), cast_node.getAggregateOutputColumns().size());
                    
                    
//                    Set<Integer> foundCols = new HashSet<Integer>();
//                    for (AbstractPlanNode child : node.getChildren()) {
//                        Collection<Integer> childCols = PlanNodeUtil.getOutputColumnIdsForPlanNode(child);
//                        System.err.println("CHILD " + child + " OUTPUT: " + childCols);
//                        
//                        for (Integer childCol : childCols) {
//                            if (planCols.contains(childCol)) {
//                                foundCols.add(childCol);
//                            }
//                        } // FOR
//                        if (foundCols.size() == planCols.size()) break;
//                    } // FOR
//                    
//                    if (PlanNodeUtil.getPlanNodes(node, SeqScanPlanNode.class).isEmpty() && // HACK
//                        foundCols.containsAll(planCols) == false) {
//                        throw new Exception(String.format("Failed to find all of the columns referenced by %s in the output columns of %s",
//                                                          node, planCols));
//                    }
                    break;
                }
                
                
            } // SWITCH
            
            for (AbstractPlanNode child : node.getChildren()) {
                BasePlanOptimizerTestCase.validate(child);
            }
            return;
        }
    
}
