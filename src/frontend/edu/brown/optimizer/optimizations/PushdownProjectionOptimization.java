package edu.brown.optimizer.optimizations;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.planner.PlanColumn;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SendPlanNode;

import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.PlanOptimizerState;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * 
 * @author pavlo
 */
public class PushdownProjectionOptimization extends AbstractOptimization {
    private static final Logger LOG = Logger.getLogger(PushdownProjectionOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    
    public PushdownProjectionOptimization(PlanOptimizerState state) {
        super(state);
    }
    
    private void addProjectionColumn(Set<PlanColumn> proj_columns, Integer col_id) {
        PlanColumn new_column = state.m_context.get(col_id);
        boolean exists = false;
        for (PlanColumn plan_col : proj_columns) {
            if (new_column.getDisplayName().equals(plan_col.getDisplayName())) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            proj_columns.add(new_column);
        }
    }
    
//    private void addProjectionColumn(Set<PlanColumn> proj_columns, Integer col_id, int offset) throws CloneNotSupportedException {
//        PlanColumn orig_pc = state.m_context.get(col_id);
//        assert (orig_pc.getExpression() instanceof TupleValueExpression);
//        TupleValueExpression orig_tv_exp = (TupleValueExpression)orig_pc.getExpression();
//        TupleValueExpression clone_tv_exp = (TupleValueExpression)orig_tv_exp.clone();
//        clone_tv_exp.setColumnIndex(offset);
//        PlanColumn new_col = state.m_context.getPlanColumn(clone_tv_exp, orig_pc.getDisplayName(), orig_pc.getSortOrder(), orig_pc.getStorage());
//        boolean exists = false;
//        for (PlanColumn plan_col : proj_columns) {
//            if (new_col.getDisplayName().equals(plan_col.getDisplayName())) {
//                exists = true;
//                break;
//            }
//        }
//        if (!exists) {
//            proj_columns.add(new_col);
//        }
//    }
    
    @Override
    public AbstractPlanNode optimize(AbstractPlanNode root) {
        
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element instanceof NestLoopIndexPlanNode && element.getParent(0) instanceof SendPlanNode) {
                    assert(state.join_node_index.size() == state.join_tbl_mapping.size()) :
                        "Join data structures don't have the same size!!!";
                    assert(state.join_tbl_mapping.get(element.getPlanNodeId()) != null) :
                        "Element : " + element.getPlanNodeId() + " does NOT exist in join map!!!";
                    
                    final Set<String> current_tbls_in_join = state.join_tbl_mapping.get(element.getPlanNodeId());
                    final Set<PlanColumn> join_columns = new HashSet<PlanColumn>();
                    
                    // traverse the tree from bottom up from the current nestloop index
                    final int outer_depth = this.getDepth();
//                    final SortedMap<Integer, PlanColumn> proj_column_order = new TreeMap<Integer, PlanColumn>();
                    /**
                     * Adds a projection column as a parent of the the current join node.
                     * (Sticks it between the send and join)
                     **/
                    final boolean top_join = (state.join_node_index.get(state.join_node_index.firstKey()) == element);
                    new PlanNodeTreeWalker(true) {
                        @Override
                        protected void callback(AbstractPlanNode inner_element) {
                            int inner_depth = this.getDepth();
                            if (inner_depth < outer_depth) {
                                // only interested in projections and index scans
                                if (inner_element instanceof ProjectionPlanNode || inner_element instanceof IndexScanPlanNode || inner_element instanceof AggregatePlanNode) {
                                    Set<Column> col_set = state.planNodeColumns.get(inner_element);
                                    assert (col_set != null) : "Null column set for inner element: " + inner_element;
                                    //Map<String, Integer> current_join_output = new HashMap<String, Integer>();
                                    // Check whether any output columns have operator, aggregator and project
                                    // those columns now!
                                    // iterate through columns and build the projection columns
                                    if (top_join) {
                                        for (Integer output_guid : inner_element.getOutputColumnGUIDs()) {
                                            PlanColumn plan_col = state.m_context.get(output_guid);
                                            for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                                TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                                if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                                    addProjectionColumn(join_columns, output_guid);
                                                }
                                            }
                                        }
                                    }
                                    else {
                                        for (Column col : col_set) {
                                            Integer col_guid = CollectionUtil.first(state.column_guid_xref.get(col));
                                            PlanColumn plan_col = state.m_context.get(col_guid);
                                            for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                                TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                                if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                                    addProjectionColumn(join_columns, col_guid);
                                                }
                                            }
                                        }                                            
                                    }
                                } else if (inner_element instanceof AggregatePlanNode) {
                                    Set<Column> col_set = state.planNodeColumns.get(inner_element);
                                    for (Column col : col_set) {
                                        Integer col_guid = CollectionUtil.first(state.column_guid_xref.get(col));
                                        PlanColumn plan_col = state.m_context.get(col_guid);
                                        for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                            TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                            if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                                addProjectionColumn(join_columns, col_guid);
                                            }
                                        }
                                    }                                                                                   
//                                    }
                                }
                            }
                        }
                    }.traverse(PlanNodeUtil.getRoot(element));
                    /** END OF "CONSTRUCTING" THE PROJECTION PLAN NODE **/
                    
                    // Add a projection above the current nestloopindex plan node
                    AbstractPlanNode temp_parent = element.getParent(0);
                    // clear old parents
                    element.clearParents();
                    temp_parent.clearChildren();
                    ProjectionPlanNode projectionNode = new ProjectionPlanNode(state.m_context, PlanAssembler.getNextPlanNodeId());
                    projectionNode.getOutputColumnGUIDs().clear();

//                    if (join_node_index.get(join_node_index.firstKey()) == element) {
//                        assert (proj_column_order != null);
//                        Iterator<Integer> order_iterator = proj_column_order.keySet().iterator();
//                        while (order_iterator.hasNext()) {
//                            projectionNode.appendOutputColumn(proj_column_order.get(order_iterator.next()));                                        
//                        }
//                    } else {
                    AbstractExpression orig_col_exp = null;
                    int orig_guid = -1;
                    for (PlanColumn plan_col : join_columns) {
                        boolean exists = false;
                        for (Integer guid : element.getOutputColumnGUIDs()) {
                            PlanColumn output_plan_column = state.m_context.get(guid);
                            if (plan_col.equals(output_plan_column, true, true)) {
//                                    PlanColumn orig_plan_col = plan_col;
                                orig_col_exp = output_plan_column.getExpression();
                                orig_guid = guid;
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
                            if (debug.get()) LOG.warn("Trouble plan column name: " + plan_col.getDisplayName());
                        } else {
                            assert (orig_col_exp != null);
                            AbstractExpression new_col_exp = null;
                            try {
                                new_col_exp = (AbstractExpression) orig_col_exp.clone();
                            } catch (CloneNotSupportedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            assert (new_col_exp != null);
                            PlanColumn new_plan_col = null;
                            if (new_col_exp instanceof TupleValueExpression) {
                                new_plan_col = new PlanColumn(orig_guid,
                                                              new_col_exp,
                                                              ((TupleValueExpression)new_col_exp).getColumnName(),
                                                              plan_col.getSortOrder(),
                                                              plan_col.getStorage());                                    
                                projectionNode.appendOutputColumn(new_plan_col);                                
                            }
                        }
                    }                            
//                    }
                    
                    projectionNode.addAndLinkChild(element);
                    temp_parent.addAndLinkChild(projectionNode);
                    // add to list of projection nodes
                    state.projection_plan_nodes.add(projectionNode);
                    // mark projectionNode as dirty
                    state.markDirty(projectionNode);
                    join_columns.clear();
                    //this.stop();
                }
            }
        }.traverse(PlanNodeUtil.getRoot(root));
        return (root);
    }

}
