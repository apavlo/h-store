package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.planner.PlanColumn;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SendPlanNode;

import edu.brown.catalog.CatalogUtil;
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
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    
    public PushdownProjectionOptimization(PlanOptimizerState state) {
        super(state);
    }
    
    @Override
    public AbstractPlanNode optimize(AbstractPlanNode root) {
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // ---------------------------------------------------
                // ADD INLINE PROJECTION TO LEAF SCANS
                // ---------------------------------------------------
                if (element.getChildPlanNodeCount() == 0 && element instanceof AbstractScanPlanNode) {
                    if (addInlineProjection((AbstractScanPlanNode) element) == false) {
                        this.stop();
                        return;
                    }
                }
                // ---------------------------------------------------
                // Distributed NestLoopIndexPlanNode
                // This is where the NestLoopIndexPlanNode immediately passes its
                // intermediate results to a SendPlanNode 
                // ---------------------------------------------------
                else if (element instanceof NestLoopIndexPlanNode && element.getParent(0) instanceof SendPlanNode) {
                    assert(state.join_node_index.size() == state.join_tbl_mapping.size()) :
                        "Join data structures don't have the same size!!!";
                    assert(state.join_tbl_mapping.get(element.getPlanNodeId()) != null) :
                        "Element : " + element.getPlanNodeId() + " does NOT exist in join map!!!";
                    
                    // This will contain all the PlanColumns that are needed to perform the join
                    final Set<PlanColumn> join_columns = new HashSet<PlanColumn>();
                    
                    boolean ret = extractJoinColumns(element, join_columns);
                    assert(ret);
                    
                    ProjectionPlanNode projectionNode = new ProjectionPlanNode(state.plannerContext, PlanAssembler.getNextPlanNodeId());
                    projectionNode.getOutputColumnGUIDs().clear(); // Is this necessary?

                    AbstractExpression orig_col_exp = null;
                    int orig_guid = -1;
                    for (PlanColumn plan_col : join_columns) {
                        boolean exists = false;
                        for (Integer guid : element.getOutputColumnGUIDs()) {
                            PlanColumn output_plan_column = state.plannerContext.get(guid);
                            if (plan_col.equals(output_plan_column, true, true)) {
                                orig_col_exp = output_plan_column.getExpression();
                                orig_guid = guid;
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
//                            if (debug.get()) 
                                LOG.warn(String.format("Failed to find PlanColumn %s in %s output columns?", plan_col, element));
                        } else {
                            assert (orig_col_exp != null);
                            AbstractExpression new_col_exp = null;
                            try {
                                new_col_exp = (AbstractExpression) orig_col_exp.clone();
                            } catch (CloneNotSupportedException ex) {
                                throw new RuntimeException(ex);
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
                    
                    // Add a projection above the current NestLoopIndexPlanNode
                    // Note that we can't make it inline because the NestLoopIndexPlanNode's executor
                    // doesn't support it
                    AbstractPlanNode parent = element.getParent(0);
                    assert(parent != null);
                    element.clearParents();
                    parent.clearChildren();
                    projectionNode.addAndLinkChild(element);
                    parent.addAndLinkChild(projectionNode);

                    // Add it to the global list of ProjectionPlanNodes in this plan tree
                    // and mark the new ProjectionPlanNode as dirty
                    state.projection_plan_nodes.add(projectionNode);
                    state.markDirty(projectionNode);
                }
            }
        }.traverse(root);
        return (root);
    }
    
    private boolean extractJoinColumns(final AbstractPlanNode node, final Set<PlanColumn> join_columns) {
        // Adds a projection column as a parent of the the current join node.
        // Sticks it between the send and join
        final boolean top_join = (state.join_node_index.get(state.join_node_index.firstKey()) == node);
        
        final Set<String> current_tbls_in_join = state.join_tbl_mapping.get(node.getPlanNodeId());
        
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode inner_element) {
                // We are only interested in projections and index scans
                if (inner_element instanceof ProjectionPlanNode || inner_element instanceof IndexScanPlanNode || inner_element instanceof AggregatePlanNode) {
                    Set<Column> col_set = state.planNodeColumns.get(inner_element);
                    assert (col_set != null) : "Null column set for inner element: " + inner_element;
                    
                    // Check whether any output columns have operator, aggregator and project those columns now!
                    // iterate through columns and build the projection columns
                    if (top_join) {
                        for (Integer output_guid : inner_element.getOutputColumnGUIDs()) {
                            PlanColumn plan_col = state.plannerContext.get(output_guid);
                            for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                                TupleValueExpression tv_exp = (TupleValueExpression) exp;
                                if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                    addProjectionColumn(join_columns, output_guid);
                                }
                            } // FOR
                        } // FOR
                    }
                    else {
                        for (Column col : col_set) {
                            Integer col_guid = CollectionUtil.first(state.column_guid_xref.get(col));
                            PlanColumn plan_col = state.plannerContext.get(col_guid);
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
                        PlanColumn plan_col = state.plannerContext.get(col_guid);
                        for (AbstractExpression exp : ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class)) {
                            TupleValueExpression tv_exp = (TupleValueExpression) exp;
                            if (current_tbls_in_join.contains(tv_exp.getTableName())) {
                                addProjectionColumn(join_columns, col_guid);
                            }
                        }
                    }                                                                                   
                }
            }
        }.traverse(node);
        return (true);
    }
    
    /**
     * @param scan_node
     * @return
     */
    private boolean addInlineProjection(final AbstractScanPlanNode scan_node) {
        Collection<Table> tables = CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, scan_node);
        if (tables.size() != 1) {
            LOG.error(PlanNodeUtil.debugNode(scan_node));
        }
        assert (tables.size() == 1) : scan_node + ": " + tables;
        Table catalog_tbl = CollectionUtil.first(tables);

        Set<Column> output_columns = state.tableColumns.get(catalog_tbl);

        // Stop if there is no column information.
        // XXX: Is this a bad thing?
        if (output_columns == null) {
            if (trace.get())
                LOG.warn("No column information for " + catalog_tbl);
            return (false);
            // Only create the projection if the number of columns we need to
            // output is less
            // then the total number of columns for the table
        } else if (output_columns.size() == catalog_tbl.getColumns().size()) {
            if (trace.get())
                LOG.warn("All columns needed in query. No need for inline projection on " + catalog_tbl);
            return (false);
        }

        // Create new projection and add in all of the columns that our table
        // will ever need
        ProjectionPlanNode proj_node = new ProjectionPlanNode(state.plannerContext, PlanAssembler.getNextPlanNodeId());
        if (debug.get())
            LOG.debug(String.format("Adding inline Projection for %s with %d columns. Original table has %d columns", catalog_tbl.getName(), output_columns.size(), catalog_tbl.getColumns().size()));
//        int idx = 0;
        for (Column catalog_col : output_columns) {
            // Get the old GUID from the original output columns
            int orig_idx = catalog_col.getIndex();
            int orig_guid = scan_node.getOutputColumnGUID(orig_idx);
            PlanColumn orig_col = state.plannerContext.get(orig_guid);
            assert (orig_col != null);
            proj_node.appendOutputColumn(orig_col);

            // Set<Integer> guids = column_guid_xref.get(catalog_col);
            // assert(guids != null && guids.isEmpty() == false) :
            // "No PlanColumn GUID for " +
            // CatalogUtil.getDisplayName(catalog_col);
            // Integer col_guid = CollectionUtil.getFirst(guids);
            //            
            //
            // // Always try make a new PlanColumn and update the
            // TupleValueExpresion index
            // // This ensures that we always get the ordering correct
            // TupleValueExpression clone_exp =
            // (TupleValueExpression)orig_col.getExpression().clone();
            // clone_exp.setColumnIndex(idx);
            // Storage storage = (catalog_tbl.getIsreplicated() ?
            // Storage.kReplicated : Storage.kPartitioned);
            // PlanColumn new_col = state.m_context.getPlanColumn(clone_exp,
            // orig_col.displayName(), orig_col.getSortOrder(), storage);
            // assert(new_col != null);
            // proj_node.appendOutputColumn(new_col);
            state.addColumnMapping(catalog_col, orig_col.guid());
//            idx++;
        } // FOR
        if (trace.get())
            LOG.trace("New Projection Output Columns:\n" + PlanNodeUtil.debugNode(proj_node));

        // Add projection inline to scan node
        scan_node.addInlinePlanNode(proj_node);
        assert (proj_node.isInline());

        // Then make sure that we update it's output columns to match the inline
        // output
        scan_node.getOutputColumnGUIDs().clear();
        scan_node.getOutputColumnGUIDs().addAll(proj_node.getOutputColumnGUIDs());

        // add element to the "dirty" list
        state.markDirty(scan_node);
        if (trace.get())
            LOG.trace(String.format("Added inline %s with %d columns to leaf node %s", proj_node, proj_node.getOutputColumnGUIDCount(), scan_node));
        return (true);
    }

    private void addProjectionColumn(Set<PlanColumn> proj_columns, Integer col_id) {
        PlanColumn new_column = state.plannerContext.get(col_id);
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
    
}
