package edu.brown.optimizer.optimizations;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections15.set.ListOrderedSet;
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
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

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
                    if (element.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
                        if (debug.get())
                            LOG.debug("SKIP - " + element + " already has an inline ProjectionPlanNode");
                    }
                    else if (addInlineProjection((AbstractScanPlanNode) element) == false) {
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
                    final Set<PlanColumn> join_columns = extractReferencedColumns(element, state.join_tbl_mapping.get(element.getPlanNodeId()));
                    
                    ProjectionPlanNode proj_node = new ProjectionPlanNode(state.plannerContext, PlanAssembler.getNextPlanNodeId());
                    assert(proj_node.getOutputColumnGUIDCount() == 0);
//                    proj_node.getOutputColumnGUIDs().clear(); // Is this necessary?

                    populateProjectionPlanNode(element, proj_node, join_columns);
                    
                    // Add a projection above the current NestLoopIndexPlanNode
                    // Note that we can't make it inline because the NestLoopIndexPlanNode's executor
                    // doesn't support it
                    AbstractPlanNode parent = element.getParent(0);
                    assert(parent != null);
                    element.clearParents();
                    parent.clearChildren();
                    proj_node.addAndLinkChild(element);
                    parent.addAndLinkChild(proj_node);

                    // Add it to the global list of ProjectionPlanNodes in this plan tree
                    // and mark the new ProjectionPlanNode as dirty
                    state.projection_plan_nodes.add(proj_node);
                    state.markDirty(proj_node);
                }
            }
        }.traverse(root);
        return (root);
    }
    
    private void populateProjectionPlanNode(AbstractPlanNode parent, ProjectionPlanNode proj_node, Collection<PlanColumn> output_columns) {
        AbstractExpression orig_col_exp = null;
        int orig_guid = -1;
        for (PlanColumn plan_col : output_columns) {
            boolean exists = false;
            for (Integer guid : parent.getOutputColumnGUIDs()) {
                PlanColumn output_plan_column = state.plannerContext.get(guid);
                if (plan_col.equals(output_plan_column, true, true)) {
                    orig_col_exp = output_plan_column.getExpression();
                    orig_guid = guid;
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                    LOG.warn(String.format("Failed to find PlanColumn %s in %s output columns?", plan_col, parent));
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
                    proj_node.appendOutputColumn(new_plan_col);                                
                }
            }
        }   
    }
    
    /**
     * Extract all the PlanColumns that we are going to need in the query plan tree above
     * the given node.
     * @param node
     * @param tables
     * @return
     */
    private Set<PlanColumn> extractReferencedColumns(final AbstractPlanNode node, final Collection<String> tables) {
        final Set<PlanColumn> ref_columns = new HashSet<PlanColumn>();
        final ListOrderedSet<Integer> col_guids = new ListOrderedSet<Integer>();
        final boolean top_join = (node instanceof NestLoopIndexPlanNode &&
                                  state.join_node_index.get(state.join_node_index.firstKey()) == node);
        
        // Walk up the tree from the current node and figure out what columns that we need from it are
        // referenced. This will tell us how many we can actually project out at this point
        new PlanNodeTreeWalker(true, true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // If this is the same node that we're examining, then we can skip it
                // Otherwise, anything that this guy references but nobody else does
                // will incorrectly get included in the projection
                if (element == node) return;
                
                int ctr = 0;
                // ---------------------------------------------------
                // ProjectionPlanNode
                // AbstractScanPlanNode
                // AggregatePlanNode
                // ---------------------------------------------------
                if (element instanceof ProjectionPlanNode ||
                    element instanceof AbstractScanPlanNode ||
                    element instanceof AggregatePlanNode) {
                    
                    // Check whether any output columns have operator, aggregator and project those columns now!
                    // iterate through columns and build the projection columns
                    if (top_join) {
                        ctr += element.getOutputColumnGUIDCount();
                        col_guids.addAll(element.getOutputColumnGUIDs());
                    } else {
                        Set<Column> col_set = state.getPlanNodeColumns(element);
                        assert (col_set != null) : "Null column set for " + element;
                        for (Column col : col_set) {
                            col_guids.add(CollectionUtil.first(state.column_guid_xref.get(col)));
                            ctr++;
                        } // FOR
                    }
                }
                // ---------------------------------------------------
                // OrderByPlanNode
                // ---------------------------------------------------
                else if (element instanceof OrderByPlanNode) {
                    ctr += ((OrderByPlanNode)element).getSortColumnGuids().size();
                    col_guids.addAll(((OrderByPlanNode)element).getSortColumnGuids());
                }
                
                if (debug.get() && ctr > 0)
                    LOG.debug(String.format("[%s] Found %d PlanColumns referenced in %s", node, ctr, element));
            }
        }.traverse(node);
        
        // Now extract the TupleValueExpression and get the PlanColumn that we really want
        for (Integer col_guid : col_guids) {
            PlanColumn plan_col = state.plannerContext.get(col_guid);
            Collection<TupleValueExpression> exps = ExpressionUtil.getExpressions(plan_col.getExpression(), TupleValueExpression.class);
            assert(exps != null);
            for (TupleValueExpression tv_exp : exps) {
                if (tables.contains(tv_exp.getTableName())) {
                    addProjectionColumn(ref_columns, col_guid);
                }
            } // FOR
        } // FOR
        
        return (ref_columns);
    }
    
    /**
     * @param scan_node
     * @return
     */
    private boolean addInlineProjection(final AbstractScanPlanNode scan_node) {
        Collection<Table> tables = CatalogUtil.getReferencedTablesForPlanNode(state.catalog_db, scan_node);
        if (tables.size() != 1) LOG.error(PlanNodeUtil.debugNode(scan_node));
        assert (tables.size() == 1) : scan_node + ": " + tables;
        Table catalog_tbl = CollectionUtil.first(tables);

        // Figure out what is the bare minimum number set of columns that we're going
        // to need above us...
        Set<PlanColumn> output_columns = extractReferencedColumns(scan_node, Collections.singleton(catalog_tbl.getName()));

        // Stop if there is no column information.
        // XXX: Is this a bad thing?
        if (output_columns == null) {
            LOG.warn("No column information for " + catalog_tbl);
            return (false);
        }
        // Only create the projection if the number of columns we need to
        // output is less then the total number of columns for the table
        else if (output_columns.size() == catalog_tbl.getColumns().size()) {
            if (debug.get())
                LOG.debug("SKIP - All columns needed in query. No need for inline projection on " + catalog_tbl);
            return (false);
        }

        // Create new projection and add in all of the columns we will ever need in the PlanNodes above our ScanNode
        ProjectionPlanNode proj_node = new ProjectionPlanNode(state.plannerContext, PlanAssembler.getNextPlanNodeId());
        if (debug.get())
            LOG.debug(String.format("Adding inline Projection for %s with %d columns. Original table has %d columns",
                                    catalog_tbl.getName(), output_columns.size(), catalog_tbl.getColumns().size()));
        populateProjectionPlanNode(scan_node, proj_node, output_columns);
        
//        for (Column catalog_col : output_columns) {
//            // Get the old GUID from the original output columns
//            int orig_idx = catalog_col.getIndex();
//            int orig_guid = scan_node.getOutputColumnGUID(orig_idx);
//            PlanColumn orig_col = state.plannerContext.get(orig_guid);
//            assert (orig_col != null);
//            proj_node.appendOutputColumn(orig_col);
//            state.addColumnMapping(catalog_col, orig_col.guid());
//        } // FOR
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
