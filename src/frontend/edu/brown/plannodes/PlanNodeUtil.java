package edu.brown.plannodes;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;

import org.voltdb.expressions.*;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.*;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.utils.ClassUtil;

/**
 * Utility methods for extracting information from AbstractPlanNode trees/nodes
 * @author pavlo
 */
public abstract class PlanNodeUtil {
    private static final Logger LOG = Logger.getLogger(PlanNodeUtil.class);

    private static final String INLINE_SPACER_PREFIX = "\u2502";
    private static final String INLINE_INNER_PREFIX = "\u251C";

    private static final String NODE_PREFIX = "\u25B6 ";
    private static final String SPACER_PREFIX = "\u2503";
    private static final String INNER_PREFIX = "\u2523";    

    /**
     * Returns the root node in the tree for the given node
     * @param node
     * @return
     */
    public static AbstractPlanNode getRoot(AbstractPlanNode node) {
        return (node.getParentCount() > 0 ? getRoot(node.getParent(0)) : node);
    }
    
    /**
     * Return a set of all the PlanNodeTypes in the tree
     * @param node
     * @return
     */
    public static Set<PlanNodeType> getPlanNodeTypes(AbstractPlanNode node) {
        final Set<PlanNodeType> types = new HashSet<PlanNodeType>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                types.add(element.getPlanNodeType());
            }
        }.traverse(node);
        assert(types.size() > 0);
        return (types);
    }
    
    /**
     * Get all the AbstractExpression roots used in the given AbstractPlanNode. Non-recursive
     * @param node
     * @return
     */
    public static Set<AbstractExpression> getExpressions(AbstractPlanNode node) {
        final Set<AbstractExpression> exps = new HashSet<AbstractExpression>();

        // ---------------------------------------------------
        // SCAN NODES
        // ---------------------------------------------------
        if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode scan_node = (AbstractScanPlanNode)node;
            
            // Main Predicate Expression
            if (scan_node.getPredicate() != null) exps.add(scan_node.getPredicate());
            
            if (scan_node instanceof IndexScanPlanNode) {
                IndexScanPlanNode idx_node = (IndexScanPlanNode)scan_node;
                
                // End Expression
                if (idx_node.getEndExpression() != null) exps.add(idx_node.getEndExpression());
                
                // Search Key Expressions
                if (idx_node.getSearchKeyExpressions().isEmpty() == false) exps.addAll(idx_node.getSearchKeyExpressions());
            }

        // ---------------------------------------------------
        // JOINS
        // ---------------------------------------------------
        } else if (node instanceof AbstractJoinPlanNode) {
            AbstractJoinPlanNode join_node = (AbstractJoinPlanNode)node;
            
            // Get all the AbstractExpressions used in the predicates for this join
            if (join_node.getPredicate() != null) exps.add(join_node.getPredicate());
        }

        // ---------------------------------------------------
        // AGGREGATE
        // ---------------------------------------------------
        else if (node instanceof AggregatePlanNode) {
            AggregatePlanNode agg_node = (AggregatePlanNode)node;
            for (Integer col_guid: agg_node.getAggregateColumnGuids()) {
                PlanColumn col = PlannerContext.singleton().get(col_guid);
                assert(col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null) exps.add(col.getExpression());
            } // FOR
            for (Integer col_guid : agg_node.getGroupByColumnIds()) {
                PlanColumn col = PlannerContext.singleton().get(col_guid);
                assert(col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null) exps.add(col.getExpression());
            } // FOR
        // ---------------------------------------------------
        // ORDER BY
        // ---------------------------------------------------
        } else if (node instanceof OrderByPlanNode) {
            OrderByPlanNode orby_node = (OrderByPlanNode)node;
            for (Integer col_guid : orby_node.getSortColumnGuids()) {
                PlanColumn col = PlannerContext.singleton().get(col_guid);
                assert(col != null) : "Invalid PlanColumn #" + col_guid;
                if (col.getExpression() != null) exps.add(col.getExpression());
            } // FOR
        }
        
        return (exps);
    }
    
    /**
     * Return all the ExpressionTypes used for scan predicates in the given PlanNode
     * @param catalog_db
     * @param node
     * @return
     */
    public static Set<ExpressionType> getScanExpressionTypes(final Database catalog_db, AbstractPlanNode root) {
        final Set<ExpressionType> found = new HashSet<ExpressionType>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode node) {
                Set<AbstractExpression> exps = new HashSet<AbstractExpression>();
                switch (node.getPlanNodeType()) {
                    // SCANS
                    case INDEXSCAN: {
                        IndexScanPlanNode idx_node = (IndexScanPlanNode)node;
                        exps.add(idx_node.getEndExpression());
                        exps.addAll(idx_node.getSearchKeyExpressions());
                    }
                    case SEQSCAN: {
                        AbstractScanPlanNode scan_node = (AbstractScanPlanNode)node;
                        exps.add(scan_node.getPredicate());
                        break;
                    }
                    // JOINS
                    case NESTLOOP:
                    case NESTLOOPINDEX: {
                        AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode)node;
                        exps.add(cast_node.getPredicate());
                        break;
                    }
                    default:
                        // Do nothing...
                } // SWITCH
                
                for (AbstractExpression exp : exps) {
                    if (exp == null) continue;
                    found.addAll(ExpressionUtil.getExpressionTypes(exp));
                } // FOR
                return;
            }
        }.traverse(root);
        return (found);
    }
    
    /**
     * Get the set of columns 
     * @param catalog_db
     * @param node
     * @return
     */
    public static Set<Column> getOutputColumns(final Database catalog_db, AbstractPlanNode node) {
        Set<Column> columns = new ListOrderedSet<Column>();
        for (int ctr = 0, cnt = node.m_outputColumns.size(); ctr < cnt; ctr++) {
            int column_guid = node.m_outputColumns.get(ctr);
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            assert(column != null);
            
            final String column_name = column.displayName();
            String table_name = column.originTableName();
            
            // If there is no table name, then check whether this is a scan node.
            // If it is, then we can try to get the table name from the node's target
            if (table_name == null && node instanceof AbstractScanPlanNode) {
                table_name = ((AbstractScanPlanNode)node).getTargetTableName();
            }
            
            // If this is a TupleAddressExpression or there is no target table name, then we have
            // to skip this output column
            if (column_name.equalsIgnoreCase("tuple_address") || table_name == null) continue;
            
            Table catalog_tbl = null; 
            try {
                catalog_tbl = catalog_db.getTables().get(table_name);
            } catch (Exception ex) {
                ex.printStackTrace();
                LOG.fatal("table_name: " + table_name);
                LOG.fatal(CatalogUtil.debug(catalog_db.getTables()));
                System.exit(1);
            }
            assert(catalog_tbl != null) : "Invalid table '" + table_name + "'";
            
            Column catalog_col = catalog_tbl.getColumns().get(column_name);
            assert(catalog_col != null) : "Invalid column '" + table_name + "." + column_name;

            columns.add(catalog_col);
        } // FOR
        return (columns);
    }
    
    /**
     * Returns all the PlanNodes in the given tree that of a specific type
     * @param root
     * @param search_class
     * @return
     */
    public static <T extends AbstractPlanNode> Set<T> getPlanNodes(AbstractPlanNode root, final Class<? extends T> search_class) {
        final Set<T> found = new HashSet<T>();
        new PlanNodeTreeWalker() {
            @SuppressWarnings("unchecked")
            @Override
            protected void callback(AbstractPlanNode element) {
                Class<? extends AbstractPlanNode> element_class = element.getClass();
                if (ClassUtil.getSuperClasses(element_class).contains(search_class)) {
                    found.add((T)element);
                }
                return;
            }
        }.traverse(root);
        return (found);
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends AbstractPlanNode> Set<T> getChildren(AbstractPlanNode node, Class<T> search_class) {
        final Set<T> found = new HashSet<T>();
        for (int i = 0, cnt = node.getChildCount(); i < cnt; i++) {
            AbstractPlanNode child = node.getChild(i);
            Class<? extends AbstractPlanNode> child_class = child.getClass();
            if (ClassUtil.getSuperClasses(child_class).contains(search_class)) {
                found.add((T)child);
            }
        } // FOR
        return (found);
    }
    
    /**
     * Get the total depth of the tree
     * @param root
     * @return
     */
    public static int getDepth(AbstractPlanNode root) {
        final AtomicInteger depth = new AtomicInteger(0);
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                int current_depth = this.getDepth();
                if (current_depth > depth.intValue()) depth.set(current_depth);
            }
        }.traverse(root);
        return (depth.intValue());
    }
    
    /**
     * Return all the nodes in an tree that reference a particular table
     * This can be either scan nodes or operation nodes
     * @param root
     * @param catalog_tbl
     * @return
     */
    public static Set<AbstractPlanNode> getNodesReferencingTable(AbstractPlanNode root, final Table catalog_tbl) {
        final Set<AbstractPlanNode> found = new HashSet<AbstractPlanNode>();
        new PlanNodeTreeWalker() {
            /**
             * Visit the inline nodes after the parent
             */
            @Override
            protected void populate_children(PlanNodeTreeWalker.Children<AbstractPlanNode> children, AbstractPlanNode node) {
                super.populate_children(children, node);
                for (AbstractPlanNode inline_node : node.getInlinePlanNodes().values()) {
                    children.addAfter(inline_node);
                }
                return;
            }
            
            @Override
            protected void callback(AbstractPlanNode element) {
                //
                // AbstractScanNode
                //
                if (element instanceof AbstractScanPlanNode) {
                    AbstractScanPlanNode cast_node = (AbstractScanPlanNode)element;
                    if (cast_node.getTargetTableName().equals(catalog_tbl.getName())) found.add(cast_node);
                //
                // AbstractOperationPlanNode
                //
                } else if (element instanceof AbstractOperationPlanNode) {
                    AbstractOperationPlanNode cast_node = (AbstractOperationPlanNode)element;
                    if (cast_node.getTargetTableName().equals(catalog_tbl.getName())) found.add(cast_node);
                }
                return;
            }
        }.traverse(root);
        return (found);
    }

    /**
     * Return all of the PlanColumn guids used in this query plan tree (including inline nodes)
     * @param root
     * @return
     */
    public static Set<Integer> getAllPlanColumnGuids(AbstractPlanNode root) {
        final Set<Integer> guids = new HashSet<Integer>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                guids.addAll(element.m_outputColumns);
            }
        }.traverse(root);
        return (guids);
    }
    
    public static String debug(AbstractPlanNode node) {
        return (PlanNodeUtil.debug(node, ""));
    }

    /**
     * 
     * @param label
     * @param guids
     * @param spacer
     * @return
     */
    private static String debugOutputColumns(String label, List<Integer> guids, String spacer) {
        String ret = "";
        
        ret += label + "[" + guids.size() + "]:\n";
        for (int ctr = 0, cnt = guids.size(); ctr < cnt; ctr++) {
            int column_guid = guids.get(ctr);
            String name = "???";
            String inner = "";
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            if (column != null) {
                assert(column_guid == column.guid());
                name = column.displayName();
                inner = " : type=" + column.type() +
                        " : size=" + column.width() +
                        " : sort=" + column.getSortOrder() +
                        " : storage=" + column.getStorage();
            }
            inner += " : guid=" + column_guid;
            ret += String.format("%s   [%02d] %s%s\n", spacer, ctr, name, inner);
            
            if (column != null && column.getExpression() != null) { //  && (true || node instanceof ProjectionPlanNode)) {
                ret += ExpressionUtil.debug(column.getExpression(), spacer + "    ");
            }
        } // FOR
        return (ret);
    }
    
    private static String debug(AbstractPlanNode node, String spacer) {
        String ret = debugNode(node, spacer);
        
        // Print out all of our children
        spacer += "  ";
        for (int ctr = 0, cnt = node.getChildCount(); ctr < cnt; ctr++) {
            ret += PlanNodeUtil.debug(node.getChild(ctr), spacer);
        } 

        return (ret);
    }
        
    public static String debugNode(AbstractPlanNode node) {
        return (debugNode(node, ""));
    }
    
    public static String debugNode(AbstractPlanNode node, final String orig_spacer) {
        StringBuilder sb = new StringBuilder();
        
        final String inner_prefix = (node.isInline() ? INLINE_INNER_PREFIX : INNER_PREFIX) + " ";
        final String spacer_prefix = (node.isInline() ? INLINE_SPACER_PREFIX : SPACER_PREFIX) + " ";
//        final String last_prefix = (node.isInline() ? INLINE_LAST_PREFIX : LAST_PREFIX);

        String spacer = orig_spacer + "  ";
        String inner_spacer = spacer + inner_prefix;
        String line_spacer = spacer + spacer_prefix;
        
        // General Information
        if (node.isInline() == false) sb.append(orig_spacer).append(NODE_PREFIX + node.toString() + "\n");
        sb.append(inner_spacer).append("Inline[" + node.isInline() + "]\n");
        
        // AbstractJoinPlanNode
        if (node instanceof AbstractJoinPlanNode) {
            AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode)node;
            sb.append(inner_spacer).append("JoinType[" + cast_node.getJoinType() + "]\n");
            sb.append(inner_spacer).append("Join Expression: " + (cast_node.getPredicate() != null ? "\n" + ExpressionUtil.debug(cast_node.getPredicate(), line_spacer) : null + "\n"));
        
        // AbstractOperationPlanNode
        } else if (node instanceof AbstractOperationPlanNode) {
            sb.append(inner_spacer).append("TargetTableId[" + ((AbstractOperationPlanNode)node).getTargetTableName() + "]\n");
        
        // AbstractScanPlanNode
        } else if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode cast_node = (AbstractScanPlanNode)node;
            sb.append(inner_spacer).append("TargetTableName[" + cast_node.getTargetTableName() + "]\n");
            sb.append(inner_spacer).append("TargetTableAlias[" + cast_node.getTargetTableAlias() + "]\n");
            sb.append(inner_spacer).append("TargetTableId[" + cast_node.getTargetTableName() + "]\n");
        }
        
        // AggregatePlanNode
        if (node instanceof AggregatePlanNode) {
            AggregatePlanNode cast_node = (AggregatePlanNode)node;
            sb.append(inner_spacer).append("AggregateTypes[" + cast_node.getAggregateTypes() + "]\n");
//            sb.append(inner_spacer).append("AggregateColumns" + cast_node.getAggregateOutputColumns() + "\n");
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("AggregateColumns", cast_node.getAggregateColumnGuids(), line_spacer));
//            sb.append(inner_spacer).append("GroupByColumns" + cast_node.getGroupByColumns() + "\n");
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("GroupByColumns", cast_node.getGroupByColumnIds(), line_spacer));

            
        // DeletePlanNode
        } else if (node instanceof DeletePlanNode) {
            sb.append(inner_spacer).append("Truncate[" + ((DeletePlanNode)node).isTruncate() + "\n");
            
        // DistinctPlanNode
        } else if (node instanceof DistinctPlanNode) {
            DistinctPlanNode dist_node = (DistinctPlanNode)node;
            PlanColumn col = PlannerContext.singleton().get(dist_node.getDistinctColumnGuid());
            sb.append(inner_spacer).append("DistinctColumn[" + col + "]\n");
            
        // IndexScanPlanNode
        } else if (node instanceof IndexScanPlanNode) {
            IndexScanPlanNode cast_node = (IndexScanPlanNode)node;
            sb.append(inner_spacer).append("TargetIndexName[" + cast_node.getTargetIndexName() + "]\n");
            sb.append(inner_spacer).append("EnableKeyIteration[" + cast_node.getKeyIterate() + "]\n");
            sb.append(inner_spacer).append("IndexLookupType[" + cast_node.getLookupType() + "]\n");
            sb.append(inner_spacer).append("SearchKey Expressions:\n");
            for (AbstractExpression search_key : cast_node.getSearchKeyExpressions()) {
                sb.append(ExpressionUtil.debug(search_key, line_spacer));
            } 
            sb.append(inner_spacer).append("End Expression: " + (cast_node.getEndExpression() != null ? "\n" + ExpressionUtil.debug(cast_node.getEndExpression(), line_spacer) : null + "\n"));
            sb.append(inner_spacer).append("Post-Scan Expression: " + (cast_node.getPredicate() != null ? "\n" + ExpressionUtil.debug(cast_node.getPredicate(), line_spacer) : null + "\n"));
            
        // InsertPlanNode
        } else if (node instanceof InsertPlanNode) {
            sb.append(inner_spacer).append("MultiPartition[" + ((InsertPlanNode)node).getMultiPartition() + "]\n");
            
        // LimitPlanNode
        } else if (node instanceof LimitPlanNode) {
            sb.append(inner_spacer).append("Limit[" + ((LimitPlanNode)node).getLimit() + "]\n");
            sb.append(inner_spacer).append("Offset[" + ((LimitPlanNode)node).getOffset() + "]\n");
            
        // NestLoopIndexPlanNode
        } else if (node instanceof NestLoopIndexPlanNode) {
            // Nothing
            
        // NestLoopPlanNode
        } else if (node instanceof NestLoopPlanNode) {
            // Nothing
            
        } else if (node instanceof OrderByPlanNode) {
            OrderByPlanNode cast_node = (OrderByPlanNode)node;
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("SortColumns", cast_node.getSortColumnGuids(), line_spacer));
            
        } else if (node instanceof ProjectionPlanNode) {
//            ProjectionPlanNode cast_node = (ProjectionPlanNode)node;
            if (node instanceof MaterializePlanNode) {
                sb.append(line_spacer).append("Batched[" + ((MaterializePlanNode)node).isBatched() + "]\n");
            }
            
        } else if (node instanceof ReceivePlanNode) {
            // Nothing
            
        } else if (node instanceof SendPlanNode) {
            sb.append(inner_spacer).append("Fake[" + ((SendPlanNode)node).getFake() + "]\n");
            
        } else if (node instanceof SeqScanPlanNode) {
            sb.append(inner_spacer).append("Scan Expression: " + (((SeqScanPlanNode)node).getPredicate() != null ? "\n" + ExpressionUtil.debug(((SeqScanPlanNode)node).getPredicate(), line_spacer) : null + "\n"));
            
        } else if (node instanceof UnionPlanNode) {
            // Nothing
        } else if (node instanceof UpdatePlanNode) {
            sb.append(inner_spacer).append("UpdateIndexes[" + ((UpdatePlanNode)node).doesUpdateIndexes() + "]\n");
        } else {
            LOG.fatal("Unsupported PlanNode type: " + node.getClass().getSimpleName());
            System.exit(1);
        }
        
        // Output Columns
//        if (false && node.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
//            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("OutputColumns (Inline Projection)", node.getInlinePlanNode(PlanNodeType.PROJECTION), line_spacer));
//        } else {
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("OutputColumns", node.m_outputColumns, line_spacer));
//        }
        
        // Inline PlanNodes
        if (!node.getInlinePlanNodes().isEmpty()) {
            for (AbstractPlanNode inline_node : node.getInlinePlanNodes().values()) {
                sb.append(inner_spacer).append("Inline " + inline_node + ":\n");
                sb.append(PlanNodeUtil.debug(inline_node, line_spacer));
            } 
        }
        return (sb.toString());
    }
}
