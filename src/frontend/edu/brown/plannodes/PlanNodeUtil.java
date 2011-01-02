package edu.brown.plannodes;

import java.util.*;

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

public abstract class PlanNodeUtil {
    private static final Logger LOG = Logger.getLogger(PlanNodeUtil.class);
    
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
            protected void populate_children(PlanNodeTreeWalker.Children children, AbstractPlanNode node) {
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
        };
        return (guids);
    }
    
    public static String debug(AbstractPlanNode node) {
        return (PlanNodeUtil.debug(node, ""));
    }
    
    private static String debugOutputColumns(String label, AbstractPlanNode node, String spacer) {
        String ret = "";
        
        ret += spacer + label + "[" + node.m_outputColumns.size() + "]:\n";
        for (int ctr = 0, cnt = node.m_outputColumns.size(); ctr < cnt; ctr++) {
            int column_guid = node.m_outputColumns.get(ctr);
            String name = "???";
            String inner = "";
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            if (column != null) {
                assert(column_guid == column.guid());
                name = column.displayName();
                inner = " : size=" + column.width() +
                        " : type=" + column.type();
            }
            inner += " : guid=" + column_guid;
            ret += String.format("%s   [%d] %s%s\n", spacer, ctr, name, inner);
            
            if (column != null && column.getExpression() != null && (true || node instanceof ProjectionPlanNode)) {
                ret += ExpressionUtil.debug(column.getExpression(), spacer + "   ");
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
    
    public static String debugNode(AbstractPlanNode node, String spacer) {
        String ret = spacer + "* " + node + "\n";
        String info_spacer = spacer + "  |";
        
        //
        // Abstract PlanNode Types
        //
        if (node instanceof AbstractJoinPlanNode) {
            AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode)node;
            ret += spacer + "JoinType[" + cast_node.getJoinType() + "]\n";
            ret += spacer + "Join Expression: " + (cast_node.getPredicate() != null ? "\n" + ExpressionUtil.debug(cast_node.getPredicate(), spacer) : null + "\n");
        } else if (node instanceof AbstractOperationPlanNode) {
            ret += spacer + "TargetTableId[" + ((AbstractOperationPlanNode)node).getTargetTableName() + "]\n";
        } else if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode cast_node = (AbstractScanPlanNode)node;
            ret += spacer + "TargetTableName[" + cast_node.getTargetTableName() + "]\n";
            ret += spacer + "TargetTableAlias[" + cast_node.getTargetTableAlias() + "]\n";
            ret += spacer + "TargetTableId[" + cast_node.getTargetTableName() + "]\n";
            
            // Pull from inline Projection
            if (cast_node.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
                ret += PlanNodeUtil.debugOutputColumns("OutputColumns (Inline Projection)", cast_node.getInlinePlanNode(PlanNodeType.PROJECTION), spacer);
            } else if (cast_node.m_outputColumns.isEmpty()) {
                ret += PlanNodeUtil.debugOutputColumns("OutputColumns", cast_node, spacer);
            }
        }
        
        //
        // PlanNodeTypes
        //
        if (node instanceof AggregatePlanNode) {
            AggregatePlanNode cast_node = (AggregatePlanNode)node;
            ret += spacer + "AggregateTypes[" + cast_node.getAggregateTypes() + "]\n";
            ret += spacer + "AggregateColumns[" + cast_node.getAggregateOutputColumns() + "]\n";
            ret += spacer + "GroupByColumns" + cast_node.getGroupByColumns() + "\n";
            ret += PlanNodeUtil.debugOutputColumns("OutputColumns", cast_node, spacer);
        } else if (node instanceof DeletePlanNode) {
            ret += spacer + "Truncate[" + ((DeletePlanNode)node).isTruncate() + "\n";
        } else if (node instanceof DistinctPlanNode) {
            ret += spacer + "DistinctColumn[" + ((DistinctPlanNode)node).getDistinctColumnName() + "]\n";
        } else if (node instanceof IndexScanPlanNode) {
            IndexScanPlanNode cast_node = (IndexScanPlanNode)node;
            ret += spacer + "TargetIndexName[" + cast_node.getTargetIndexName() + "]\n";
            ret += spacer + "EnableKeyIteration[" + cast_node.getKeyIterate() + "]\n";
            ret += spacer + "IndexLookupType[" + cast_node.getLookupType() + "]\n";
            ret += spacer + "SearchKey Expressions:\n";
            for (AbstractExpression search_key : cast_node.getSearchKeyExpressions()) {
                ret += ExpressionUtil.debug(search_key, spacer);
            } 
            ret += spacer + "End Expression: " + (cast_node.getEndExpression() != null ? "\n" + ExpressionUtil.debug(cast_node.getEndExpression(), spacer) : null + "\n");
            ret += spacer + "Post-Scan Expression: " + (cast_node.getPredicate() != null ? "\n" + ExpressionUtil.debug(cast_node.getPredicate(), spacer) : null + "\n");
            
        } else if (node instanceof InsertPlanNode) {
            ret += spacer + "MultiPartition[" + ((InsertPlanNode)node).getMultiPartition() + "]\n";
            
        } else if (node instanceof LimitPlanNode) {
            ret += spacer + "Limit[" + ((LimitPlanNode)node).getLimit() + "]\n";
            ret += spacer + "Offset[" + ((LimitPlanNode)node).getOffset() + "]\n";
            
        } else if (node instanceof NestLoopIndexPlanNode) {
            // Nothing
            
        } else if (node instanceof NestLoopPlanNode) {
            // Nothing
            
        } else if (node instanceof OrderByPlanNode) {
            OrderByPlanNode cast_node = (OrderByPlanNode)node;
            ret += spacer + "SortColumns[" + cast_node.getSortColumns().size() + "]:\n";
            for (int ctr = 0, cnt = cast_node.getSortColumns().size(); ctr < cnt; ctr++) {
                ret += spacer + "  [" + ctr + "] " + cast_node.getSortColumnNames().get(ctr) + "::" + cast_node.getSortColumns().get(ctr) + "::" + cast_node.getSortDirections().get(ctr) + "\n";
            }
            
        } else if (node instanceof ProjectionPlanNode) {
            ProjectionPlanNode cast_node = (ProjectionPlanNode)node;
            if (node instanceof MaterializePlanNode) {
                ret += spacer + "Batched[" + ((MaterializePlanNode)node).isBatched() + "]\n";
            }
            ret += PlanNodeUtil.debugOutputColumns("ProjectionOutput", cast_node, spacer);
            
        } else if (node instanceof ReceivePlanNode) {
            ReceivePlanNode cast_node = (ReceivePlanNode)node;
            ret += PlanNodeUtil.debugOutputColumns("OutputColumns", cast_node, spacer);
            
        } else if (node instanceof SendPlanNode) {
            ret += spacer + "Fake[" + ((SendPlanNode)node).getFake() + "]\n";
            ret += PlanNodeUtil.debugOutputColumns("InputColumns", node, spacer);
            
        } else if (node instanceof SeqScanPlanNode) {
            ret += spacer + "Scan Expression: " + (((SeqScanPlanNode)node).getPredicate() != null ? "\n" + ExpressionUtil.debug(((SeqScanPlanNode)node).getPredicate(), spacer) : null + "\n");
            
        } else if (node instanceof UnionPlanNode) {
            // Nothing
        } else if (node instanceof UpdatePlanNode) {
            // Nothing
        } else {
            LOG.fatal("Unsupported PlanNode type: " + node.getClass().getSimpleName());
            System.exit(1);
        }
        
        //
        // Inline PlanNodes
        //
        if (!node.getInlinePlanNodes().isEmpty()) {
            String internal_spacer = info_spacer + "  ";
            for (AbstractPlanNode inline_node : node.getInlinePlanNodes().values()) {
                ret += info_spacer + "Inline " + inline_node + ":\n";
                ret += PlanNodeUtil.debug(inline_node, internal_spacer);
            } 
        }
        return (ret);
    }
}
