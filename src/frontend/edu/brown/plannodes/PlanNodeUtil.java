package edu.brown.plannodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractOperationPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MaterializePlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;

/**
 * Utility methods for extracting information from AbstractPlanNode trees/nodes
 * 
 * @author pavlo
 */
public abstract class PlanNodeUtil {
    private static final Logger LOG = Logger.getLogger(PlanNodeUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final String INLINE_SPACER_PREFIX = "\u2502";
    private static final String INLINE_INNER_PREFIX = "\u251C";

    private static final String NODE_PREFIX = "\u25B6 ";
    private static final String SPACER_PREFIX = "\u2503";
    private static final String INNER_PREFIX = "\u2523";

    // ------------------------------------------------------------
    // CACHES
    // ------------------------------------------------------------

    /**
     * PlanFragmentId -> AbstractPlanNode
     */
    private static final Map<String, AbstractPlanNode> CACHE_DESERIALIZE_FRAGMENT = new HashMap<String, AbstractPlanNode>();

    /**
     * Procedure.Statement -> AbstractPlanNode
     */
    private static final Map<String, AbstractPlanNode> CACHE_DESERIALIZE_SP_STATEMENT = new HashMap<String, AbstractPlanNode>();
    private static final Map<String, AbstractPlanNode> CACHE_DESERIALIZE_MP_STATEMENT = new HashMap<String, AbstractPlanNode>();

    /**
     * Statement -> Sorted List of PlanFragments
     */
    private static final Map<Statement, List<PlanFragment>> CACHE_SORTED_SP_FRAGMENTS = new HashMap<Statement, List<PlanFragment>>();
    private static final Map<Statement, List<PlanFragment>> CACHE_SORTED_MP_FRAGMENTS = new HashMap<Statement, List<PlanFragment>>();

    private static final Map<Statement, Collection<Column>> CACHE_OUTPUT_COLUMNS = new HashMap<Statement, Collection<Column>>();

    /**
     * 
     */
    private static final Map<String, String> CACHE_STMTPARAMETER_COLUMN = new HashMap<String, String>();

    // ------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------

    public static void clearCache() {
        CACHE_DESERIALIZE_FRAGMENT.clear();
        CACHE_DESERIALIZE_MP_STATEMENT.clear();
        CACHE_DESERIALIZE_SP_STATEMENT.clear();
        CACHE_SORTED_MP_FRAGMENTS.clear();
        CACHE_SORTED_SP_FRAGMENTS.clear();
        CACHE_OUTPUT_COLUMNS.clear();
        CACHE_STMTPARAMETER_COLUMN.clear();
    }

    /**
     * Returns the root node in the tree for the given node
     * 
     * @param node
     * @return
     */
    public static AbstractPlanNode getRoot(AbstractPlanNode node) {
        return (node.getParentPlanNodeCount() > 0 ? getRoot(node.getParent(0)) : node);
    }

    /**
     * Return a set of all the PlanNodeTypes in the tree
     * 
     * @param node
     * @return
     */
    public static Collection<PlanNodeType> getPlanNodeTypes(AbstractPlanNode node) {
        final Set<PlanNodeType> types = new HashSet<PlanNodeType>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                types.add(element.getPlanNodeType());
            }
        }.traverse(node);
        assert (types.size() > 0);
        return (types);
    }
    
    /**
     * Returns true if the AbstractPlanNode contains a range query
     * @param rootNode
     * @return
     */
    public static boolean isRangeQuery(AbstractPlanNode rootNode) {
        for (ExpressionType expType : getScanExpressionTypes(rootNode)) {
            switch (expType) {
                case COMPARE_GREATERTHAN:
                case COMPARE_GREATERTHANOREQUALTO:
                case COMPARE_IN:
                case COMPARE_LESSTHAN:
                case COMPARE_LESSTHANOREQUALTO:
                case COMPARE_LIKE:
                case CONJUNCTION_OR:
                    return (true);
            } // SWITCH
        } // FOR
        return (false);
    }

    /**
     * Returns true if the tree at the given root node is for a distributed
     * query plan
     * 
     * @param rootNode
     * @return
     */
    public static boolean isDistributedQuery(AbstractPlanNode rootNode) {
        return (PlanNodeUtil.getPlanNodeTypes(rootNode).contains(PlanNodeType.RECEIVE));
    }

    /**
     * Get all the AbstractExpression roots used in the given AbstractPlanNode.
     * Non-recursive
     * 
     * @param node
     * @return
     */
    public static Collection<AbstractExpression> getExpressionsForPlanNode(AbstractPlanNode node, PlanNodeType... exclude) {
        return PlanNodeUtil.getExpressionsForPlanNode(node, new ListOrderedSet<AbstractExpression>(), exclude);
    }

    /**
     * Get all the AbstractExpression roots used in the given AbstractPlanNode.
     * Non-recursive
     * 
     * @param node
     * @param exps
     * @return
     */
    public static Collection<AbstractExpression> getExpressionsForPlanNode(AbstractPlanNode node, Set<AbstractExpression> exps, PlanNodeType... exclude) {
        final PlannerContext plannerContext = PlannerContext.singleton();
        final PlanNodeType node_type = node.getPlanNodeType();
        for (PlanNodeType e : exclude) {
            if (node_type == e)
                return (exps);
        } // FOR

        switch (node_type) {
            // ---------------------------------------------------
            // SCANS
            // ---------------------------------------------------
            case INDEXSCAN: {
                IndexScanPlanNode idx_node = (IndexScanPlanNode) node;
                if (idx_node.getEndExpression() != null)
                    exps.add(idx_node.getEndExpression());
                for (AbstractExpression exp : idx_node.getSearchKeyExpressions()) {
                    if (exp != null)
                        exps.add(exp);
                } // FOR

                // Fall through down into SEQSCAN....
            }
            case SEQSCAN: {
                AbstractScanPlanNode scan_node = (AbstractScanPlanNode) node;
                if (scan_node.getPredicate() != null)
                    exps.add(scan_node.getPredicate());
                break;
            }
            // ---------------------------------------------------
            // JOINS
            // ---------------------------------------------------
            case NESTLOOP:
            case NESTLOOPINDEX: {
                AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode) node;
                if (cast_node.getPredicate() != null)
                    exps.add(cast_node.getPredicate());

                // We always need to look at the inline scan nodes for joins
                for (AbstractPlanNode inline_node : cast_node.getInlinePlanNodes().values()) {
                    if (inline_node instanceof AbstractScanPlanNode) {
                        PlanNodeUtil.getExpressionsForPlanNode(inline_node, exps);
                    }
                } // FOR
                break;
            }
            // ---------------------------------------------------
            // PROJECTION
            // ---------------------------------------------------
            case MATERIALIZE:
            case PROJECTION: {
                for (Integer col_guid : node.getOutputColumnGUIDs()) {
                    PlanColumn col = plannerContext.get(col_guid);
                    assert (col != null) : "Invalid PlanColumn #" + col_guid;
                    if (col.getExpression() != null)
                        exps.add(col.getExpression());
                } // FOR
                break;
            }
            // ---------------------------------------------------
            // AGGREGATE
            // ---------------------------------------------------
            case AGGREGATE:
            case HASHAGGREGATE: {
                AggregatePlanNode agg_node = (AggregatePlanNode) node;
                for (Integer col_guid : agg_node.getAggregateColumnGuids()) {
                    PlanColumn col = plannerContext.get(col_guid);
                    assert (col != null) : "Invalid PlanColumn #" + col_guid;
                    if (col.getExpression() != null)
                        exps.add(col.getExpression());
                } // FOR
                for (Integer col_guid : agg_node.getGroupByColumnGuids()) {
                    PlanColumn col = plannerContext.get(col_guid);
                    assert (col != null) : "Invalid PlanColumn #" + col_guid;
                    if (col.getExpression() != null)
                        exps.add(col.getExpression());
                } // FOR
                break;
            }
            // ---------------------------------------------------
            // ORDERBY
            // ---------------------------------------------------
            case ORDERBY: {
                OrderByPlanNode orby_node = (OrderByPlanNode) node;
                for (Integer col_guid : orby_node.getSortColumnGuids()) {
                    PlanColumn col = plannerContext.get(col_guid);
                    assert (col != null) : "Invalid PlanColumn #" + col_guid;
                    if (col.getExpression() != null)
                        exps.add(col.getExpression());
                } // FOR
                break;
            }
            default:
                // Do nothing...
        } // SWITCH
        return (exps);
    }

    /**
     * Return all the ExpressionTypes used for scan predicates in the given PlanNode
     * @param node
     * @return
     */
    public static Collection<ExpressionType> getScanExpressionTypes(AbstractPlanNode root) {
        final Set<ExpressionType> found = new HashSet<ExpressionType>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode node) {
                Set<AbstractExpression> exps = new HashSet<AbstractExpression>();
                switch (node.getPlanNodeType()) {
                    // SCANS
                    case INDEXSCAN: {
                        IndexScanPlanNode idx_node = (IndexScanPlanNode) node;
                        exps.add(idx_node.getEndExpression());
                        exps.addAll(idx_node.getSearchKeyExpressions());
                    }
                    case SEQSCAN: {
                        AbstractScanPlanNode scan_node = (AbstractScanPlanNode) node;
                        exps.add(scan_node.getPredicate());
                        break;
                    }
                    // JOINS
                    case NESTLOOP:
                    case NESTLOOPINDEX: {
                        AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode) node;
                        exps.add(cast_node.getPredicate());
                        break;
                    }
                    default:
                        // Do nothing...
                } // SWITCH

                for (AbstractExpression exp : exps) {
                    if (exp == null)
                        continue;
                    found.addAll(ExpressionUtil.getExpressionTypes(exp));
                } // FOR
                return;
            }
        }.traverse(root);
        return (found);
    }
    
    /**
     * Get the Columns referenced in the output portion of a SELECT query
     * @param catalog_stmt
     * @return A collection of the output Columns
     * @throws Exception
     */
    public static Collection<Column> getOutputColumnsForStatement(Statement catalog_stmt) throws Exception {
        Collection<Column> ret = CACHE_OUTPUT_COLUMNS.get(catalog_stmt);
        if (ret == null && catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
            // It's easier to figure things out if we use the single-partition
            // query plan
            final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
            final AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
            assert (root != null);
            assert (root instanceof SendPlanNode) : "Unexpected PlanNode root " + root + " for " + catalog_stmt.fullName();

            // We need to examine down the tree to figure out what this thing
            // shoving out to the outside world
            assert (root.getChildPlanNodeCount() == 1) : "Unexpected one child for " + root + " for " + catalog_stmt.fullName() + " but it has " + root.getChildPlanNodeCount();
            ret = Collections.unmodifiableCollection(PlanNodeUtil.getOutputColumnsForPlanNode(catalog_db, root.getChild(0)));
            CACHE_OUTPUT_COLUMNS.put(catalog_stmt, ret);
        }
        return (ret);
    }

    /**
     * Get the set of columns
     * 
     * @param catalog_db
     * @param node
     * @return
     */
    public static Collection<Column> getOutputColumnsForPlanNode(final Database catalog_db, AbstractPlanNode node) {
        final PlannerContext pcontext = PlannerContext.singleton();
        final Collection<Integer> planColumnIds = getOutputColumnIdsForPlanNode(node);

        final Set<Column> columns = new ListOrderedSet<Column>();
        for (Integer column_guid : planColumnIds) {
            PlanColumn planColumn = pcontext.get(column_guid);
            assert (planColumn != null);
            AbstractExpression exp = planColumn.getExpression();
            assert (exp != null);
            Collection<Column> exp_cols = ExpressionUtil.getReferencedColumns(catalog_db, exp);
            if (debug.val)
                LOG.debug(planColumn.toString() + " => " + exp_cols);
            columns.addAll(exp_cols);
        } // FOR

        return (columns);
    }

    /**
     * Get the set of columns
     * 
     * @param catalogContext
     * @param node
     * @return
     */
    public static Collection<AbstractExpression> getOutputExpressionsForPlanNode(AbstractPlanNode node) {
        final PlannerContext pcontext = PlannerContext.singleton();
        final Collection<Integer> planColumnIds = getOutputColumnIdsForPlanNode(node);

        final Collection<AbstractExpression> exps = new ListOrderedSet<AbstractExpression>();
        for (Integer column_guid : planColumnIds) {
            PlanColumn planColumn = pcontext.get(column_guid);
            assert (planColumn != null);
            AbstractExpression exp = planColumn.getExpression();
            assert (exp != null);
            exps.add(exp);
        } // FOR

        return (exps);
    }

    /**
     * @param node
     * @return
     */
    public static Collection<Integer> getOutputColumnIdsForPlanNode(AbstractPlanNode node) {
        final Collection<Integer> planColumnIds = new ListOrderedSet<Integer>();

        // 2011-07-20: Using the AbstractExpressions is the more accurate way of
        // getting the
        // Columns referenced in the output
        // If this is Scan that has an inline Projection, grab those too
        if ((node instanceof AbstractScanPlanNode) && node.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
            ProjectionPlanNode prj_node = node.getInlinePlanNode(PlanNodeType.PROJECTION);
            planColumnIds.addAll(prj_node.getOutputColumnGUIDs());
            if (debug.val)
                LOG.debug(prj_node.getPlanNodeType() + ": " + planColumnIds);
        } else {
            planColumnIds.addAll(node.getOutputColumnGUIDs());
            if (debug.val)
                LOG.debug(node.getPlanNodeType() + ": " + planColumnIds);
        }

        // If this is an AggregatePlanNode, then we also need to include columns
        // computed in the aggregates
        if (node instanceof AggregatePlanNode) {
            AggregatePlanNode agg_node = (AggregatePlanNode) node;
            planColumnIds.addAll(agg_node.getAggregateColumnGuids());
            if (debug.val)
                LOG.debug(node.getPlanNodeType() + ": " + agg_node.getAggregateColumnGuids());
        }

        return (planColumnIds);
    }

    /**
     * Get the set of columns that
     * 
     * @param catalog_db
     * @param node
     * @return
     */
    public static Collection<Column> getUpdatedColumnsForPlanNode(final Database catalog_db, AbstractPlanNode node) {
        Set<Column> columns = new ListOrderedSet<Column>();
        for (int ctr = 0, cnt = node.getOutputColumnGUIDs().size(); ctr < cnt; ctr++) {
            int column_guid = node.getOutputColumnGUIDs().get(ctr);
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            assert (column != null);

            final String column_name = column.getDisplayName();
            String table_name = column.originTableName();

            // If there is no table name, then check whether this is a scan
            // node.
            // If it is, then we can try to get the table name from the node's
            // target
            if (table_name == null && node instanceof AbstractScanPlanNode) {
                table_name = ((AbstractScanPlanNode) node).getTargetTableName();
            }

            // If this is a TupleAddressExpression or there is no target table
            // name, then we have
            // to skip this output column
            if (column_name.equalsIgnoreCase("tuple_address") || table_name == null)
                continue;

            Table catalog_tbl = null;
            try {
                catalog_tbl = catalog_db.getTables().get(table_name);
            } catch (Exception ex) {
                LOG.fatal("Failed to retrieve table '" + table_name + "'", ex);
                LOG.fatal(CatalogUtil.debug(catalog_db.getTables()));
                throw new RuntimeException(ex);
            }
            assert (catalog_tbl != null) : "Invalid table '" + table_name + "'";

            Column catalog_col = catalog_tbl.getColumns().get(column_name);
            assert (catalog_col != null) : "Invalid column '" + table_name + "." + column_name;

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
    public static <T extends AbstractPlanNode> Collection<T> getPlanNodes(AbstractPlanNode root, final Class<? extends T> search_class) {
        final Set<T> found = new HashSet<T>();
        new PlanNodeTreeWalker() {
            @SuppressWarnings("unchecked")
            @Override
            protected void callback(AbstractPlanNode element) {
                Class<? extends AbstractPlanNode> element_class = element.getClass();
                if (ClassUtil.getSuperClasses(element_class).contains(search_class)) {
                    found.add((T) element);
                }
                return;
            }
        }.traverse(root);
        return (found);
    }

    @SuppressWarnings("unchecked")
    public static <T extends AbstractPlanNode> Collection<T> getChildren(AbstractPlanNode node, Class<T> search_class) {
        final Set<T> found = new HashSet<T>();
        for (int i = 0, cnt = node.getChildPlanNodeCount(); i < cnt; i++) {
            AbstractPlanNode child = node.getChild(i);
            Class<? extends AbstractPlanNode> child_class = child.getClass();
            if (ClassUtil.getSuperClasses(child_class).contains(search_class)) {
                found.add((T) child);
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
        final int depth[] = { 0 };
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                int current_depth = this.getDepth();
                if (current_depth > depth[0])
                    depth[0] = current_depth;
            }
        }.traverse(root);
        return (depth[0]);
    }
    
    /**
     * Get the depth of an element in the tree
     * @param root
     * @param node
     * @return
     */
    public static int getDepth(AbstractPlanNode root, final AbstractPlanNode node) {
        final int depth[] = { 0 };
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element.equals(node)) {
                    depth[0] = this.getDepth();
                    this.stop();
                }
            }
        }.traverse(root);
        return (depth[0]);
    }

    /**
     * Return all the nodes in an tree that reference a particular table This
     * can be either scan nodes or operation nodes
     * 
     * @param root
     * @param catalog_tbl
     * @return
     */
    public static Collection<AbstractPlanNode> getPlanNodesReferencingTable(AbstractPlanNode root, final Table catalog_tbl) {
        final Set<AbstractPlanNode> found = new HashSet<AbstractPlanNode>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // AbstractScanNode
                if (element instanceof AbstractScanPlanNode) {
                    AbstractScanPlanNode cast_node = (AbstractScanPlanNode) element;
                    if (cast_node.getTargetTableName().equals(catalog_tbl.getName()))
                        found.add(cast_node);
                    // AbstractOperationPlanNode
                } else if (element instanceof AbstractOperationPlanNode) {
                    AbstractOperationPlanNode cast_node = (AbstractOperationPlanNode) element;
                    if (cast_node.getTargetTableName().equals(catalog_tbl.getName()))
                        found.add(cast_node);
                }
                return;
            }
        }.traverse(root);
        return (found);
    }

    /**
     * Return all of the PlanColumn guids used in this query plan tree
     * (including inline nodes)
     * 
     * @param root
     * @return
     */
    public static Collection<Integer> getAllPlanColumnGuids(AbstractPlanNode root) {
        final Set<Integer> guids = new HashSet<Integer>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                guids.addAll(element.getOutputColumnGUIDs());
            }
        }.traverse(root);
        return (guids);
    }

    public static String debug(AbstractPlanNode node) {
        return (PlanNodeUtil.debug(node, ""));
    }

    /**
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
            PlanColumn column = PlannerContext.singleton().get(column_guid);
            String inner = " : guid=" + column_guid;
            if (column != null) {
                assert (column_guid == column.guid());
                name = column.getDisplayName();
                inner += " : type=" + column.type() + " : size=" + column.width() + " : sort=" + column.getSortOrder() + " : storage=" + column.getStorage();
            }

            ret += String.format("%s   [%02d] %s%s\n", spacer, ctr, name, inner);

            if (column != null && column.getExpression() != null) { // && (true
                                                                    // || node
                                                                    // instanceof
                                                                    // ProjectionPlanNode))
                                                                    // {
                ret += ExpressionUtil.debug(column.getExpression(), spacer + "    ");
            }
        } // FOR
        return (ret);
    }

    private static String debug(AbstractPlanNode node, String spacer) {
        if (node == null) return (null);
        String ret = debugNode(node, spacer);

        // Print out all of our children
        spacer += "  ";
        for (int ctr = 0, cnt = node.getChildPlanNodeCount(); ctr < cnt; ctr++) {
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
        // final String last_prefix = (node.isInline() ? INLINE_LAST_PREFIX :
        // LAST_PREFIX);

        String spacer = orig_spacer + "  ";
        String inner_spacer = spacer + inner_prefix;
        String line_spacer = spacer + spacer_prefix;

        // General Information
        if (node.isInline() == false)
            sb.append(orig_spacer).append(NODE_PREFIX + node.toString() + "\n");
        sb.append(inner_spacer).append("Inline[" + node.isInline() + "]\n");

        // AbstractJoinPlanNode
        if (node instanceof AbstractJoinPlanNode) {
            AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode) node;
            sb.append(inner_spacer).append("JoinType[" + cast_node.getJoinType() + "]\n");
            sb.append(inner_spacer).append("Join Expression: " + (cast_node.getPredicate() != null ? "\n" + ExpressionUtil.debug(cast_node.getPredicate(), line_spacer) : null + "\n"));

            // AbstractOperationPlanNode
        } else if (node instanceof AbstractOperationPlanNode) {
            sb.append(inner_spacer).append("TargetTableId[" + ((AbstractOperationPlanNode) node).getTargetTableName() + "]\n");

            // AbstractScanPlanNode
        } else if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode cast_node = (AbstractScanPlanNode) node;
            sb.append(inner_spacer).append("TargetTableName[" + cast_node.getTargetTableName() + "]\n");
            sb.append(inner_spacer).append("TargetTableAlias[" + cast_node.getTargetTableAlias() + "]\n");
            sb.append(inner_spacer).append("TargetTableId[" + cast_node.getTargetTableName() + "]\n");
        }

        // AggregatePlanNode
        if (node instanceof AggregatePlanNode) {
            AggregatePlanNode cast_node = (AggregatePlanNode) node;
            sb.append(inner_spacer).append("AggregateTypes[" + cast_node.getAggregateTypes().size() + "]: " + cast_node.getAggregateTypes() + "\n");
            sb.append(inner_spacer).append("AggregateColumnOffsets[" + cast_node.getAggregateOutputColumns().size() + "]: " + cast_node.getAggregateOutputColumns() + "\n");
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("AggregateColumns", cast_node.getAggregateColumnGuids(), line_spacer));
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("GroupByColumns", cast_node.getGroupByColumnGuids(), line_spacer));

            // DeletePlanNode
        } else if (node instanceof DeletePlanNode) {
            sb.append(inner_spacer).append("Truncate[" + ((DeletePlanNode) node).isTruncate() + "\n");

            // DistinctPlanNode
        } else if (node instanceof DistinctPlanNode) {
            DistinctPlanNode dist_node = (DistinctPlanNode) node;
            PlanColumn col = PlannerContext.singleton().get(dist_node.getDistinctColumnGuid());
            sb.append(inner_spacer).append("DistinctColumn[" + col + "]\n");

            // IndexScanPlanNode
        } else if (node instanceof IndexScanPlanNode) {
            IndexScanPlanNode cast_node = (IndexScanPlanNode) node;
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
            sb.append(inner_spacer).append("MultiPartition[" + ((InsertPlanNode) node).getMultiPartition() + "]\n");

            // LimitPlanNode
        } else if (node instanceof LimitPlanNode) {
            sb.append(inner_spacer).append("Limit[" + ((LimitPlanNode) node).getLimit() + "]\n");
            sb.append(inner_spacer).append("Offset[" + ((LimitPlanNode) node).getOffset() + "]\n");

            // NestLoopIndexPlanNode
        } else if (node instanceof NestLoopIndexPlanNode) {
            // Nothing

            // NestLoopPlanNode
        } else if (node instanceof NestLoopPlanNode) {
            // Nothing

        } else if (node instanceof OrderByPlanNode) {
            OrderByPlanNode cast_node = (OrderByPlanNode) node;
            sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("SortColumns", cast_node.getSortColumnGuids(), line_spacer));

        } else if (node instanceof ProjectionPlanNode) {
            // ProjectionPlanNode cast_node = (ProjectionPlanNode)node;
            if (node instanceof MaterializePlanNode) {
                sb.append(line_spacer).append("Batched[" + ((MaterializePlanNode) node).isBatched() + "]\n");
            }

        } else if (node instanceof ReceivePlanNode) {
            // Nothing

        } else if (node instanceof SendPlanNode) {
            sb.append(inner_spacer).append("Fake[" + ((SendPlanNode) node).getFake() + "]\n");

        } else if (node instanceof SeqScanPlanNode) {
            sb.append(inner_spacer).append(
                    "Scan Expression: " + (((SeqScanPlanNode) node).getPredicate() != null ? "\n" + ExpressionUtil.debug(((SeqScanPlanNode) node).getPredicate(), line_spacer) : null + "\n"));

        } else if (node instanceof UnionPlanNode) {
            // Nothing
        } else if (node instanceof UpdatePlanNode) {
            sb.append(inner_spacer).append("UpdateIndexes[" + ((UpdatePlanNode) node).doesUpdateIndexes() + "]\n");
        } else {
            throw new RuntimeException("Unsupported PlanNode type: " + node.getClass().getSimpleName());
        }

        // Output Columns
        // if (false && node.getInlinePlanNode(PlanNodeType.PROJECTION) != null)
        // {
        // sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("OutputColumns (Inline Projection)",
        // node.getInlinePlanNode(PlanNodeType.PROJECTION), line_spacer));
        // } else {
        sb.append(inner_spacer).append(PlanNodeUtil.debugOutputColumns("OutputColumns", node.getOutputColumnGUIDs(), line_spacer));
        // }

        // Inline PlanNodes
        if (!node.getInlinePlanNodes().isEmpty()) {
            for (AbstractPlanNode inline_node : node.getInlinePlanNodes().values()) {
                sb.append(inner_spacer).append("Inline " + inline_node + ":\n");
                sb.append(PlanNodeUtil.debug(inline_node, line_spacer));
            }
        }
        return (sb.toString());
    }

    /**
     * For the given StmtParameter object, return the column that it is used
     * against in the Statement
     * 
     * @param catalog_stmt_param
     * @return
     * @throws Exception
     */
    public static Column getColumnForStmtParameter(StmtParameter catalog_stmt_param) {
        String param_key = CatalogKey.createKey(catalog_stmt_param);
        String col_key = PlanNodeUtil.CACHE_STMTPARAMETER_COLUMN.get(param_key);

        if (col_key == null) {
            Statement catalog_stmt = catalog_stmt_param.getParent();
            PredicatePairs cset = null;
            try {
                cset = CatalogUtil.extractStatementPredicates(catalog_stmt, false);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to extract ColumnSet for " + catalog_stmt_param.fullName(), ex);
            }
            assert (cset != null);
            // System.err.println(cset.debug());
            Collection<Column> matches = cset.findAllForOther(Column.class, catalog_stmt_param);
            // System.err.println("MATCHES: " + matches);
            if (matches.isEmpty()) {
                LOG.warn("Unable to find any column with param #" + catalog_stmt_param.getIndex() + " in " + catalog_stmt);
            } else {
                col_key = CatalogKey.createKey(CollectionUtil.first(matches));
            }
            PlanNodeUtil.CACHE_STMTPARAMETER_COLUMN.put(param_key, col_key);
        }
        return (col_key != null ? CatalogKey.getFromKey(CatalogUtil.getDatabase(catalog_stmt_param), col_key, Column.class) : null);
    }

    /**
     * For a given list of PlanFragments, return them in a sorted list based on
     * how they must be executed. The first element in the list will be the
     * first PlanFragment that must be executed (i.e., the one at the bottom of
     * the PlanNode tree).
     * 
     * @param catalog_frags
     * @return
     * @throws Exception
     */
    public static List<PlanFragment> sortPlanFragments(List<PlanFragment> catalog_frags) {
        Collections.sort(catalog_frags, PlanNodeUtil.PLANFRAGMENT_EXECUTION_ORDER);
        return (catalog_frags);
    }

    /**
     * Return a list of the PlanFragments for the given Statement that are sorted in
     * the order that they must be executed
     * @param catalog_stmt
     * @param singlePartition
     * @return
     */
    public static List<PlanFragment> getSortedPlanFragments(Statement catalog_stmt, boolean singlePartition) {
        Map<Statement, List<PlanFragment>> cache = (singlePartition ? PlanNodeUtil.CACHE_SORTED_SP_FRAGMENTS : PlanNodeUtil.CACHE_SORTED_MP_FRAGMENTS);
        List<PlanFragment> ret = cache.get(catalog_stmt);
        if (ret == null) {
            CatalogMap<PlanFragment> catalog_frags = null;
            if (singlePartition && catalog_stmt.getHas_singlesited()) {
                catalog_frags = catalog_stmt.getFragments();
            } else if (catalog_stmt.getHas_multisited()) {
                catalog_frags = catalog_stmt.getMs_fragments();
            }

            if (catalog_frags != null) {
                List<PlanFragment> fragments = (List<PlanFragment>) CollectionUtil.addAll(new ArrayList<PlanFragment>(), catalog_frags);
                sortPlanFragments(fragments);
                ret = Collections.unmodifiableList(fragments);
                cache.put(catalog_stmt, ret);
            }
        }
        return (ret);
    }

    /**
     * @param nodes
     * @param singlePartition
     *            TODO
     * @return
     */
    public static AbstractPlanNode reconstructPlanNodeTree(Statement catalog_stmt, List<AbstractPlanNode> nodes, boolean singlePartition) throws Exception {
        if (debug.val)
            LOG.debug("reconstructPlanNodeTree(" + catalog_stmt + ", " + nodes + ", true)");

        // HACK: We should have all SendPlanNodes here, so we just need to order
        // them
        // by their Node ids from lowest to highest (where the root has id = 1)
        TreeSet<AbstractPlanNode> sorted_nodes = new TreeSet<AbstractPlanNode>(new Comparator<AbstractPlanNode>() {
            @Override
            public int compare(AbstractPlanNode o1, AbstractPlanNode o2) {
                // o1 < o2
                return o1.getPlanNodeId() - o2.getPlanNodeId();
            }
        });
        sorted_nodes.addAll(nodes);
        if (debug.val)
            LOG.debug("SORTED NODES: " + sorted_nodes);
        AbstractPlanNode last_node = null;
        for (AbstractPlanNode node : sorted_nodes) {
            final AbstractPlanNode walker_last_node = last_node;
            final List<AbstractPlanNode> next_last_node = new ArrayList<AbstractPlanNode>();
            new PlanNodeTreeWalker() {
                @Override
                protected void callback(AbstractPlanNode element) {
                    if (element instanceof SendPlanNode && walker_last_node != null) {
                        walker_last_node.addAndLinkChild(element);
                    } else if (element instanceof ReceivePlanNode) {
                        assert (next_last_node.isEmpty());
                        next_last_node.add(element);
                    }
                }
            }.traverse(node);

            if (!next_last_node.isEmpty())
                last_node = next_last_node.remove(0);
        } // FOR
        return (CollectionUtil.first(sorted_nodes));
    }

    /**
     * Get the root AbstractPlanNode for the given Statement's query plan If the
     * singlePartition flag is true, then it will return the tree for the
     * single-partition query Otherwise, it will return the distributed query
     * plan tree
     * <B>IMPORTANT:</B> Note that the AbstractPlanNodes returned by this method will
     *                   be different than the ones returned by getPlanNodeTreeForPlanFragment()
     *                   because the roots of the PlanFragment trees will not have any parents,
     *                   whereas in this tree they could.
     * @param catalog_stmt
     * @param singlePartition
     * @return
     */
    public static AbstractPlanNode getRootPlanNodeForStatement(Statement catalog_stmt, boolean singlePartition) {
        if (singlePartition && !catalog_stmt.getHas_singlesited()) {
            String msg = "No single-partition plan is available for " + catalog_stmt + ". ";
            if (catalog_stmt.getHas_multisited()) {
                if (debug.val)
                    LOG.debug(msg + "Going to try to use multi-partition plan");
                return (getRootPlanNodeForStatement(catalog_stmt, false));
            } else {
                LOG.fatal(msg + "No other plan is available");
                return (null);
            }
        } else if (!singlePartition && !catalog_stmt.getHas_multisited()) {
            String msg = "No multi-sited plan is available for " + catalog_stmt + ". ";
            if (catalog_stmt.getHas_singlesited()) {
                if (debug.val)
                    LOG.warn(msg + "Going to try to use single-partition plan");
                return (getRootPlanNodeForStatement(catalog_stmt, true));
            } else {
                LOG.fatal(msg + "No other plan is available");
                return (null);
            }
        }

        // Check whether we have this cached already
        // This is probably not thread-safe because the AbstractPlanNode tree
        // has pointers to specific table catalog objects
        String cache_key = CatalogKey.createKey(catalog_stmt);
        Map<String, AbstractPlanNode> cache = (singlePartition ? PlanNodeUtil.CACHE_DESERIALIZE_SP_STATEMENT : PlanNodeUtil.CACHE_DESERIALIZE_MP_STATEMENT);
        AbstractPlanNode ret = cache.get(cache_key);
        if (ret != null)
            return (ret);

        // Otherwise construct the AbstractPlanNode tree
        Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        String fullPlan = (singlePartition ? catalog_stmt.getFullplan() : catalog_stmt.getMs_fullplan());
        if (fullPlan == null || fullPlan.isEmpty()) {
            throw new RuntimeException("Unable to deserialize full query plan tree for " + catalog_stmt + ": The plan attribute is empty");
        }

        try {
            String jsonString = Encoder.hexDecodeToString(fullPlan);
            JSONObject jsonObject = new JSONObject(jsonString);
            PlanNodeList list = (PlanNodeList) PlanNodeTree.fromJSONObject(jsonObject, catalog_db);
            ret = list.getRootPlanNode();
            // } else {
            // //
            // // FIXME: If it's an INSERT query, then we have to use the plan
            // fragments instead of
            // // the full query plan tree because the full plan is missing the
            // MaterializePlanNode
            // // part for some reason.
            // // NEVER TRUST THE FULL PLAN!
            // //
            // JSONObject jsonObject = null;
            // List<AbstractPlanNode> nodes = new ArrayList<AbstractPlanNode>();
            // CatalogMap<PlanFragment> fragments = (singlePartition ?
            // catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
            // for (PlanFragment catalog_frag : fragments) {
            // String jsonString =
            // Encoder.hexDecodeToString(catalog_frag.getPlannodetree());
            // jsonObject = new JSONObject(jsonString);
            // PlanNodeList list =
            // (PlanNodeList)PlanNodeTree.fromJSONObject(jsonObject,
            // catalog_db);
            // nodes.add(list.getRootPlanNode());
            // } // FOR
            // if (nodes.isEmpty()) {
            // throw new
            // Exception("Failed to retrieve query plan nodes from catalog for "
            // + catalog_stmt + " in " + catalog_stmt.getParent());
            // }
            // try {
            // ret = reconstructPlanNodeTree(catalog_stmt, nodes, true);
            // } catch (Exception ex) {
            // System.out.println("ORIGINAL NODES: " + nodes);
            // throw ex;
            // }
            // }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (ret == null) {
            throw new RuntimeException("Unable to deserialize full query plan tree for " + catalog_stmt + ": The deserializer returned a null root node");
        }

        cache.put(cache_key, ret);
        return (ret);
    }

    /**
     * Return all the leaf nodes in the query plan tree
     * 
     * @param root
     * @return
     */
    public static Collection<AbstractPlanNode> getLeafPlanNodes(AbstractPlanNode root) {
        final Set<AbstractPlanNode> leaves = new HashSet<AbstractPlanNode>();
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element.getChildPlanNodeCount() == 0)
                    leaves.add(element);
            }
        }.traverse(root);
        return (leaves);
    }

    /**
     * Returns the PlanNode for the given PlanFragment
     * 
     * @param catalog_frag
     * @return
     */
    public static AbstractPlanNode getPlanNodeTreeForPlanFragment(PlanFragment catalog_frag) {
        String id = catalog_frag.getName();
        AbstractPlanNode ret = PlanNodeUtil.CACHE_DESERIALIZE_FRAGMENT.get(id);
        if (ret == null) {
            if (debug.val)
                LOG.warn("No cached object for " + catalog_frag.fullName());
            Database catalog_db = CatalogUtil.getDatabase(catalog_frag);
            String jsonString = Encoder.hexDecodeToString(catalog_frag.getPlannodetree());
            PlanNodeList list = null;
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                list = (PlanNodeList) PlanNodeTree.fromJSONObject(jsonObject, catalog_db);
            } catch (JSONException ex) {
                String msg = String.format("Invalid PlanNodeTree for %s [plantreeLength=%d, jsonLength=%d]",
                                           catalog_frag.fullName(),
                                           catalog_frag.getPlannodetree().length(),
                                           jsonString.length());
                throw new RuntimeException(msg, ex);
            }
            ret = list.getRootPlanNode();
            PlanNodeUtil.CACHE_DESERIALIZE_FRAGMENT.put(id, ret);
        }
        return (ret);
    }
    
    /**
     * Returns true if the given AbstractPlaNode exists in plan tree for the given PlanFragment
     * This includes inline nodes
     * @param catalog_frag
     * @param node
     * @return
     */
    public static boolean containsPlanNode(final PlanFragment catalog_frag, final AbstractPlanNode node) {
        final AbstractPlanNode root = getPlanNodeTreeForPlanFragment(catalog_frag);
        final boolean found[] = { false };
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (element.equals(node)) {
                    found[0] = true;
                    this.stop();
                }
            }
        }.traverse(root);
        return (found[0]);
    }

    /**
     * Pre-load the cache for all of the PlanFragments
     * 
     * @param catalog_db
     * @throws Exception
     */
    public static void preload(Database catalog_db) throws Exception {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc() || catalog_proc.getHasjava() == false)
                continue;
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                if (catalog_stmt.getHas_singlesited()) {
                    getRootPlanNodeForStatement(catalog_stmt, true);
                    getSortedPlanFragments(catalog_stmt, true);
                }
                if (catalog_stmt.getHas_multisited()) {
                    getRootPlanNodeForStatement(catalog_stmt, false);
                    getSortedPlanFragments(catalog_stmt, false);
                }

                for (PlanFragment catalog_frag : catalog_stmt.getFragments()) {
                    getPlanNodeTreeForPlanFragment(catalog_frag);
                } // FOR
                for (PlanFragment catalog_frag : catalog_stmt.getMs_fragments()) {
                    getPlanNodeTreeForPlanFragment(catalog_frag);
                } // FOR
            } // FOR
        } // FOR
    }

    /**
     * Using this Comparator will sort a list of PlanFragments by their
     * execution order
     */
    public static final Comparator<PlanFragment> PLANFRAGMENT_EXECUTION_ORDER = new Comparator<PlanFragment>() {
        @Override
        public int compare(PlanFragment o1, PlanFragment o2) {
            AbstractPlanNode node1 = null;
            AbstractPlanNode node2 = null;
            try {
                node1 = getPlanNodeTreeForPlanFragment(o1);
                node2 = getPlanNodeTreeForPlanFragment(o2);
            } catch (Exception ex) {
                LOG.fatal(ex);
                throw new RuntimeException(ex);
            }
            // o1 > o2
            return (node2.getPlanNodeId() - node1.getPlanNodeId());
        }
    };

}
