package edu.brown.catalog;

import java.util.*;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.voltdb.catalog.*;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.*;
import org.voltdb.utils.Encoder;

import edu.brown.designer.ColumnSet;
import edu.brown.designer.DesignerUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.utils.*;

public abstract class QueryPlanUtil {
    private static final Logger LOG = Logger.getLogger(QueryPlanUtil.class.getName());
    
    /**
     * PlanFragmentId -> AbstractPlanNode
     */
    private static final Map<String, AbstractPlanNode> CACHE_DESERIALIZE_FRAGMENT = new HashMap<String, AbstractPlanNode>();

    /**
     * Procedure.Statement -> AbstractPlanNode
     */
    private static final Map<String, AbstractPlanNode> CACHE_DESERIALIZE_SS_STATEMENT = new HashMap<String, AbstractPlanNode>();
    private static final Map<String, AbstractPlanNode> CACHE_DESERIALIZE_MS_STATEMENT = new HashMap<String, AbstractPlanNode>();

    /**
     * Statement -> Sorted List of PlanFragments
     */
    private static final Map<Statement, List<PlanFragment>> CACHE_SORTED_SS_FRAGMENTS = new HashMap<Statement, List<PlanFragment>>();
    private static final Map<Statement, List<PlanFragment>> CACHE_SORTED_MS_FRAGMENTS = new HashMap<Statement, List<PlanFragment>>();
    
    /**
     * 
     */
    private static final Map<String, String> CACHE_STMTPARAMETER_COLUMN = new HashMap<String, String>();

    /**
     * Using this Comparator will sort a list of PlanFragments by their execution order
     */
    protected static final Comparator<PlanFragment> PLANFRAGMENT_EXECUTION_ORDER = new Comparator<PlanFragment>() {
        @Override
        public int compare(PlanFragment o1, PlanFragment o2) {
            AbstractPlanNode node1 = null;
            AbstractPlanNode node2 = null;
            try {
                node1 = QueryPlanUtil.deserializePlanFragment(o1);
                node2 = QueryPlanUtil.deserializePlanFragment(o2);
            } catch (Exception ex) {
                LOG.fatal(ex);
                System.exit(1);
            }
            // o1 > o2
            return (node2.getPlanNodeId() - node1.getPlanNodeId());
        }
    };
    
    /**
     * For the given StmtParameter object, return the column that it is used against in the Statement
     * @param catalog_stmt_param
     * @return
     * @throws Exception
     */
    public static Column getColumnForStmtParameter(StmtParameter catalog_stmt_param) throws Exception {
        String param_key = CatalogKey.createKey(catalog_stmt_param);
        String col_key = CACHE_STMTPARAMETER_COLUMN.get(param_key);

        if (col_key == null) {
            Statement catalog_stmt = (Statement)catalog_stmt_param.getParent();
            ColumnSet cset = DesignerUtil.extractStatementColumnSet(catalog_stmt, false);
            // System.err.println(cset.debug());
            Set<Column> matches = cset.findAllForOther(Column.class, catalog_stmt_param);
            // System.err.println("MATCHES: " + matches);
            if (matches.isEmpty()) {
                LOG.warn("Unable to find any column with param #" + catalog_stmt_param.getIndex() + " in " + catalog_stmt);
            } else {
                col_key = CatalogKey.createKey(CollectionUtil.getFirst(matches));
            }
            CACHE_STMTPARAMETER_COLUMN.put(param_key, col_key);
        }
        return (col_key != null ? CatalogKey.getFromKey(CatalogUtil.getDatabase(catalog_stmt_param), col_key, Column.class) : null);
    }
    
    /**
     * For a given list of PlanFragments, return them in a sorted list based on how they
     * must be executed. The first element in the list will be the first PlanFragment that must be
     * executed (i.e., the one at the bottom of the PlanNode tree).
     *
     * @param catalog_frags
     * @return
     * @throws Exception
     */
    public static List<PlanFragment> sortPlanFragments(List<PlanFragment> catalog_frags) {
        Collections.sort(catalog_frags, PLANFRAGMENT_EXECUTION_ORDER);
        return (catalog_frags);
    }
    
    /**
     * 
     * @param catalog_stmt
     * @param singlepartition
     * @return
     */
    public static List<PlanFragment> getSortedPlanFragments(Statement catalog_stmt, boolean singlepartition) {
        Map<Statement, List<PlanFragment>> cache = (singlepartition ? CACHE_SORTED_SS_FRAGMENTS : CACHE_SORTED_MS_FRAGMENTS); 
        List<PlanFragment> ret = cache.get(catalog_stmt);
        if (ret == null) {
            CatalogMap<PlanFragment> catalog_frags = null;
            if (singlepartition && catalog_stmt.getHas_singlesited()) {
                catalog_frags = catalog_stmt.getFragments();
            } else if (catalog_stmt.getHas_multisited()) {
                catalog_frags = catalog_stmt.getMs_fragments();
            }
            
            if (catalog_frags != null) {
                List<PlanFragment> fragments = (List<PlanFragment>)CollectionUtil.addAll(new ArrayList<PlanFragment>(), catalog_frags); 
                QueryPlanUtil.sortPlanFragments(fragments);
                ret = Collections.unmodifiableList(fragments);
                cache.put(catalog_stmt, ret);
            }
        }
        return (ret);
    }
    
    /**
     * 
     * @param nodes
     * @param singlesited TODO
     * @return
     */
    private static AbstractPlanNode reconstructPlanNodeTree(Statement catalog_stmt, List<AbstractPlanNode> nodes, boolean singlesited) throws Exception {
        LOG.debug("reconstructPlanNodeTree(" + catalog_stmt + ", " + nodes + ", true)");
        
        // HACK: We should have all SendPlanNodes here, so we just need to order them 
        // by their Node ids from lowest to highest (where the root has id = 1)
        TreeSet<AbstractPlanNode> sorted_nodes = new TreeSet<AbstractPlanNode>(new Comparator<AbstractPlanNode>() {
            @Override
            public int compare(AbstractPlanNode o1, AbstractPlanNode o2) {
                // o1 < o2
                return o1.getPlanNodeId() - o2.getPlanNodeId();
            }
        });
        sorted_nodes.addAll(nodes);
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
                        assert(next_last_node.isEmpty());
                        next_last_node.add(element);
                    }
                }
            }.traverse(node);
            
            if (!next_last_node.isEmpty()) last_node = next_last_node.remove(0);
        } // FOR
        return (CollectionUtil.getFirst(sorted_nodes));
    }
    
    /**
     * 
     * @param catalog_db
     * @param exptree
     * @return
     * @throws Exception
     */
    public static AbstractExpression deserializeExpression(Database catalog_db, String exptree) throws Exception {
        AbstractExpression exp = null;
        if (exptree != null && !exptree.isEmpty()) {
            JSONObject json_obj = new JSONObject(Encoder.hexDecodeToString(exptree));
            exp = AbstractExpression.fromJSONObject(json_obj, catalog_db);
        }
        return (exp);
    }
    
    /**
     * 
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static AbstractPlanNode deserializeStatement(Statement catalog_stmt, boolean singlesited) throws Exception {
        if (singlesited && !catalog_stmt.getHas_singlesited()) {
            String msg = "No single-sited plan is available for " + catalog_stmt + ". ";
            if (catalog_stmt.getHas_multisited()) {
                LOG.debug(msg + "Going to try to use multi-site plan");
                return (deserializeStatement(catalog_stmt, false));
            } else {
                LOG.fatal(msg + "No other plan is available");
                return (null);
            }
        } else if (!singlesited && !catalog_stmt.getHas_multisited()) {
            String msg = "No multi-sited plan is available for " + catalog_stmt + ". ";
            if (catalog_stmt.getHas_singlesited()) {
                LOG.warn(msg + "Going to try to use single-site plan");
                return (deserializeStatement(catalog_stmt, true));
            } else {
                LOG.fatal(msg + "No other plan is available");
                return (null);
            }
        }
        
        AbstractPlanNode ret = null;
        String cache_key = CatalogKey.createKey(catalog_stmt);
        Map<String, AbstractPlanNode> cache = (singlesited ? QueryPlanUtil.CACHE_DESERIALIZE_SS_STATEMENT : QueryPlanUtil.CACHE_DESERIALIZE_MS_STATEMENT);
        
        // This is probably not thread-safe because the AbstractPlanNode tree has pointers to
        // specific table catalog objects 
        if (cache.containsKey(cache_key)) {
            return (cache.get(cache_key));
        }
        Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        
        if (catalog_stmt.getFullplan() == null || catalog_stmt.getFullplan().isEmpty()) {
            throw new Exception("Unable to deserialize full query plan tree for " + catalog_stmt + ": The plan attribute is empty");
        }
        
        if (catalog_stmt.getFullplan() == null || catalog_stmt.getFullplan().isEmpty()) {
            throw new Exception("Unable to deserialize full query plan tree for " + catalog_stmt + ": The plan attribute is empty");
        }
        
        //
        // FIXME: If it's an INSERT query, then we have to use the plan fragments instead of
        // the full query plan tree because the full plan is missing the MaterializePlanNode
        // part for some reason.
        // NEVER TRUST THE FULL PLAN!
        //
        JSONObject jsonObject = null;
        List<AbstractPlanNode> nodes = new ArrayList<AbstractPlanNode>();
        CatalogMap<PlanFragment> fragments = (singlesited ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
        for (PlanFragment catalog_frag : fragments) {
            String jsonString = Encoder.hexDecodeToString(catalog_frag.getPlannodetree());
            jsonObject = new JSONObject(jsonString);
            PlanNodeList list = (PlanNodeList)PlanNodeTree.fromJSONObject(jsonObject, catalog_db);
            nodes.add(list.getRootPlanNode());
        } // FOR
        if (nodes.isEmpty()) {
            throw new Exception("Failed to retrieve query plan nodes from catalog for " + catalog_stmt + " in " + catalog_stmt.getParent());
        }
        try {
            ret = QueryPlanUtil.reconstructPlanNodeTree(catalog_stmt, nodes, true);
        } catch (Exception ex) {
            System.out.println("ORIGINAL NODES: " + nodes);
            throw ex;
        }
        
        if (ret == null) {
            throw new Exception("Unable to deserialize full query plan tree for " + catalog_stmt + ": The deserializer returned a null root node");
            //System.err.println(CatalogUtil.debugJSON(catalog_stmt));
            //System.exit(1);
        }
        
        cache.put(cache_key, ret);
        return (ret);
    }
    
    /**
     * Returns the PlanNode for the given PlanFragment
     * @param catalog_frgmt
     * @return
     * @throws Exception
     */
    public static AbstractPlanNode deserializePlanFragment(PlanFragment catalog_frgmt) throws Exception {
        String id = catalog_frgmt.getName();
        AbstractPlanNode ret = QueryPlanUtil.CACHE_DESERIALIZE_FRAGMENT.get(id);
        if (ret == null) {
            if (LOG.isDebugEnabled()) LOG.warn("No cached object for " + catalog_frgmt.fullName());
            Database catalog_db = CatalogUtil.getDatabase(catalog_frgmt);
            String jsonString = Encoder.hexDecodeToString(catalog_frgmt.getPlannodetree());
            PlanNodeList list = (PlanNodeList)PlanNodeTree.fromJSONObject(new JSONObject(jsonString), catalog_db);
            ret = list.getRootPlanNode();
            QueryPlanUtil.CACHE_DESERIALIZE_FRAGMENT.put(id, ret);
        }
        return (ret);
    }
    
    /**
     * Pre-load the cache for all of the PlanFragments
     * @param catalog_db
     * @throws Exception
     */
    public static void preload(Database catalog_db) throws Exception {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc() || catalog_proc.getHasjava() == false) continue; 
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                if (catalog_stmt.getHas_singlesited()) {
                    QueryPlanUtil.deserializeStatement(catalog_stmt, true);
                    QueryPlanUtil.getSortedPlanFragments(catalog_stmt, true);
                }
                if (catalog_stmt.getHas_multisited()) {
                    QueryPlanUtil.deserializeStatement(catalog_stmt, false);
                    QueryPlanUtil.getSortedPlanFragments(catalog_stmt, false);
                }
                
                for (PlanFragment catalog_frag : catalog_stmt.getFragments()) {
                    QueryPlanUtil.deserializePlanFragment(catalog_frag);
                } // FOR
                for (PlanFragment catalog_frag : catalog_stmt.getMs_fragments()) {
                    QueryPlanUtil.deserializePlanFragment(catalog_frag);
                } // FOR
            } // FOR
        } // FOR
    }
}
