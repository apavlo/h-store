package edu.brown.designer;

import java.util.*;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;

import org.voltdb.catalog.*;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.*;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;
import org.voltdb.expressions.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.graphs.*;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * 
 * @author pavlo
 */
public abstract class DesignerUtil {
    private static final Logger LOG = Logger.getLogger(DesignerUtil.class.getName());

    //
    // CACHES
    //
    private static final Map<Pair<String, Set<String>>, ColumnSet> CACHE_extractColumnSet = new HashMap<Pair<String, Set<String>>, ColumnSet>();
    
    public static List<Vertex> createCandidateRoots(DesignerInfo info, DesignerHints hints) throws Exception {
        return (DesignerUtil.createCandidateRoots(info, hints, info.dgraph));
    }
    
    /**
     * 
     * @param graph
     * @param agraph
     * @return
     * @throws Exception
     */
    public static List<Vertex> createCandidateRoots(final DesignerInfo info, final DesignerHints hints, final IGraph<Vertex, Edge> agraph) throws Exception {
        LOG.debug("Searching for candidate roots...");
        if (agraph == null) throw new NullPointerException("AccessGraph is Null");
        //
        // For each vertex, count the number of edges that point to it
        //
        List<Vertex> roots = new ArrayList<Vertex>();
        SortedMap<Double, Set<Vertex>> candidates = new TreeMap<Double, Set<Vertex>>(Collections.reverseOrder());
        for (Vertex vertex : info.dgraph.getVertices()) {
            if (!agraph.containsVertex(vertex)) continue;
            
            if (hints.force_replication.contains(vertex.getCatalogItem().getName())) continue;
            //
            // We only can only use this vertex as a candidate root if 
            // none of its parents (if it even has any) are used in the AccessGraph or are 
            // not marked for replication
            //
            boolean valid = true;
            Collection<Vertex> parents = info.dgraph.getPredecessors(vertex);
            for (Vertex other : agraph.getNeighbors(vertex)) {
                if (parents.contains(other) && !hints.force_replication.contains(other.getCatalogItem().getName())) {
                    LOG.debug("SKIP " + vertex + " [" + other + "]");
                    valid = false;
                    break;
                }
            } // FOR
            if (!valid) continue;
            //System.out.println("CANDIDATE: " + vertex);
            
            //
            // We now need to set the weight of the candidate.
            // The first way I did this was to count the number of outgoing edges from the candidate
            // Now I'm going to use the weights of the outgoing edges in the AccessGraph.
            //
            final List<Double> weights = new ArrayList<Double>(); 
            new VertexTreeWalker<Vertex, Edge>(info.dgraph) {
                @Override
                protected void callback(Vertex element) {
                    // Get the total weights from this vertex to all of its descendants
                    if (agraph.containsVertex(element)) {
                        double total_weight = 0d;
                        Collection<Vertex> descedents = info.dgraph.getDescendants(element);
                        //System.out.println(element + ": " + descedents);
                        for (Vertex descendent : descedents) {
                             if (descendent != element && agraph.containsVertex(descendent)) {
                                for (Edge edge : agraph.findEdgeSet(element, descendent)) {
                                    Double weight = edge.getTotalWeight();
                                    if (weight != null) total_weight += weight;
                                    //System.out.println(element + "->" + descendent);
                                }
                            }
                        } // FOR
                        weights.add(total_weight);
                    }
//                    Vertex parent = this.getPrevious();
//                    if (agraph.containsVertex(element) && parent != null) {
//                        for (Edge edge : agraph.findEdgeSet(parent, element)) {
//                            Double weight = (Double)edge.getAttribute(AccessGraph.EdgeAttributes.WEIGHT.name());
//                            weights.add(weight);
//                            System.out.println(parent + "->" + element);
//                        }
//                    }
                }
            }.traverse(vertex);
            double weight = 0d;
            for (Double _weight : weights) weight+= _weight;
            if (!candidates.containsKey(weight)) candidates.put(weight, new HashSet<Vertex>());
            candidates.get(weight).add(vertex);
            /*
            int count = info.dgraph.getOutEdges(vertex).size();
            if (count > 0 || agraph.getVertexCount() == 1) {
                if (!candidates.containsKey(count)) candidates.put(count, new HashSet<Vertex>());
                candidates.get(count).add(vertex);
                LOG.debug("Found candidate root '" + vertex + "' [" + count + "]");
            }*/
        } // FOR
        
        StringBuilder buffer = new StringBuilder();
        buffer.append("Found ").append(candidates.size()).append(" candidate roots and ranked them as follows:\n");
        int ctr = 0;
        for (Double weight : candidates.keySet()) {
            for (Vertex vertex : candidates.get(weight)) {
                buffer.append("\t[").append(ctr++).append("] ")
                      .append(vertex).append("  Weight=").append(weight).append("\n");
                roots.add(vertex);
            } // FOR
        } // FOR
        LOG.debug(buffer.toString());
        //LOG.info(buffer.toString());
        
        return (roots);
    }

    /**
     * Extract a ColumnSet for the given Statement catalog object for all Tables
     * If convert_params is set to true, then we will convert any StmtParameters that are mapped to ProcParameter
     * directly into the ProcParameter object.
     * @param catalog_stmt
     * @param convert_params
     * @return
     * @throws Exception
     */
    public static ColumnSet extractStatementColumnSet(final Statement catalog_stmt, final boolean convert_params) throws Exception {
        Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        Table tables[] = catalog_db.getTables().values();
        return (DesignerUtil.extractStatementColumnSet(catalog_stmt, convert_params, tables));
    }

    /**
     * Extract a ColumnSet for the given Statement catalog object that only consists of the Columns that
     * are referenced in the list of tables
     * If convert_params is set to true, then we will convert any StmtParameters that are mapped to ProcParameter
     * directly into the ProcParameter object.
     * @param catalog_stmt
     * @param convert_params
     * @param catalog_tables
     * @return
     * @throws Exception
     */
    public static ColumnSet extractStatementColumnSet(final Statement catalog_stmt, final boolean convert_params, final Table... catalog_tables) throws Exception {
        final Database catalog_db = (Database)catalog_stmt.getParent().getParent();
        final Set<Table> tables = new HashSet<Table>();
        final Set<String> table_keys = new HashSet<String>();
        for (Table table : catalog_tables) {
            // For some reason we get a null table when we use itemsArray up above
            if (table == null) continue;
            assert(table != null) : "Null table object? " + Arrays.toString(catalog_tables);
            tables.add(table);
            table_keys.add(CatalogKey.createKey(table));
        } // FOR
        
        Pair<String, Set<String>> key = Pair.of(CatalogKey.createKey(catalog_stmt), table_keys);
        ColumnSet cset = CACHE_extractColumnSet.get(key);
        if (cset == null) {
            cset = new ColumnSet();
            AbstractPlanNode root_node = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
        
            // WHERE Clause
            if (catalog_stmt.getExptree() != null && !catalog_stmt.getExptree().isEmpty()) {
                AbstractExpression root_exp = QueryPlanUtil.deserializeExpression(catalog_db, catalog_stmt.getExptree());
                DesignerUtil.extractExpressionColumnSet(catalog_stmt, catalog_db, cset, root_exp, convert_params, tables);
            }
            // INSERT
            if (catalog_stmt.getQuerytype() == QueryType.INSERT.getValue()) {
                DesignerUtil.extractInsertColumnSet(catalog_stmt, cset, root_node, convert_params, catalog_tables);
            // UPDATE
            // XXX: Should we be doing this?
            } else if (catalog_stmt.getQuerytype() == QueryType.UPDATE.getValue()) {
                DesignerUtil.extractUpdateColumnSet(catalog_stmt, catalog_db, cset, root_node, convert_params, tables);
            }
            
            CACHE_extractColumnSet.put(key, cset);
        }
        return (cset);
    }

    /**
     * Extract ColumnSet for the SET portion of an UPDATE statement
     * @param catalog_stmt
     * @param catalog_tables
     * @return
     * @throws Exception
     */
    public static ColumnSet extractUpdateColumnSet(final Statement catalog_stmt, final boolean convert_params, final Table... catalog_tables) throws Exception {
        assert(catalog_stmt.getQuerytype() == QueryType.UPDATE.getValue());
        final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        
        final Set<Table> tables = new HashSet<Table>();
        final Set<String> table_keys = new HashSet<String>();
        for (Table table : catalog_tables) {
            tables.add(table);
            table_keys.add(CatalogKey.createKey(table));
        } // FOR
        
        Pair<String, Set<String>> key = Pair.of(CatalogKey.createKey(catalog_stmt), table_keys);
        ColumnSet cset = CACHE_extractColumnSet.get(key);
        if (cset == null) {
            cset = new ColumnSet();
            AbstractPlanNode root_node = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
            DesignerUtil.extractUpdateColumnSet(catalog_stmt, catalog_db, cset, root_node, convert_params, tables);
            CACHE_extractColumnSet.put(key, cset);
        }
        return (cset);
    }

    
    /**
     * 
     * @param stats_catalog_db
     * @param last_cset
     * @param root_exp
     * @param catalog_tables
     */
    public static ColumnSet extractFragmentColumnSet(final PlanFragment catalog_frag, final boolean convert_params, final Table... catalog_tables) throws Exception {
        final Statement catalog_stmt = (Statement)catalog_frag.getParent();
        final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        // if (catalog_frag.guid == 279) LOG.setLevel(Level.DEBUG);
        
        // We need to be clever about what we're doing here
        // We always have to examine the fragment (rather than just the entire Statement), because
        // we don't know whehter they want the multi-sited version or not
        final Set<Table> tables = new HashSet<Table>();
        final Set<String> table_keys = new HashSet<String>();
        for (Table table : catalog_tables) {
            tables.add(table);
            table_keys.add(CatalogKey.createKey(table));
        } // FOR

        LOG.debug("Extracting column set for fragment #" + catalog_frag.getName() + ": " + tables);
        Pair<String, Set<String>> key = Pair.of(CatalogKey.createKey(catalog_frag), table_keys);
        ColumnSet cset = CACHE_extractColumnSet.get(key);
        if (cset == null) {
            AbstractPlanNode root_node = QueryPlanUtil.deserializePlanFragment(catalog_frag);
            // LOG.debug("PlanFragment Node:\n" + PlanNodeUtil.debug(root_node));
            
            cset = new ColumnSet();
            DesignerUtil.extractPlanNodeColumnSet(catalog_stmt, catalog_db, cset, root_node, convert_params, tables);
            CACHE_extractColumnSet.put(key, cset);
        }
        
        return (cset);
    }
    
    /**
     * 
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_node
     * @param tables
     * @throws Exception
     */
    public static void extractInsertColumnSet(final Statement catalog_stmt, final ColumnSet cset, final AbstractPlanNode root_node, final boolean convert_params, final Table...catalog_tables) throws Exception {
        assert(catalog_stmt.getQuerytype() == QueryType.INSERT.getValue());
        final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        final List<List<CatalogType>> materialize_elements = new ArrayList<List<CatalogType>>();
        
        // Find the MaterializePlanNode that feeds into the Insert
        // This will have the list of columns that will be used to insert into the table
        new PlanNodeTreeWalker() {
            @Override
            protected void callback(final AbstractPlanNode node) {
                // We should find the Materialize node before the Insert
                if (node instanceof MaterializePlanNode) {
                    for (Integer column_guid : node.m_outputColumns) {
                        PlanColumn column = PlannerContext.singleton().get(column_guid);
                        assert(column != null);
                        AbstractExpression exp = column.getExpression();
                        
                        // Now extract the CatalogType objects that are being referenced by this materialization column
                        final List<CatalogType> catalog_refs = new ArrayList<CatalogType>();
                        new ExpressionTreeWalker() {
                            @Override
                            protected void callback(AbstractExpression exp) {
                                if (! (exp instanceof AbstractValueExpression)) return;
                                CatalogType element = null;
                                switch (exp.getExpressionType()) {
                                    case VALUE_PARAMETER: {
                                        int param_idx = ((ParameterValueExpression)exp).getParameterId();
                                        element = catalog_stmt.getParameters().get(param_idx);
                                        if (element == null) {
                                            LOG.warn("ERROR: Unable to find Parameter object in catalog [" + ((ParameterValueExpression)exp).getParameterId() + "]");
                                            this.stop();
                                        }
                                        // We want to use the ProcParameter instead of the StmtParameter
                                        // It's not an error if the StmtParameter is not mapped to a ProcParameter
                                        if (convert_params && ((StmtParameter)element).getProcparameter() != null) {
                                            LOG.debug(element + "(" + element + ") --> ProcParameter[" + element.getField("procparameter") + "]");
                                            element = ((StmtParameter)element).getProcparameter();
                                        }
                                        break;
                                    }
                                    case VALUE_TUPLE_ADDRESS:
                                    case VALUE_TUPLE: {
                                        // This shouldn't happen, but it is nice to be told if it does...
                                        LOG.warn("Unexpected " + exp.getClass().getSimpleName() + " node when examining " + node.getClass().getSimpleName() + " for " + catalog_stmt);
                                        break;
                                    }
                                    default: {
                                        // Do nothing...
                                    }
                                } // SWITCH
                                if (element != null) {
                                    catalog_refs.add(element);
                                    LOG.debug(node + ": " + catalog_refs);
                                }
                                return;
                            }
                        }.traverse(exp);
                        materialize_elements.add(catalog_refs);
                    } // FOR
                    
                // InsertPlanNode
                } else if (node instanceof InsertPlanNode) {
                    InsertPlanNode insert_node = (InsertPlanNode)node;
                    Table catalog_tbl = catalog_db.getTables().get(insert_node.getTargetTableName());
                    
                    // We only support when the Materialize node is inserting data into all columns
                    if (materialize_elements.size() != catalog_tbl.getColumns().size()) {
                        String msg = String.format("%s has %d columns but the MaterializePlanNode has %d output columns",
                                                   catalog_tbl, catalog_tbl.getColumns().size(), materialize_elements.size()); 
                        LOG.fatal(PlanNodeUtil.debug(node));
                        throw new RuntimeException(msg);
                    }
                    
                    // Loop through each column position and add an entry in the ColumnSet for
                    // each catalog item that was used in the MaterializePlanNode
                    // For example, if the INSERT clause for a column FOO was "PARAM1 + PARAM2", then
                    // the column set will have separate entries for FOO->PARAM1 and FOO->PARAM2
                    LOG.debug("Materialize Elements: " + materialize_elements);
                    for (int ctr = 0, cnt = materialize_elements.size(); ctr < cnt; ctr++) {
                        Column catalog_col = catalog_tbl.getColumns().get(ctr);
                        assert(catalog_col != null);
                        for (CatalogType catalog_item : materialize_elements.get(ctr)) {
                            cset.add(catalog_col, catalog_item, ExpressionType.COMPARE_EQUAL, catalog_stmt);
                            LOG.debug(String.format("[%02d] Adding Entry %s => %s", ctr, catalog_col, catalog_item));
                        } // FOR
                    } // FOR
                }
                return;
            }
        }.traverse(root_node);
        return;
    }
    
    /**
     * 
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_node
     * @param tables
     * @throws Exception
     */
    public static void extractPlanNodeColumnSet(final Statement catalog_stmt, final Database catalog_db, final ColumnSet cset, final AbstractPlanNode root_node, final boolean convert_params, final Collection<Table> tables) throws Exception {
        final boolean d = LOG.isDebugEnabled();
        
        // Walk through the tree and figure out how the tables are being referenced
        new PlanNodeTreeWalker() {
            {
                this.setAllowRevisit(true);
            }
            protected void populate_children(PlanNodeTreeWalker.Children<AbstractPlanNode> children, AbstractPlanNode node) {
                super.populate_children(children, node);
                List<AbstractPlanNode> to_add = new ArrayList<AbstractPlanNode>();
                for (AbstractPlanNode child : children.getBefore()) {
                    to_add.addAll(child.getInlinePlanNodes().values());
                } // FOR
                children.addBefore(to_add);
                
                to_add.clear();
                for (AbstractPlanNode child : children.getAfter()) {
                    to_add.addAll(child.getInlinePlanNodes().values());
                } // FOR
                children.addAfter(to_add);
                
                LOG.debug(children);
                LOG.debug("-------------------------");
            };
            
            @Override
            protected void callback(final AbstractPlanNode node) {
                try {
                    LOG.debug("Examining child node " + node);
                    this._callback(node);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
                
            protected void _callback(final AbstractPlanNode node) throws Exception {
                ListOrderedSet<AbstractExpression> exps = new ListOrderedSet<AbstractExpression>();
                
                // IndexScanPlanNode
                if (node instanceof IndexScanPlanNode) {
                    IndexScanPlanNode cast_node = (IndexScanPlanNode)node;
                    Table catalog_tbl = catalog_db.getTables().get(cast_node.getTargetTableName());
                    assert(catalog_tbl != null);
                    Index catalog_idx = catalog_tbl.getIndexes().get(cast_node.getTargetIndexName());
                    assert(catalog_idx != null);
                    
                    // Search Key Expressions
                    List<ColumnRef> index_cols = CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index");
                    for (int i = 0, cnt = cast_node.getSearchKeyExpressions().size(); i < cnt; i++) {
                        AbstractExpression index_exp = cast_node.getSearchKeyExpressions().get(i);
                        Column catalog_col = index_cols.get(i).getColumn();
                        if (d) LOG.debug("[" + i + "] " + catalog_col);
                        exps.add(DesignerUtil.createTempExpression(catalog_col, index_exp));
                        if (d) LOG.debug("Added temp index search key expression:\n" + ExpressionUtil.debug(exps.get(exps.size()-1)));
                    } // FOR
                    
                    // End Expression
                    if (cast_node.getEndExpression() != null) {
                        exps.add(cast_node.getEndExpression());
                        if (d) LOG.debug("Added scan end expression:\n" + ExpressionUtil.debug(exps.get(exps.size()-1)));
                    }
                    
                    // Post-Scan Expression
                    if (cast_node.getPredicate() != null) {
                        exps.add(cast_node.getPredicate());
                        if (d) LOG.debug("Added post-scan predicate:\n" + ExpressionUtil.debug(exps.get(exps.size()-1)));
                    }
                
                //  SeqScanPlanNode
                } else if (node instanceof SeqScanPlanNode) {
                    SeqScanPlanNode cast_node = (SeqScanPlanNode)node;
                    if (cast_node.getPredicate() != null) {
                        exps.add(cast_node.getPredicate());
                        if (d) LOG.debug("Adding scan node predicate:\n" + ExpressionUtil.debug(exps.get(exps.size()-1)));
                    }
                  
                // Materialize
                } else if (node instanceof MaterializePlanNode) {
                    // Assume that if we're here, then they want the mappings from columns to StmtParameters
                    assert(tables.size() == 1);
                    Table catalog_tbl = CollectionUtil.getFirst(tables);
                    for (int ctr = 0, cnt = node.m_outputColumns.size(); ctr < cnt; ctr++) {
                        int column_guid = node.m_outputColumns.get(ctr);
                        PlanColumn column = PlannerContext.singleton().get(column_guid);
                        assert(column != null);
                        
                        Column catalog_col = catalog_tbl.getColumns().get(column.displayName());
                        assert(catalog_col != null) : "Invalid column name '" + column.displayName() + "' for " + catalog_tbl;
                        
                        AbstractExpression exp = column.getExpression();
                        if (exp instanceof ParameterValueExpression) {
                            StmtParameter catalog_param = catalog_stmt.getParameters().get(((ParameterValueExpression)exp).getParameterId());
                            cset.add(catalog_col, catalog_param, ExpressionType.COMPARE_EQUAL, catalog_stmt);
                        } else if (exp instanceof AbstractValueExpression) {
                            if (d) LOG.debug("Ignoring AbstractExpressionType type: " + exp);
                        } else {
                            throw new Exception("Unexpected AbstractExpression type: " + exp); 
                        }
                        
                    } // FOR
                // Join Nodes
                } else if (node instanceof AbstractJoinPlanNode) {
                    AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode)node;
                    if (cast_node.getPredicate() != null) {
                        exps.add(cast_node.getPredicate());
                        if (d) LOG.debug("Added join node predicate: " + ExpressionUtil.debug(exps.get(exps.size()-1)));
                    }
                }
                
                if (d) LOG.debug("Extracting expressions information from " + node + " for tables " + tables);
                for (AbstractExpression exp : exps) {
                    if (exp == null) continue;
                    DesignerUtil.extractExpressionColumnSet(catalog_stmt, catalog_db, cset, exp, convert_params, tables);
                } // FOR
                return;
            }
        }.traverse(root_node);
        return;
    }
    
    
    /**
     * 
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_exp
     * @param tables
     * @throws Exception
     */
    public static void extractExpressionColumnSet(final Statement catalog_stmt, final Database catalog_db, final ColumnSet cset, final AbstractExpression root_exp, final boolean convert_params, final Collection<Table> tables) throws Exception {
        final boolean d = LOG.isDebugEnabled();
        if (d) LOG.debug(catalog_stmt + "\n" + ExpressionUtil.debug(root_exp));
        
        new ExpressionTreeWalker() {
            {
                this.setAllowRevisit(true);
            }
            
            boolean on_leftside = true;
            ExpressionType compare_exp = null;
            final Set<CatalogType> left  = new HashSet<CatalogType>();
            final Set<CatalogType> right = new HashSet<CatalogType>();
            final Set<Table> used_tables = new HashSet<Table>();
            
            private void populate() {
                if (d) {
                    LOG.debug("POPULATE!");
                    LOG.debug("LEFT:  " + this.left);
                    LOG.debug("RIGHT: " + this.right);
                    LOG.debug("USED:  " + this.used_tables);
                }
                //
                // Both sides can't be empty and our extract items must cover 
                // all the tables that we are looking for. That is, if we were asked 
                // to lookup information on two tables (i.e., a JOIN), then we need
                // to have extract information from our tree that uses those two tables
                //
                if (!this.left.isEmpty() && !this.right.isEmpty()) { //  && (this.used_tables.size() == tables.size())) {
                    for (CatalogType left_element : this.left) {
                        for (CatalogType right_element : this.right) {
                            if (d) LOG.debug("Added entry: [" + left_element + " " + this.compare_exp + " " + right_element + "]"); 
                            cset.add(left_element, right_element, this.compare_exp, catalog_stmt);
                            if (d) LOG.debug("ColumnSet:\n" + cset.debug());
                        } // FOR
                    } // FOR
                }
                this.left.clear();
                this.right.clear();
                this.used_tables.clear();
                this.on_leftside = true;
                this.compare_exp = null;
                return;
            }
            
            @Override
            protected void callback(AbstractExpression exp) {
                if (d) LOG.debug("CALLBACK(counter=" + this.getCounter() + ") " + exp.getClass().getSimpleName());
//                if (exp instanceof ComparisonExpression) {
//                    LOG.debug("\n" + ExpressionUtil.debug(exp));
//                }
                
                // ComparisionExpression
                // We need to switch from the left to the right element set.
                // TODO: This assumes that they aren't doing something funky like "(col1 AND col2) AND col3"
                if (exp instanceof ComparisonExpression) {
                    if (!this.on_leftside) {
                        LOG.error("ERROR: Invalid expression tree format : Unexpected ComparisonExpression");
                        this.stop();
                        return;
                    }
                    this.on_leftside = false;
                    this.compare_exp = exp.getExpressionType();
                    
                    //
                    // Special Case: IN
                    //
                    if (exp instanceof InComparisonExpression) {
                        InComparisonExpression in_exp = (InComparisonExpression)exp;
                        for (AbstractExpression value_exp : in_exp.getValues()) {
                            this.processValueExpression(value_exp);
                        } // FOR
                        this.populate();
                    }
                    
                // When we hit a conjunction, we need to make a cross product of the left and
                // right attribute sets
                } else if (exp instanceof ConjunctionExpression) {
                    this.populate();
                    
                // Values: NULL, Tuple, Constants, Parameters
                } else if (exp instanceof AbstractValueExpression) {
                    this.processValueExpression(exp);
                }
                return;
            }
            
            /**
             * 
             * @param exp
             */
            private void processValueExpression(AbstractExpression exp) {
                CatalogType element = null;
                switch (exp.getExpressionType()) {
                    case VALUE_TUPLE: {
                        String table_name = ((TupleValueExpression)exp).getTableName();
                        String column_name = ((TupleValueExpression)exp).getColumnName();
                        if (d) LOG.debug("VALUE TUPLE: " + table_name + "." + column_name);
                        Table catalog_tbl = catalog_db.getTables().get(table_name);
                        
                        //
                        // Always use because we don't know whether the next table will
                        // be one of the ones that we are looking for
                        //
                        if (tables.contains(catalog_tbl)) {
                            if (d) LOG.debug("FOUND VALUE_TUPLE: " + table_name);
                            element = catalog_tbl.getColumns().get(column_name);
                            this.used_tables.add(catalog_tbl);
                        }
                        break;
                    }
                    case VALUE_TUPLE_ADDRESS: {
                        //
                        // ????
                        //
                        //String table_name = ((TupleAddressExpression)exp).getTableName();
                        //String column_name = ((TupleAddressExpression)exp).getColumnName();
                        //element = CatalogUtil.getColumn(catalog_db, table_name, column_name);
                        break;
                    }
                    case VALUE_CONSTANT: {
                        element = new ConstantValue();
                        ((ConstantValue)element).setIs_null(false);
                        ((ConstantValue)element).setType(((ConstantValueExpression)exp).getValueType().getValue());
                        ((ConstantValue)element).setValue(((ConstantValueExpression)exp).getValue());
                        break;
                    }
                    case VALUE_PARAMETER: {
                        int param_idx = ((ParameterValueExpression)exp).getParameterId();
                        element = catalog_stmt.getParameters().get(param_idx);
                        if (element == null) {
                            LOG.warn("ERROR: Unable to find Parameter object in catalog [" + ((ParameterValueExpression)exp).getParameterId() + "]");
                            this.stop();
                        }
                        // We want to use the ProcParameter instead of the StmtParameter
                        // It's not an error if the StmtParameter is not mapped to a ProcParameter
//                        LOG.debug("PARAMETER: " + element);
                        if (convert_params && ((StmtParameter)element).getProcparameter() != null) {
                            element = ((StmtParameter)element).getProcparameter();
                        }
                        
                        break;
                    }
                    default:
                        // Do nothing...
                } // SWITCH
                if (element != null) {
                    if (this.on_leftside) this.left.add(element);
                    else this.right.add(element);
                    if (d) LOG.debug("LEFT: " + this.left);
                    if (d) LOG.debug("RIGHT: " + this.right);
                }
            }
            
            @Override
            protected void callback_last(AbstractExpression element) {
                this.populate();
            }
        }.traverse(root_exp);
        return;
    }
    
    /**
     * 
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_node
     * @param tables
     * @throws Exception
     */
    public static void extractUpdateColumnSet(final Statement catalog_stmt, final Database catalog_db, final ColumnSet cset, final AbstractPlanNode root_node, final boolean convert_params, final Collection<Table> tables) throws Exception {
        //
        // Grab the columns that the plannode is going to update from the children feeding into us.
        //
        Set<UpdatePlanNode> update_nodes = PlanNodeUtil.getPlanNodes(root_node, UpdatePlanNode.class);
        for (UpdatePlanNode update_node : update_nodes) {
            Table catalog_tbl = catalog_db.getTables().get(update_node.getTargetTableName());
            //
            // Grab all the scan nodes that are feeding into us
            //
            Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root_node, AbstractScanPlanNode.class);
            assert(!scan_nodes.isEmpty());
            for (AbstractScanPlanNode scan_node : scan_nodes) {
                List<PlanColumn> output_cols = new ArrayList<PlanColumn>();
                List<AbstractExpression> output_exps = new ArrayList<AbstractExpression>();
                if (scan_node.m_outputColumns.isEmpty()) {
                    if (scan_node.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
                        ProjectionPlanNode proj_node = (ProjectionPlanNode)scan_node.getInlinePlanNode(PlanNodeType.PROJECTION);
                        for (int guid : proj_node.m_outputColumns) {
                            PlanColumn column = PlannerContext.singleton().get(guid);
                            assert(column != null);
                            output_cols.add(column);
                            output_exps.add(column.getExpression());
                        } // FOR
                    }
                } else {
                    for (int guid : scan_node.m_outputColumns) {
                        PlanColumn column = PlannerContext.singleton().get(guid);
                        assert(column != null);
                        output_cols.add(column);
                        output_exps.add(column.getExpression());
                    } // FOR
                }
                
                for (int i = 0, cnt = output_cols.size(); i < cnt; i++) {
                    PlanColumn column = output_cols.get(i);
                    AbstractExpression exp = output_exps.get(i);
                    // Skip TupleAddressExpression
                    if (!(exp instanceof TupleAddressExpression)) {
                        //
                        // Make a temporary expression where COL = Expression
                        // Har har har! I'm so clever!
                        //
                        String column_name = (column.originColumnName() != null ? column.originColumnName() : column.displayName());
                        Column catalog_col = catalog_tbl.getColumns().get(column_name);
                        if (catalog_col == null) System.err.println(catalog_tbl + ": " + CatalogUtil.debug(catalog_tbl.getColumns()));
                        assert(catalog_col != null) : "Missing column '" + catalog_tbl.getName() + "." + column_name + "'";
                        AbstractExpression root_exp = DesignerUtil.createTempExpression(catalog_col, exp);
                        // System.out.println(ExpressionUtil.debug(root_exp));
                        DesignerUtil.extractExpressionColumnSet(catalog_stmt, catalog_db, cset, root_exp, convert_params, tables);
                    }
                } // FOR
            }
            //System.out.println(PlanNodeUtil.debug(root_node));
        }
    } 
    
    /**
     * Create a temporary column expression that can be used with extractExpressionColumnSet
     * @param catalog_col
     * @param exp
     * @return
     */
    private static AbstractExpression createTempExpression(Column catalog_col, AbstractExpression exp) {
        Table catalog_tbl = (Table)catalog_col.getParent();
        
        TupleValueExpression tuple_exp = new TupleValueExpression();
        tuple_exp.setTableName(catalog_tbl.getName());
        tuple_exp.setColumnIndex(catalog_col.getIndex());
        tuple_exp.setColumnAlias(catalog_col.getName());
        tuple_exp.setColumnName(catalog_col.getName());
        
        return (new ComparisonExpression(ExpressionType.COMPARE_EQUAL, tuple_exp, exp));
    }
}