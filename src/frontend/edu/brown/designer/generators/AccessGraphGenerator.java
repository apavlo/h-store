/**
 * 
 */
package edu.brown.designer.generators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogPair;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.AccessGraph.AccessType;
import edu.brown.designer.AccessGraph.EdgeAttributes;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.graphs.GraphvizExport;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * @author pavlo
 */
public class AccessGraphGenerator extends AbstractGenerator<AccessGraph> {
    private static final Logger LOG = Logger.getLogger(AccessGraphGenerator.class);
    private static final boolean d = LOG.isDebugEnabled();
    private static final boolean t = LOG.isTraceEnabled();

    private final Procedure catalog_proc;
    private final Map<Statement, Set<DesignerEdge>> stmt_edge_xref = new HashMap<Statement, Set<DesignerEdge>>();
    private final Map<DesignerEdge, Set<Set<Statement>>> multi_stmt_edge_xref = new HashMap<DesignerEdge, Set<Set<Statement>>>();
    private final Set<Table> debug_tables = new HashSet<Table>();

    private final List<ProcParameter> shared_params = new ArrayList<ProcParameter>();
    private final Map<Pair<CatalogType, CatalogType>, PredicatePairs> table_csets = new HashMap<Pair<CatalogType, CatalogType>, PredicatePairs>();
    
    /**
     * @param stats_catalog_db
     * @param graph
     * @param workload
     * @param catalog_proc
     */
    public AccessGraphGenerator(DesignerInfo info, Procedure catalog_proc) {
        super(info);
        this.catalog_proc = catalog_proc;

        debug_tables.add(this.info.catalogContext.database.getTables().get("CUSTOMER"));
        debug_tables.add(this.info.catalogContext.database.getTables().get("DISTRICT"));
    }

    /**
     * Generate a single AccessGraph for all of the procedures in catalog
     * 
     * @param info
     * @return
     */
    public static AccessGraph generateGlobal(DesignerInfo info) {
        if (d)
            LOG.debug("Generating AccessGraph for entire catalog");

        AccessGraph agraph = new AccessGraph(info.catalogContext.database);
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            // Skip if there are no transactions in the workload for this
            // procedure
            assert (info.workload != null);
            if (info.workload.getTraces(catalog_proc).isEmpty()) {
                if (d)
                    LOG.debug("No " + catalog_proc + " transactions in workload. Skipping...");
            } else {
                try {
                    new AccessGraphGenerator(info, catalog_proc).generate(agraph);
                } catch (Exception ex) {
                    LOG.fatal(String.format("Failed to incorporate %s into global AccessGraph", catalog_proc.getName()), ex);
                    throw new RuntimeException(ex);
                }
            }
        } // FOR
        return (agraph);
    }

    public static AccessGraph convertToSingleColumnEdges(Database catalog_db, AccessGraph orig_agraph) {
        AccessGraph agraph = new AccessGraph(catalog_db);
        Map<CatalogPair, DesignerEdge> entry_edges = new HashMap<CatalogPair, DesignerEdge>();

        for (DesignerVertex v : orig_agraph.getVertices()) {
            agraph.addVertex(v);
        } // FOR

        for (DesignerEdge e : orig_agraph.getEdges()) {
            // Split up the ColumnSet into separate edges, one per entry
            PredicatePairs cset = e.getAttribute(EdgeAttributes.COLUMNSET);
            assert (cset != null);
            Collection<DesignerVertex> vertices = orig_agraph.getIncidentVertices(e);
            DesignerVertex v0 = CollectionUtil.first(vertices);
            DesignerVertex v1 = v0;
            if (vertices.size() > 1)
                v1 = CollectionUtil.get(vertices, 1);
            assert (v1 != null);

            Table catalog_tbl0 = v0.getCatalogItem();
            assert (catalog_tbl0 != null);
            Table catalog_tbl1 = v1.getCatalogItem();
            assert (catalog_tbl1 != null);

            if (d)
                LOG.debug(String.format("%s <-> %s\n%s", v0, v1, cset));

            for (CatalogPair entry : cset) {
                DesignerEdge new_e = entry_edges.get(entry);
                if (new_e == null) {
                    PredicatePairs new_cset = new PredicatePairs(cset.getStatements());
                    new_cset.add(entry);
                    new_e = new DesignerEdge(agraph, e);
                    new_e.setAttribute(EdgeAttributes.COLUMNSET, new_cset);
                    entry_edges.put(entry, new_e);

                    CatalogType parent0 = entry.getFirst().getParent();
                    CatalogType parent1 = entry.getSecond().getParent();

                    if (parent0 == null && parent1 == null) {
                        if (d)
                            LOG.debug(String.format("Skipping %s ===> %s, %s", entry, v0, v1));
                        continue;

                        // Check whether this is a self-referencing edge
                    } else if ((parent0 == null && parent1.equals(catalog_tbl0)) || (parent1 == null && parent0.equals(catalog_tbl0))) {
                        if (d)
                            LOG.debug(String.format("Self-referencing edge %s ===> %s, %s", entry, v0, v0));
                        agraph.addEdge(new_e, v0, v0);

                    } else if ((parent0 == null && parent1.equals(catalog_tbl1)) || (parent1 == null && parent0.equals(catalog_tbl1))) {
                        if (d)
                            LOG.debug(String.format("Self-referencing edge %s ===> %s, %s", entry, v1, v1));
                        agraph.addEdge(new_e, v1, v1);

                    } else if ((parent0.equals(catalog_tbl0) && parent1.equals(catalog_tbl1) == false) || (parent1.equals(catalog_tbl0) && parent0.equals(catalog_tbl1) == false)
                            || (parent0.equals(catalog_tbl0) && parent1.equals(catalog_tbl0))) {
                        if (d)
                            LOG.debug(String.format("Self-referencing edge %s ===> %s, %s", entry, v0, v0));
                        agraph.addEdge(new_e, v0, v0);

                    } else if ((parent0.equals(catalog_tbl1) && parent1.equals(catalog_tbl0) == false) || (parent1.equals(catalog_tbl1) && parent0.equals(catalog_tbl0) == false)
                            || (parent0.equals(catalog_tbl0) && parent1.equals(catalog_tbl0))) {
                        if (d)
                            LOG.debug(String.format("Self-referencing edge %s ===> %s, %s", entry, v1, v1));
                        agraph.addEdge(new_e, v1, v1);

                    } else {
                        if (d)
                            LOG.debug(String.format("New edge %s ===> %s, %s", entry, v0, v1));
                        agraph.addEdge(new_e, v0, v1);

                    }
                }
                // Add weights from original edge to this new edge
                new_e.addToWeight(e);
            } // FOR (entry)

            if (d)
                LOG.debug("=====================================================================");
        } // FOR
        return (agraph);
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.designer.analyzers.AbstractAnalyzer#generate(edu.brown.graphs
     * .AccessGraph)
     */
    @Override
    public void generate(AccessGraph agraph) throws Exception {
        if (d)
            LOG.debug("Constructing AccessGraph for Procedure '" + this.catalog_proc.getName() + "'");

        // First initialize our little graph...
        this.initialize(agraph);

        // Get the list of tables used in the procedure
        Collection<Table> proc_tables = CatalogUtil.getReferencedTables(this.catalog_proc);

        // Loop through each query and create edges for the explicitly defined
        // relationships:
        // + Add an edge from each table in the query table to all other
        //   tables in the same query
        // + Add a self-referencing edge for table0 if it is used in a filter
        //   predicate using a variable or an input parameter
        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            this.createExplicitEdges(agraph, catalog_stmt);
        } // FOR
        if (debug)
            LOG.debug("===============================================================");

        // After the initial edges have been added to the graph, loop back
        // through again looking for implicit references between two tables:
        //  + Tables implicitly joined by sharing parameters on foreign keys
        //  + Tables implicitly referenced together by using the same values on
        //    foreign keys
        Statement catalog_stmts[] = this.catalog_proc.getStatements().values();
        for (int stmt_ctr0 = 0, stmt_cnt = catalog_stmts.length; stmt_ctr0 < stmt_cnt; stmt_ctr0++) {
            Statement catalog_stmt0 = catalog_stmts[stmt_ctr0];
            Collection<Table> stmt0_tables = CatalogUtil.getReferencedTables(catalog_stmt0);
            this.setDebug(stmt0_tables.containsAll(debug_tables));

            // --------------------------------------------------------------
            // (3) Add an edge between the tables in this Statement and all other 
            // tables in other queries that share input parameters on foreign
            // keys relationships
            // --------------------------------------------------------------
            List<ProcParameter> params0 = new ArrayList<ProcParameter>();
            for (StmtParameter param : catalog_stmt0.getParameters()) {
                if (param.getProcparameter() != null)
                    params0.add(param.getProcparameter());
            } // FOR
            if (debug && d)
                LOG.debug(catalog_stmt0.getName() + " Params: " + params0);

            for (int stmt_ctr1 = stmt_ctr0 + 1; stmt_ctr1 < stmt_cnt; stmt_ctr1++) {
                Statement catalog_stmt1 = catalog_stmts[stmt_ctr1];
                if (catalog_stmt1 == null || catalog_stmt1.getParameters() == null)
                    continue;
                this.createSharedParamEdges(agraph, catalog_stmt0, catalog_stmt1, params0);
            } // FOR

            // --------------------------------------------------------------
            // (4) Create an edge between implicit references by primary key
            // This is when one statement has a predicate that uses an input
            // parameter that is not referenced in conjunction with other tables
            // --------------------------------------------------------------
            if (debug && d)
                LOG.debug("-----------------------------------------\n" +
                		  "Looking for implicit reference joins in " + catalog_stmt0 + "\n" + 
                          "TABLES: " + stmt0_tables);
            for (Table catalog_tbl : stmt0_tables) {
                this.createImplicitEdges(agraph, proc_tables, catalog_stmt0, catalog_tbl);
            } // FOR
        } // FOR

        //
        // (5) Now run through the workload and update the edges with all number
        // of times they
        // are used in the workload
        //
        // if (debug) System.err.println("STATEMENT EDGE XREF: " +
        // this.stmt_edge_xref.size());
        // for (Statement catalog_stmt : this.stmt_edge_xref.keySet()) {
        // if (debug) System.err.println(catalog_stmt + ": " +
        // this.stmt_edge_xref.get(catalog_stmt).size());
        // }
        // if (this.catalog_proc.getName().equals("neworder")) System.exit(1);
        // if (debug) System.err.println("MULTI-STATEMENT EDGE XREF: " +
        // this.multi_stmt_edge_xref.size());
        // for (Edge edge : this.multi_stmt_edge_xref.keySet()) {
        // if (debug) System.err.println(edge + ": " +
        // this.multi_stmt_edge_xref.get(edge).size());
        // }
        // if (this.catalog_proc.getName().equals("neworder")) System.exit(1);

        List<TransactionTrace> traces = this.info.workload.getTraces(this.catalog_proc);
        if (d)
            LOG.debug("Updating edge weights using " + traces.size() + " txn traces from workload");
        for (TransactionTrace xact : traces) {
            this.updateEdgeWeights(agraph, xact);
        } // FOR
        this.updateEdgeWeighModifiers(agraph);

        // Prune vertices that weren't used in the the procedure
        agraph.pruneIsolatedVertices();

        // Make sure all of the table used in the procedure are in the graph
        for (Table catalog_tbl : proc_tables) {
            assert (agraph.getVertex(catalog_tbl) != null) : "The AccessGraph is missing a vertex for " + catalog_tbl;
        }

        // Done!
        return;
    }

    /**
     * @param agraph
     * @throws Exception
     */
    protected void initialize(AccessGraph agraph) throws Exception {
        // Copy the vertices from the DependencyGraph into our graph
        for (DesignerVertex vertex : this.info.dgraph.getVertices()) {
            if (agraph.getVertex(vertex.getCatalogItem()) == null)
                agraph.addVertex(vertex);
        } // FOR
    }

    /**
     * @param agraph
     * @throws Exception
     */
    protected void createExplicitEdges(AccessGraph agraph, Statement catalog_stmt) throws Exception {
        if (d)
            LOG.debug("Looking at " + catalog_stmt.fullName());
        if (t)
            LOG.trace("SQL: " + catalog_stmt.getSqltext());
        List<Table> tables = new ArrayList<Table>();
        tables.addAll(CatalogUtil.getReferencedTables(catalog_stmt));
        for (int ctr = 0, cnt = tables.size(); ctr < cnt; ctr++) {
            Table table0 = tables.get(ctr);
            boolean debug_table = (debug_tables.contains(table0) && t);

            // --------------------------------------------------------------
            // (1) Add an edge from table0 to all other tables in the query
            // --------------------------------------------------------------
            for (int ctr2 = ctr + 1; ctr2 < cnt; ctr2++) {
                Table table1 = tables.get(ctr2);
                PredicatePairs cset = CatalogUtil.extractStatementPredicates(catalog_stmt, true, table0, table1);
                if (debug_table)
                    LOG.trace("Creating join edge between " + table0 + "<->" + table1 + " for " + catalog_stmt);
                this.addEdge(agraph, AccessType.SQL_JOIN, cset, agraph.getVertex(table0), agraph.getVertex(table1), catalog_stmt);
            } // FOR
              // --------------------------------------------------------------

            // --------------------------------------------------------------
            // (2) Add a self-referencing edge for table0 if it is used in
            // a filter predicate using a variable or an input parameter
            // --------------------------------------------------------------
            if (debug_table)
                LOG.trace("Looking for scan ColumnSet on table '" + table0.getName() + "'");
            PredicatePairs cset = CatalogUtil.extractStatementPredicates(catalog_stmt, true, table0);
            if (!cset.isEmpty()) {
                if (debug_table)
                    LOG.trace("Creating scan edge to " + table0 + " for " + catalog_stmt);
                // if (debug) if (d) LOG.debug("Scan Column SET[" +
                // table0.getName() + "]: " + cset.debug());
                DesignerVertex vertex = agraph.getVertex(table0);
                AccessType atype = (catalog_stmt.getQuerytype() == QueryType.INSERT.getValue() ? AccessType.INSERT : AccessType.SCAN);
                this.addEdge(agraph, atype, cset, vertex, vertex, catalog_stmt);
            }
            // --------------------------------------------------------------
        } // FOR
        return;
    }

    /**
     * @param agraph
     * @param stmt_ctr0
     * @throws Exception
     */
    protected void createSharedParamEdges(AccessGraph agraph, Statement catalog_stmt0, Statement catalog_stmt1, List<ProcParameter> catalog_stmt0_params) throws Exception {
        this.table_csets.clear();
        this.shared_params.clear();
        if (d) LOG.debug("Creating Shared Parameter Edges between " + catalog_stmt0.getName() + " <-> " + catalog_stmt1.getName());
        
        // Check whether these two queries share input parameters
        for (StmtParameter stmt_param : catalog_stmt1.getParameters()) {
            if (stmt_param.getProcparameter() != null && catalog_stmt0_params.contains(stmt_param.getProcparameter())) {
                shared_params.add(stmt_param.getProcparameter());
            }
        } // FOR
        if (shared_params.isEmpty())
            return;
        if (t) {
            LOG.trace(catalog_stmt0.getName() + " <==> " + catalog_stmt1.getName());
            LOG.trace("SHARED PARAMS: " + shared_params);
        }

        // For each shared parameter, we need to get the expressions that
        // use each one and figure out whether there is a foreign key
        // relationship
        // between the two tables used.
        for (ProcParameter param : shared_params) {
            Set<Column> columns0 = new HashSet<Column>();
            Set<Column> columns1 = new HashSet<Column>();

            if (this.stmt_edge_xref.containsKey(catalog_stmt0)) {
                for (DesignerEdge edge : this.stmt_edge_xref.get(catalog_stmt0)) {
                    PredicatePairs cset = (PredicatePairs) edge.getAttribute(EdgeAttributes.COLUMNSET.name());
                    columns0.addAll(cset.findAllForOther(Column.class, param));
                } // FOR
            }
            if (this.stmt_edge_xref.containsKey(catalog_stmt1)) {
                for (DesignerEdge edge : this.stmt_edge_xref.get(catalog_stmt1)) {
                    PredicatePairs cset = (PredicatePairs) edge.getAttribute(EdgeAttributes.COLUMNSET.name());
                    columns1.addAll(cset.findAllForOther(Column.class, param));
                } // FOR
            }

            // Partition the columns into sets based on their Table. This is
            // necessary
            // so that we can have specific edges between vertices
            Map<Table, Set<Column>> table_column_xref0 = new HashMap<Table, Set<Column>>();
            Map<Table, Set<Column>> table_column_xref1 = new HashMap<Table, Set<Column>>();
            for (CatalogType ctype : columns0) {
                Column col = (Column) ctype;
                Table tbl = (Table) col.getParent();
                if (!table_column_xref0.containsKey(tbl)) {
                    table_column_xref0.put(tbl, new HashSet<Column>());
                }
                table_column_xref0.get(tbl).add(col);
            } // FOR
            for (CatalogType ctype : columns1) {
                Column col = (Column) ctype;
                Table tbl = (Table) col.getParent();
                if (!table_column_xref1.containsKey(tbl)) {
                    table_column_xref1.put(tbl, new HashSet<Column>());
                }
                table_column_xref1.get(tbl).add(col);
            } // FOR

            // if (debug) {
            // StringBuilder buffer = new StringBuilder();
            // buffer.append("table_column_xref0:\n");
            // for (Table table : table_column_xref0.keySet()) {
            // buffer.append("  ").append(table).append(": ").append(table_column_xref0.get(table)).append("\n");
            // }
            // if (d) LOG.debug(buffer.toString());
            //
            // buffer = new StringBuilder();
            // buffer.append("table_column_xref1:\n");
            // for (Table table : table_column_xref1.keySet()) {
            // buffer.append("  ").append(table).append(": ").append(table_column_xref1.get(table)).append("\n");
            // }
            // if (d) LOG.debug(buffer.toString());
            // }

            // Now create a cross product of the two sets based on tables --
            // Nasty!!
            for (Table table0 : table_column_xref0.keySet()) {
                for (Table table1 : table_column_xref1.keySet()) {
                    if (table0 == table1)
                        continue;
                    Pair<CatalogType, CatalogType> table_pair = CatalogUtil.pair(table0, table1);
                    PredicatePairs cset = null;
                    if (table_csets.containsKey(table_pair)) {
                        cset = table_csets.get(table_pair);
                    } else {
                        cset = new PredicatePairs();
                    }
                    for (Column column0 : table_column_xref0.get(table0)) {
                        for (Column column1 : table_column_xref1.get(table1)) {
                            // TODO: Enforce foreign key dependencies
                            // TODO: Set real ComparisionExpression attribute
                            cset.add(column0, column1, ExpressionType.COMPARE_EQUAL);
                        } // FOR
                    } // FOR
                    table_csets.put(table_pair, cset);
                } // FOR
            } // FOR
        } // FOR

        // Now create the edges between the vertices
        for (Pair<CatalogType, CatalogType> table_pair : table_csets.keySet()) {
            DesignerVertex vertex0 = agraph.getVertex((Table) table_pair.getFirst());
            DesignerVertex vertex1 = agraph.getVertex((Table) table_pair.getSecond());
            PredicatePairs cset = table_csets.get(table_pair);

            if (t) {
                LOG.trace("Vertex0: " + vertex0.getCatalogKey());
                LOG.trace("Vertex1: " + vertex0.getCatalogKey());
                LOG.trace("ColumnSet:\n" + cset.debug() + "\n");
            }

            DesignerEdge edge = this.addEdge(agraph, AccessType.PARAM_JOIN, cset, vertex0, vertex1);
            if (!this.multi_stmt_edge_xref.containsKey(edge)) {
                this.multi_stmt_edge_xref.put(edge, new HashSet<Set<Statement>>());
            }
            Set<Statement> new_set = new HashSet<Statement>();
            new_set.add(catalog_stmt0);
            new_set.add(catalog_stmt1);
            this.multi_stmt_edge_xref.get(edge).add(new_set);

            cset.getStatements().add(catalog_stmt0);
            cset.getStatements().add(catalog_stmt1);
        } // FOR tablepair columnsets
    }

    /**
     * @param agraph
     * @param proc_tables
     * @param catalog_stmt0
     * @param catalog_tbl
     * @throws Exception
     */
    protected void createImplicitEdges(AccessGraph agraph, Collection<Table> proc_tables, Statement catalog_stmt0, Table catalog_tbl) throws Exception {
        // For each SCAN edge for this vertex, check whether the column has a
        // foreign key
        DesignerVertex vertex = agraph.getVertex(catalog_tbl);
        if (d) LOG.debug("Creating Implicit Edges for " + catalog_tbl);
        Collection<DesignerEdge> scanEdges = agraph.getIncidentEdges(vertex,
                                                                 AccessGraph.EdgeAttributes.ACCESSTYPE.name(),
                                                                 AccessType.SCAN);
        if (t) LOG.trace("\t" + catalog_tbl.getName() + " Incident Edges: " + scanEdges);
        for (DesignerEdge edge : scanEdges) {
            PredicatePairs scan_cset = (PredicatePairs) edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name());
            if (t) LOG.trace("\tSCAN EDGE: " + edge); // + "\n" +
                                                   // scan_cset.debug());

            // For each table that our foreign keys reference, will construct a
            // ColumnSet mapping
            Map<Table, PredicatePairs> fkey_column_xrefs = new HashMap<Table, PredicatePairs>();
            if (t) LOG.trace("\tOUR COLUMNS: " + scan_cset.findAllForParent(Column.class, catalog_tbl));
            for (Column catalog_col : scan_cset.findAllForParent(Column.class, catalog_tbl)) {
                // Get the foreign key constraint for this column and then add
                // it to the ColumnSet
                Collection<Constraint> catalog_consts = CatalogUtil.getConstraints(catalog_col.getConstraints());
                catalog_consts = CatalogUtil.findAll(catalog_consts, "type", ConstraintType.FOREIGN_KEY.getValue());
                if (!catalog_consts.isEmpty()) {
                    assert (catalog_consts.size() == 1) : CatalogUtil.getDisplayName(catalog_col) + " has " + catalog_consts.size() + " foreign key constraints: " + catalog_consts;
                    Constraint catalog_const = CollectionUtil.first(catalog_consts);
                    Table catalog_fkey_tbl = catalog_const.getForeignkeytable();
                    assert (catalog_fkey_tbl != null);

                    // Important! We only want to include tables that are
                    // actually referenced in this procedure
                    if (proc_tables.contains(catalog_fkey_tbl)) {
                        Column catalog_fkey_col = CollectionUtil.first(catalog_const.getForeignkeycols()).getColumn();
                        assert (catalog_fkey_col != null);

                        // TODO: Use real ExpressionType from the entry
                        if (!fkey_column_xrefs.containsKey(catalog_fkey_tbl)) {
                            fkey_column_xrefs.put(catalog_fkey_tbl, new PredicatePairs());
                        }
                        // if (debug) System.err.println("Foreign Keys: " +
                        // CollectionUtil.getFirst(catalog_const.getForeignkeycols()).getColumn());
                        // if (debug) System.err.println("catalog_fkey_tbl: " +
                        // catalog_fkey_tbl);
                        // if (debug) System.err.println("catalog_col: " +
                        // catalog_col);
                        // if (debug) System.err.println("catalog_fkey_col: " +
                        // catalog_fkey_col);
                        // if (debug) System.err.println("fkey_column_xrefs: " +
                        // fkey_column_xrefs.get(catalog_fkey_tbl));
                        fkey_column_xrefs.get(catalog_fkey_tbl).add(catalog_col, catalog_fkey_col, ExpressionType.COMPARE_EQUAL);
                    } else {
                        if (t) LOG.trace("\tSkipping Implict Reference Join for " + catalog_fkey_tbl + " because it is not used in " + this.catalog_proc);
                    }
                } else if (t) {
                    LOG.warn("\tNo foreign keys found for " + catalog_col.fullName());
                }
            } // FOR

            // Now for each table create an edge from our table to the table
            // referenced in the foreign
            // key using all the columdns referenced by in the SCAN predicates
            for (Table catalog_fkey_tbl : fkey_column_xrefs.keySet()) {
                DesignerVertex other_vertex = agraph.getVertex(catalog_fkey_tbl);
                PredicatePairs implicit_cset = fkey_column_xrefs.get(catalog_fkey_tbl);
                if (t)
                    LOG.trace("\t" + catalog_tbl + "->" + catalog_fkey_tbl + "\n" + implicit_cset.debug());
                Collection<DesignerEdge> edges = agraph.findEdgeSet(vertex, other_vertex);
                if (edges.isEmpty()) {
                    this.addEdge(agraph, AccessType.IMPLICIT_JOIN, implicit_cset, vertex, other_vertex, catalog_stmt0);
                    // Even though we don't need to create a new edge, we still
                    // need to know that
                    // we have found a reference that should be counted when we
                    // calculate weights
                } else {
                    // 08/25/2009 - Skip tables that already have an edge...
                    // if (!this.stmt_edge_xref.containsKey(catalog_stmt0)) {
                    // this.stmt_edge_xref.put(catalog_stmt0, new
                    // HashSet<Edge>());
                    // }
                    // this.stmt_edge_xref.get(catalog_stmt0).addAll(edges);
                    // if (d) LOG.debug("An edge already exists between " +
                    // catalog_tbl + " and " + catalog_fkey_tbl + ". " +
                    // "Skipping implicit join edge...");
                }
            } // FOR
        } // FOR
        return;
    }

    /**
     * @param access_type
     * @param cset
     * @param vertices
     * @param catalog_stmt
     */
    protected DesignerEdge addEdge(AccessGraph agraph, AccessType access_type, PredicatePairs cset, DesignerVertex v0, DesignerVertex v1, Statement... catalog_stmts) {

        // Sort the vertices by their CatalogTypes
        if (v0.getCatalogItem().compareTo(v1.getCatalogItem()) > 0) {
            DesignerVertex temp = v0;
            v0 = v1;
            v1 = temp;
        }

        String stmts_debug = "";
        if (d) {
            stmts_debug = "[";
            String add = "";
            for (Statement catalog_stmt : catalog_stmts) {
                stmts_debug += add + catalog_stmt.getName();
                add = ", ";
            } // FOR
            stmts_debug += "]";
        }

        // We need to first check whether we already have an edge representing
        // this ColumnSet
        DesignerEdge new_edge = null;
        for (DesignerEdge edge : agraph.getEdges()) {
            Collection<DesignerVertex> vertices = agraph.getIncidentVertices(edge);
            PredicatePairs other_cset = (PredicatePairs) edge.getAttribute(EdgeAttributes.COLUMNSET.name());
            if (vertices.contains(v0) && vertices.contains(v1) && cset.equals(other_cset)) {
                if (t)
                    LOG.trace("FOUND DUPLICATE COLUMN SET: " + other_cset.debug() + "\n" + cset.toString() + "\n[" + edge.hashCode() + "] + " + cset.size() + " == " + other_cset.size() + "\n");
                new_edge = edge;
                break;
            }
        } // FOR
        if (new_edge == null) {
            new_edge = new DesignerEdge(agraph);
            new_edge.setAttribute(EdgeAttributes.ACCESSTYPE.name(), access_type);
            new_edge.setAttribute(EdgeAttributes.COLUMNSET.name(), cset);
            agraph.addEdge(new_edge, v0, v1, EdgeType.UNDIRECTED);
            if (d)
                LOG.debug("New " + access_type + " edge for " + stmts_debug + " between " + v0 + "<->" + v1 + ": " + cset.debug());
        }
        // For edges created by implicitly for joins, we're not going to have a
        // Statement object
        if (catalog_stmts.length > 0) {
            for (Statement catalog_stmt : catalog_stmts) {
                if (!this.stmt_edge_xref.containsKey(catalog_stmt)) {
                    this.stmt_edge_xref.put(catalog_stmt, new HashSet<DesignerEdge>());
                }
                this.stmt_edge_xref.get(catalog_stmt).add(new_edge);
            } // FOR
        }
        if (d)
            LOG.debug("# OF EDGES: " + agraph.getEdgeCount());
        return (new_edge);
    }

    /**
     * @param agraph
     * @param xact
     * @param time
     *            TODO
     * @throws Exception
     */
    protected void updateEdgeWeights(AccessGraph agraph, TransactionTrace xact) throws Exception {
        int time = info.workload.getTimeInterval(xact, this.info.getNumIntervals());

        // Update the weights for the direct query-to-edge mappings
        for (QueryTrace query : xact.getQueries()) {
            Statement catalog_stmt = query.getCatalogItem(this.info.catalogContext.database);
            if (!this.stmt_edge_xref.containsKey(catalog_stmt)) {
                if (d)
                    LOG.warn("Missing query '" + catalog_stmt.fullName() + "' in Statement-Edge Xref mapping");
            } else {
                for (DesignerEdge edge : this.stmt_edge_xref.get(catalog_stmt)) {
                    edge.addToWeight(time, 1d);
                } // FOR
            }
        } // FOR

        // Update the weights for any edges that required multiple statements to
        // be
        // executed in order the edges to be updated
        Map<Statement, Integer> catalog_stmt_counts = xact.getStatementCounts(info.catalogContext.database);
        for (DesignerEdge edge : this.multi_stmt_edge_xref.keySet()) {
            // For each edge in our list, there is a list of Statements that
            // need to be executed
            // in order for the implicitly join to have occurred. Thus, we need
            // to check whether
            // this transaction executed all the Statements. We keep the minimum
            // count for each
            // of these Statements and will update the weight accordingly.
            for (Set<Statement> stmts : this.multi_stmt_edge_xref.get(edge)) {
                int min_cnt = Integer.MAX_VALUE;
                for (Statement stmt : stmts) {
                    min_cnt = Math.min(min_cnt, catalog_stmt_counts.get(stmt));
                } // FOR
                if (min_cnt > 0) {
                    edge.addToWeight(time, min_cnt);
                }
            } // FOR
        } // FOR
        return;
    }

    /**
     * @param agraph
     */
    protected void updateEdgeWeighModifiers(AccessGraph agraph) {
        for (DesignerEdge edge : agraph.getEdges()) {
            List<DesignerVertex> vertices = new ArrayList<DesignerVertex>(agraph.getIncidentVertices(edge));
            AccessGraph.AccessType type = (AccessGraph.AccessType) edge.getAttribute(AccessGraph.EdgeAttributes.ACCESSTYPE.name());

            // Modifier
            double modifier = 1.0d;

            // Join Modifier
            switch (type) {
                case IMPLICIT_JOIN:
                    // weight *= AccessGraph.WEIGHT_IMPLICIT;
                    break;
                case PARAM_JOIN:

                    break;
                case SQL_JOIN:

                    break;
            } // SWITCH

            // Foreign Key Modifier
            if (!(this.info.dgraph.findEdgeSet(vertices.get(0), vertices.get(1)).isEmpty() && this.info.dgraph.findEdgeSet(vertices.get(1), vertices.get(0)).isEmpty())) {
                modifier += AccessGraph.WEIGHT_FOREIGN_KEY;
                edge.setAttribute(AccessGraph.EdgeAttributes.FOREIGNKEY.name(), true);
            } else {
                edge.setAttribute(AccessGraph.EdgeAttributes.FOREIGNKEY.name(), false);
            }

            // Update all weights using our modifier
            for (int time = 0, cnt = edge.getIntervalCount(); time < cnt; time++) {
                double weight = edge.getWeight(time) * modifier;
                edge.setWeight(time, weight);
            } // FOR
        } // FOR
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD);
        DesignerInfo info = new DesignerInfo(args);
        boolean global = true;
        boolean single = false;

        if (global) {
            AccessGraph agraph = AccessGraphGenerator.generateGlobal(info);
            assert (agraph != null);
            if (single)
                agraph = AccessGraphGenerator.convertToSingleColumnEdges(args.catalog_db, agraph);
            agraph.setVerbose(true);
            GraphvizExport<DesignerVertex, DesignerEdge> gv = new GraphvizExport<DesignerVertex, DesignerEdge>(agraph);
            gv.setEdgeLabels(true);
            String path = "/tmp/" + args.catalog_type.toString().toLowerCase() + ".dot";
            FileUtil.writeStringToFile(path, gv.export(args.catalog_type.toString()));
            LOG.info("Wrote Graphviz file to " + path);

        } else {
            for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
                if (catalog_proc.getSystemproc())
                    continue;
                if (catalog_proc.getName().equals("neworder") == false)
                    continue;
                AccessGraphGenerator gen = new AccessGraphGenerator(info, catalog_proc);
                final AccessGraph agraph = new AccessGraph(args.catalog_db);
                gen.generate(agraph);
                agraph.setVerbose(true);

                GraphvizExport<DesignerVertex, DesignerEdge> gv = new GraphvizExport<DesignerVertex, DesignerEdge>(agraph);
                gv.setEdgeLabels(true);
                String path = "/tmp/" + catalog_proc.getName() + ".dot";
                FileUtil.writeStringToFile(path, gv.export(catalog_proc.getName()));
                LOG.info("Wrote Graphviz file to " + path);

                // javax.swing.SwingUtilities.invokeAndWait(new Runnable() {
                // public void run() {
                // GraphVisualizationPanel.createFrame(agraph).setVisible(true);
                // }
                // });
            } // FOR
        }
    }
}