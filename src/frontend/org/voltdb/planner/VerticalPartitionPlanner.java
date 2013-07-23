package org.voltdb.planner;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.types.QueryType;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

public class VerticalPartitionPlanner {
    private static final Logger LOG = Logger.getLogger(VerticalPartitionPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final Pattern SELECT_REGEX = Pattern.compile("SELECT[\\s]+(.*?)[\\s]+FROM[\\s]+(.*?)[\\s]+WHERE[\\s]+(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_SPLIT = Pattern.compile(",");
    private static final Pattern FROM_REGEX = Pattern.compile("[\\s]+AS[\\s]+", Pattern.CASE_INSENSITIVE);

    // ======================================================================================
    // INTERNAL DATA MEMBERS
    // ======================================================================================

    /** convenience pointer to the database object in the catalog */
    private final Database catalog_db;

    private final Map<Table, MaterializedViewInfo> vp_views = new HashMap<Table, MaterializedViewInfo>();
    private final Map<Table, Collection<Column>> vp_columns = new HashMap<Table, Collection<Column>>();

    /** New schema DDL that contains all of the vertical partitions */
    private final String ddl;

    /**
     * For each Statement, maintain a mapping of the tables to rewrite, as well
     * as the new rewritten SQL
     */
    private class StatementRewrite extends HashMap<Table, Table> {
        private static final long serialVersionUID = 1L;
        private String sql;
    }

    private final Map<Statement, StatementRewrite> stmt_rewrites = new HashMap<Statement, StatementRewrite>();

    /**
     * Internal ProjectBuilder that we'll use to get new query plans that use
     * the Vertical Partitions
     */
    private final VPPlannerProjectBuilder projectBuilder;

    // ======================================================================================
    // CONSTRUCTOR
    // ======================================================================================

    /**
     * @param catalog_db
     *            Catalog info about schema, metadata and procedures
     */
    public VerticalPartitionPlanner(Database catalog_db, MaterializedViewInfo...catalog_views) {
        this.catalog_db = catalog_db;
        // Construct a DDL that includes the vertical partitions
        this.ddl = CatalogUtil.toSchema(catalog_db, false);

        // We will use this ProjectBuilder to generate new query plans
        this.projectBuilder = new VPPlannerProjectBuilder();
        
        for (MaterializedViewInfo catalog_view : catalog_views) {
            this.addVerticalPartition(catalog_view);
        } // FOR
    }
    
    public VerticalPartitionPlanner(Database catalog_db, boolean addAll) {
        this(catalog_db);
        if (addAll) this.addAllVerticalPartitions();
    }
    
    public VerticalPartitionPlanner addAllVerticalPartitions() {
        for (MaterializedViewInfo catalog_view : CatalogUtil.getVerticallyPartitionedTables(catalog_db).values()) {
            this.addVerticalPartition(catalog_view);
        }
        return (this);
    }
    
    public VerticalPartitionPlanner addVerticalPartition(MaterializedViewInfo catalog_view) {
        if (debug.val)
            LOG.debug("Adding " + catalog_view + " for discovering query optimizations");
        Table catalog_tbl = catalog_view.getParent();
        this.vp_views.put(catalog_tbl, catalog_view);
        
        Collection<Column> columns = CatalogUtil.getColumns(catalog_view.getGroupbycols());
        assert(columns.isEmpty() == false) : "No vertical partition columns for " + catalog_tbl;
        this.vp_columns.put(catalog_tbl, columns);
        
        return (this);
    }
    

    // ======================================================================================
    // MAIN ENTRY POINTS
    // ======================================================================================
    
    /**
     * Generate all the optimized query plans for all Statements in the database
     * and apply them to the catalog immediately 
     * @throws Exception
     */
    public Collection<Statement> optimizeDatabase() throws Exception {
        Set<Statement> updated = new HashSet<Statement>();
        Map<Statement, Statement> optimized = this.generateOptimizedStatements();
        if (optimized != null) {
            for (Entry<Statement, Statement> e : optimized.entrySet()) {
                applyOptimization(e.getValue(), e.getKey());
            } // FOR
            updated.addAll(optimized.keySet());
        }
        return (updated);
    }

    /**
     * Generate an optimized query plan for just one Statement and apply
     * it to the catalog immediately
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public boolean optimizeStatement(Statement catalog_stmt) throws Exception {
        this.projectBuilder.clear();
        if (this.process(catalog_stmt)) {
            StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
            this.projectBuilder.queueRewrittenStatement(catalog_stmt, rewrite.sql);
            Map<Statement, Statement> optimized = this.projectBuilder.getRewrittenQueryPlans();
            assert (optimized != null);
            assert (optimized.size() == 1);
            applyOptimization(CollectionUtil.first(optimized.values()), catalog_stmt);
            return (true);
        }
        return (false);
    }
    
    public Map<Statement, Statement> generateOptimizedStatements() throws Exception {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                if (this.process(catalog_stmt)) {
                    StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
                    this.projectBuilder.queueRewrittenStatement(catalog_stmt, rewrite.sql);
                }
            } // FOR (stmts)
        } // FOR (procs)

        if (this.stmt_rewrites.size() > 0) {
            if (trace.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                for (Statement catalog_stmt : this.stmt_rewrites.keySet()) {
                    m.put(catalog_stmt.fullName(), this.stmt_rewrites.get(catalog_stmt));
                }
                LOG.trace(String.format("Rewritten Queries [%d]\n%s", this.stmt_rewrites.size(), StringUtil.formatMaps(m)));
            }
            return (this.projectBuilder.getRewrittenQueryPlans());
        }
        return (null);
    }
    
    // ======================================================================================
    // INTERNAL UTILITY METHODS
    // ======================================================================================

    public static void applyOptimization(Statement src, Statement dest) {
        CatalogUtil.copyQueryPlans(src, dest);
        dest.setSecondaryindex(true);
        
        // Check whether the only table the query references is our replicated index
        Collection<Table> tables = CatalogUtil.getReferencedTables(dest);
        if (debug.val) LOG.debug(dest + " => " + tables);
        dest.setReplicatedonly(tables.size() == 1);
    }
    
    /**
     * Process the given Statement and rewrite its query plan if it can take
     * advantage of a vertical partitioning column
     * 
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    protected boolean process(Statement catalog_stmt) throws Exception {
        // Always skip if there are no vertically partitioned tables
        if (this.vp_views.isEmpty()) {
            if (debug.val)
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": There are no vertically partitioned tables.");
            return (false);
        }

        // We can only work our magic on SELECTs
        QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
        if (qtype != QueryType.SELECT) {
            if (debug.val)
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": QueryType is " + qtype + ".");
            return (false);
        }

        // Check whether this query references a table that has a vertical partition
        Collection<Table> tables = CollectionUtils.intersection(this.vp_views.keySet(), CatalogUtil.getReferencedTables(catalog_stmt));
        if (tables.isEmpty()) {
            if (debug.val)
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": It does not reference a vertical partitioning table.");
            return (false);
        }

        // Now check whether the columns referenced doesn't include what the
        // table is horizontally partitioned but do include the columns that
        // we have in our vertical partition
        Collection<Column> stmt_cols = CollectionUtils.union(CatalogUtil.getReferencedColumns(catalog_stmt),
                                                             CatalogUtil.getOrderByColumns(catalog_stmt));
        if (stmt_cols.isEmpty()) {
            if (debug.val)
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not reference any columns in its predicate or order-by clause.");
            return (false);
        }
        Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
        assert (output_cols.isEmpty() == false);
        for (Table catalog_tbl : tables) {
            MaterializedViewInfo catalog_view = this.vp_views.get(catalog_tbl);
            assert(catalog_view != null);
            assert(catalog_view.getGroupbycols().isEmpty() == false) :
                String.format("Missing vertical partitioning columns in %s when trying to process %s\n%s\nCACHED: %s",
                              catalog_view.fullName(), catalog_stmt.fullName(), CatalogUtil.debug(catalog_view), CatalogUtil.debug(this.vp_columns.get(catalog_tbl)));
            Collection<Column> view_cols = CatalogUtil.getColumns(catalog_view.getGroupbycols());
            assert(view_cols.isEmpty() == false) : "Missing vertical partitioning columns in " + catalog_view.fullName() + " when trying to process " + catalog_stmt.fullName();
            Column partitioning_col = catalog_tbl.getPartitioncolumn();
            assert(partitioning_col != null);

            // The current vertical partition is valid for this query if all the
            // following are true:
            // (1) The partitioning_col is in output_cols
            // (2) The partitioning_col is *not* in the predicate_cols
            // (3) At least one of the vertical partition's columns is in
            // predicate_cols
            if (debug.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                m.put("VerticalP", catalog_view.getName());
                m.put("Partitioning Col", partitioning_col.fullName());
                m.put("Output Cols", output_cols);
                m.put("Statement Cols", stmt_cols);
                m.put("VerticalP Cols", view_cols);
                LOG.debug(String.format("Checking whether %s can use vertical partition for %s\n%s", catalog_stmt.fullName(), catalog_tbl.getName(), StringUtil.formatMaps(m)));
            }
//            if (output_cols.contains(partitioning_col) == false) {
//                if (debug.val)
//                    LOG.warn("Output Columns do not contain horizontal partitioning column");
//            }
//            else 
            if (view_cols.containsAll(output_cols) == false) {
                if (debug.val)
                    LOG.warn("Vertical Partition columns do not contain output columns");
            }
            /** Is this needed?
            else if (stmt_cols.contains(partitioning_col) == true) {
                if (debug.val)
                    LOG.warn("Statement Columns contains horizontal partition column");
            }
            **/
            else if (view_cols.containsAll(stmt_cols) == false) {
                if (debug.val)
                    LOG.warn("The Vertical Partition Columns does not contain all of the Statement Columns " + CollectionUtils.subtract(view_cols, stmt_cols));
            }
            else {
                if (debug.val)
                    LOG.debug("Valid VP Candidate: " + catalog_tbl);
                StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
                if (rewrite == null) {
                    rewrite = new StatementRewrite();
                    this.stmt_rewrites.put(catalog_stmt, rewrite);
                }
                rewrite.put(catalog_tbl, catalog_view.getDest());
            }
        } // FOR
        
        // Check to make sure that we were able to generate a StatementRewrite candidate for this one
        StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
        if (rewrite == null) {
            if (debug.val)
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not have any valid vertical partitioning references.");
            return (false);
        }
        try {
            rewrite.sql = this.rewriteSQL(catalog_stmt, rewrite);
        } catch (Throwable ex) {
            String msg = String.format("Failed to rewrite SQL for %s to use replicated secondary index on %s",
                                       catalog_stmt.fullName(), CatalogUtil.debug(rewrite.keySet()));
            LOG.warn(msg, (debug.val ? ex : null));
            return (false);
        }

        return (true);
    }

    /**
     * Hackishly rewrite the SQL statement to change all tables references to
     * vertical partitions based on the provided table mapping
     * 
     * @param catalog_stmt
     * @param tbl_mapping
     * @return
     * @throws Exception
     */
    protected String rewriteSQL(Statement catalog_stmt, Map<Table, Table> tbl_mapping) throws Exception {
        if (debug.val)
            LOG.debug(String.format("Rewriting %s's SQL using %d mappings: %s", catalog_stmt.fullName(), tbl_mapping.size(), tbl_mapping));
        // This isn't perfect but it's good enough for our experiments
        Matcher m = SELECT_REGEX.matcher(catalog_stmt.getSqltext());
        if (m.matches() == false) {
            throw new Exception(String.format("Failed to match %s's SQL: %s", catalog_stmt.fullName(), catalog_stmt.getSqltext()));
        }
        String select_clause = m.group(1);
        String where_clause = m.group(3);
        String from_clause[] = FROM_SPLIT.split(m.group(2));
        if (from_clause.length == 0) {
            throw new Exception(String.format("Failed to extract %s's FROM clause: %s", catalog_stmt.fullName(), catalog_stmt.getSqltext()));
        }
        Map<String, String> from_xref = new LinkedHashMap<String, String>();
        Map<String, String> new_from_xref = new HashMap<String, String>();
        for (String from : from_clause) {
            String split[] = FROM_REGEX.split(from.trim());
            from_xref.put(split[0], (split.length > 1 ? split[1] : null));
        } // FROM

        for (Entry<Table, Table> e : tbl_mapping.entrySet()) {
            String fromTable = e.getKey().getName();
            String toTable = e.getValue().getName();

            select_clause = select_clause.replace(fromTable + ".", toTable + ".");
            where_clause = where_clause.replace(fromTable + ".", toTable + ".");
            new_from_xref.put(fromTable, toTable);
        } // FOR

        String new_from[] = new String[from_xref.size()];
        int i = 0;
        for (String tableName : from_xref.keySet()) {
            String new_tableName = new_from_xref.get(tableName);
            if (new_tableName == null)
                new_tableName = tableName;
            new_from[i] = new_tableName;
            if (from_xref.get(tableName) != null)
                new_from[i] += " AS " + from_xref.get(tableName);
            i++;
        } // FOR

        return (String.format("SELECT %s FROM %s WHERE %s", select_clause, StringUtil.join(",", new_from), where_clause));
    }

    /**
     * VPPlannerProjectBuilder We will queue up a bunch of single-query
     * Procedures and then use the VoltDB planner to generate new query plans
     * that use the vertical partitions
     */
    protected class VPPlannerProjectBuilder extends AbstractProjectBuilder {

        private final Map<Statement, String> rewritten_queries = new HashMap<Statement, String>();
        private final File tempDDL;

        public VPPlannerProjectBuilder() {
            super("vpplanner", VPPlannerProjectBuilder.class, null, null);
            this.tempDDL = this.setDDLContents(VerticalPartitionPlanner.this.ddl);

            // Add in all the table partitioning info
            for (Table catalog_tbl : CatalogUtil.getDataTables(catalog_db)) {
                if (catalog_tbl.getIsreplicated()) continue;
                this.addTablePartitionInfo(catalog_tbl, catalog_tbl.getPartitioncolumn());
            } // FOR
            
            // Make sure that we disable VP optimizations otherwise we will get stuck
            // in an infinite loop
            this.enableReplicatedSecondaryIndexes(false);
            
            // Make sure we initialize the StatementCompiler's PlanFragment counter
            // so that we don't get overlapping PlanFragment ids
            StatementCompiler.getNextFragmentId(catalog_db);
        }

        @Override
        public String getJarDirectory() {
            return FileUtil.getTempDirectory().getAbsolutePath();
        }

        public void clear() {
            this.rewritten_queries.clear();
        }

        protected Map<Statement, Statement> getRewrittenQueryPlans() throws Exception {
            if (debug.val) {
                String temp = "";
                for (String procName : this.rewritten_queries.values())
                    temp += String.format("\n%s: %s", procName, this.getStmtProcedureSQL(procName));
                LOG.debug(String.format("Generating new query plans for %d rewritten Statements%s", this.rewritten_queries.size(), temp));
            }

            // Build the catalog
            Catalog catalog = null;
            try {
                catalog = this.createCatalog();
            } catch (Throwable ex) {
                LOG.error("Failed to build compile vertical partitioning catalog");
                LOG.error("Busted DDL:\n" + VerticalPartitionPlanner.this.ddl);
                throw new Exception(ex);
            } finally {
                this.tempDDL.delete();
            }
            assert (catalog != null);
            Database catalog_db = CatalogUtil.getDatabase(catalog);

            Map<Statement, Statement> ret = new HashMap<Statement, Statement>();
            for (Statement catalog_stmt : this.rewritten_queries.keySet()) {
                String procName = this.rewritten_queries.get(catalog_stmt);
                assert (procName != null);

                Procedure new_catalog_proc = catalog_db.getProcedures().get(procName);
                assert (new_catalog_proc != null);
                Statement new_catalog_stmt = CollectionUtil.first(new_catalog_proc.getStatements());
                assert (new_catalog_stmt != null);
                ret.put(catalog_stmt, new_catalog_stmt);
            } // FOR

            // CatalogUtil.saveCatalog(catalog,
            // FileUtil.getTempFile("catalog").getAbsolutePath());

            return (ret);
        }

        protected void queueRewrittenStatement(Statement catalog_stmt, String sql) {
            String procName = String.format("%s_%s", catalog_stmt.getParent().getName(), catalog_stmt.getName());
            this.addStmtProcedure(procName, sql);
            this.rewritten_queries.put(catalog_stmt, procName);
            if (debug.val)
                LOG.debug(String.format("Queued %s to generate new query plan\n%s", catalog_stmt.fullName(), sql));
        }
    }

}
