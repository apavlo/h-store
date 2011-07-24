package org.voltdb.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class VerticalPartitionPlanner {
    private static final Logger LOG = Logger.getLogger(VerticalPartitionPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());

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
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures
     */
    public VerticalPartitionPlanner(Database catalogDb) {
        this.catalog_db = catalogDb;
        this.vp_views.putAll(CatalogUtil.getVerticallyPartitionedTables(catalogDb));
        for (Table catalog_tbl : this.vp_views.keySet()) {
            Collection<Column> columns = CatalogUtil.getColumns(this.vp_views.get(catalog_tbl).getGroupbycols());
            this.vp_columns.put(catalog_tbl, columns);
        } // FOR

        // Construct a DDL that includes the vertical partitions
        this.ddl = CatalogUtil.toSchema(catalog_db, false);

        // We will use this ProjectBuilder to generate new query plans
        this.projectBuilder = new VPPlannerProjectBuilder();
    }

    /**
     * @throws Exception
     */
    public Collection<Statement> optimizeDatabase() throws Exception {
        Set<Statement> updated = new HashSet<Statement>();

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                if (this.process(catalog_stmt)) {
                    StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
                    this.projectBuilder.queueRewrittenStatement(catalog_stmt, rewrite.sql);
                }
            } // FOR (stmts)
        } // FOR (procs)

        if (this.stmt_rewrites.size() > 0) {
            if (trace.get()) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                for (Statement catalog_stmt : this.stmt_rewrites.keySet()) {
                    m.put(catalog_stmt.fullName(), this.stmt_rewrites.get(catalog_stmt));
                }
                LOG.trace(String.format("Rewritten Queries [%d]\n%s", this.stmt_rewrites.size(), StringUtil.formatMaps(m)));
            }
            Map<Statement, Statement> optimized = this.projectBuilder.getRewrittenQueryPlans();
            assert (optimized != null);
            for (Entry<Statement, Statement> e : optimized.entrySet()) {
                this.applyOptimizedStatement(e.getValue(), e.getKey());
            } // FOR
            updated.addAll(optimized.keySet());
        }
        return (updated);
    }

    public boolean optimizeStatement(Statement catalog_stmt) throws Exception {
        this.projectBuilder.clear();
        if (this.process(catalog_stmt)) {
            StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
            this.projectBuilder.queueRewrittenStatement(catalog_stmt, rewrite.sql);
            Map<Statement, Statement> optimized = this.projectBuilder.getRewrittenQueryPlans();
            assert (optimized != null);
            assert (optimized.size() == 1);
            this.applyOptimizedStatement(CollectionUtil.getFirst(optimized.values()), catalog_stmt);
            return (true);
        }
        return (false);
    }

    private void applyOptimizedStatement(Statement copy_src, Statement copy_dest) {
        // Update both the single and multi-partition query plans
        assert(copy_src.getHas_multisited());
        copy_dest.setMs_fullplan(copy_src.getMs_fullplan());
        copy_dest.setMs_exptree(copy_src.getMs_exptree());
        copy_dest.getMs_fragments().clear();
        for (PlanFragment copy_src_frag : copy_src.getMs_fragments()) {
            PlanFragment copy_dest_frag = copy_dest.getMs_fragments().add(copy_src_frag.getName());
            for (String f : copy_src_frag.getFields()) {
                Object val = copy_src_frag.getField(f);
                if (val != null) {
                    if (val instanceof String) val = "\"" + val + "\""; // HACK
                    if (trace.get()) LOG.trace(String.format("Applied DTXN %s.%s => %s", copy_dest_frag.fullName(), f, val));
                    copy_dest_frag.set(f, val.toString());
                } else {
                    if (debug.get()) LOG.warn(String.format("Missing DTXN %s.%s", copy_dest_frag.fullName(), f));
                }
            } // FOR
        } // FOR

        assert(copy_src.getHas_singlesited());
        copy_dest.setFullplan(copy_src.getMs_fullplan());
        copy_dest.setExptree(copy_src.getMs_exptree());
        copy_dest.getFragments().clear();
        for (PlanFragment copy_src_frag : copy_src.getFragments()) {
            PlanFragment copy_dest_frag = copy_dest.getFragments().add(copy_src_frag.getName());
            for (String f : copy_src_frag.getFields()) {
                Object val = copy_src_frag.getField(f);
                if (val != null) {
                    if (val instanceof String) val = "\"" + val + "\""; // HACK
                    if (trace.get()) LOG.trace(String.format("Applied SP %s.%s => %s", copy_dest_frag.fullName(), f, val));
                    copy_dest_frag.set(f, val.toString());
                } else {
                    if (debug.get()) LOG.warn(String.format("Missing SP %s.%s", copy_dest_frag.fullName(), f));
                }
            } // FOR
        } // FOR
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
            if (debug.get())
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": There are no vertically partitioned tables.");
            return (false);
        }

        // We can only work our magic on SELECTs
        QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
        if (qtype != QueryType.SELECT) {
            if (debug.get())
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": QueryType is " + qtype + ".");
            return (false);
        }

        // Check whether this query references a table that has a vertical
        // partition
        Collection<Table> tables = CollectionUtils.intersection(this.vp_views.keySet(), CatalogUtil.getReferencedTables(catalog_stmt));
        if (tables.isEmpty()) {
            if (debug.get())
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": It does not reference a vertical partitioning table.");
            return (false);
        }

        // Now check whether the columns referenced doesn't include what the
        // table is horizontally partitioned
        // but do include the columns that we have in our vertical partition
        Collection<Column> predicate_cols = CatalogUtil.getReferencedColumns(catalog_stmt);
        if (predicate_cols.isEmpty()) {
            if (debug.get())
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not reference any columns in its predicate.");
            return (false);
        }
        Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
        assert (output_cols.isEmpty() == false);
        for (Table catalog_tbl : tables) {
            MaterializedViewInfo catalog_view = this.vp_views.get(catalog_tbl);
            assert (catalog_view != null);
            Collection<Column> view_cols = CatalogUtil.getColumns(catalog_view.getGroupbycols());
            assert (view_cols.isEmpty() == false);
            Column partitioning_col = catalog_tbl.getPartitioncolumn();
            assert (partitioning_col != null);

            // The current vertical partition is valid for this query if all the
            // following are true:
            // (1) The partitioning_col is in output_cols
            // (2) The partitioning_col is *not* in the predicate_cols
            // (3) At least one of the vertical partition's columns is in
            // predicate_cols
            if (trace.get()) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("VerticalP", catalog_view.getName());
                m.put("Partitioning Col", partitioning_col.fullName());
                m.put("Output Cols", output_cols);
                m.put("Predicate Cols", predicate_cols);
                m.put("VerticalP Cols", view_cols);
                LOG.trace(String.format("Checking whether %s can use vertical partition for %s\n%s", catalog_stmt.fullName(), catalog_tbl.getName(), StringUtil.formatMaps(m)));
            }
            if (output_cols.contains(partitioning_col) && predicate_cols.contains(partitioning_col) == false && CollectionUtils.intersection(view_cols, predicate_cols).isEmpty() == false) {
                if (trace.get())
                    LOG.trace("Valid VP Candidate: " + catalog_tbl);

                StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
                if (rewrite == null) {
                    rewrite = new StatementRewrite();
                    this.stmt_rewrites.put(catalog_stmt, rewrite);
                }
                rewrite.put(catalog_tbl, catalog_view.getDest());
            }
        } // FOR
        StatementRewrite rewrite = this.stmt_rewrites.get(catalog_stmt);
        if (rewrite == null) {
            if (debug.get())
                LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not have any valid vertical partitioning references.");
            return (false);
        }
        rewrite.sql = this.rewriteSQL(catalog_stmt, rewrite);

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
        if (debug.get())
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
        Map<String, String> from_xref = new ListOrderedMap<String, String>();
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

        public VPPlannerProjectBuilder() {
            super("vpplanner", VPPlannerProjectBuilder.class, null, null);
            this.setDDLContents(VerticalPartitionPlanner.this.ddl);

            // Add in all the table partitioning info
            for (Table catalog_tbl : catalog_db.getTables()) {
                if (catalog_tbl.getSystable() || catalog_tbl.getIsreplicated())
                    continue;
                this.addPartitionInfo(catalog_tbl.getName(), catalog_tbl.getPartitioncolumn().getName());
            } // FOR
        }

        @Override
        public String getJarDirectory() {
            return FileUtil.getTempDirectory().getAbsolutePath();
        }

        public void clear() {
            this.rewritten_queries.clear();
        }

        protected Map<Statement, Statement> getRewrittenQueryPlans() throws Exception {
            if (debug.get())
                LOG.debug(String.format("Generating new query plans for %d rewritten Statements", this.rewritten_queries.size()));

            // Build the catalog
            Catalog catalog = this.createCatalog();
            assert (catalog != null);
            Database catalog_db = CatalogUtil.getDatabase(catalog);

            Map<Statement, Statement> ret = new HashMap<Statement, Statement>();
            for (Statement catalog_stmt : this.rewritten_queries.keySet()) {
                String procName = this.rewritten_queries.get(catalog_stmt);
                assert (procName != null);

                Procedure new_catalog_proc = catalog_db.getProcedures().get(procName);
                assert (new_catalog_proc != null);
                Statement new_catalog_stmt = CollectionUtil.getFirst(new_catalog_proc.getStatements());
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
            if (debug.get())
                LOG.debug(String.format("Queued %s to generate new query plan", catalog_stmt.fullName()));
        }
    }

}
