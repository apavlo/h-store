package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.planner.VerticalPartitionPlanner;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public abstract class VerticalPartitionerUtil {
    private static final Logger LOG = Logger.getLogger(VerticalPartitionerUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * A Candidate is a set of columns that represent a potential vertical partition
     *
     */
    public static class Candidate extends ListOrderedSet<Column> {
        private static final long serialVersionUID = 1L;
        
        private final Collection<Statement> catalog_stmts = new TreeSet<Statement>();
        private final Map<Statement, Statement> optimized = new HashMap<Statement, Statement>();
        
        public Candidate(Set<Column> cols) {
            super(cols);
        }
        public Set<Column> getColumns() {
            return (this);
        }
        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<String>();
            for (Column catalog_col : this) {
                columnNames.add(catalog_col.getName());
            } // FOR
            return (columnNames);
        }
        public Table getTable() {
            return (CollectionUtil.getFirst(this).getParent());
        }
        public Collection<Statement> getStatements() {
            return (this.catalog_stmts);
        }
        public Statement getOptimizedQuery(Statement catalog_stmt) {
            return (this.optimized.get(catalog_stmt));
        }
        
        @Override
        public String toString() {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put("VP Columns", super.toString());
            m.put("Statements", CatalogUtil.debug(this.catalog_stmts));
            m.put("Optimized Queries", StringUtil.join("\n", this.optimized.entrySet()));
            return StringUtil.formatMaps(m);
        }
    }
    
    /**
     * 
     * @param catalog_db
     * @param candidates
     * @throws Exception
     */
    public static void compileOptimizedStatements(Database catalog_db, Collection<Candidate> candidates) throws Exception {
        if (debug.get()) LOG.debug("Computing vertical partition query plans for " + candidates.size() + " candidates");
        for (Candidate c : candidates) {
            Table catalog_tbl = c.getTable();
            assert(CatalogUtil.getVerticalPartition(catalog_tbl) == null);
            List<String> columnNames = c.getColumnNames();
            VoltCompiler.addVerticalPartition(catalog_db, catalog_tbl.getName(), columnNames, true);
            VerticalPartitionPlanner vp_planner = new VerticalPartitionPlanner(catalog_db);
            Map<Statement, Statement> optimized = null;
            try {
                optimized = vp_planner.generateOptimizedStatements();
            } catch (Throwable ex) {
                LOG.error("Failed to generate optimized query plans:\n" + c);
                throw new Exception(ex);
            }
            c.optimized.putAll(optimized);
            
            if (debug.get()) LOG.info(String.format("Generated %d optimized query plans using %s's vertical partition: %s",
                                                    optimized.size(), catalog_tbl.getName(), columnNames));
            
            // Always remove the vertical partition
            MaterializedViewInfo catalog_view = CatalogUtil.getVerticalPartition(catalog_tbl);
            assert(catalog_view != null);
            catalog_tbl.getViews().remove(catalog_view);
            catalog_db.getTables().remove(catalog_view.getDest());
        } // FOR
        
    }
    
    
    /**
     * 
     * @param info
     * @param agraph
     * @param catalog_tbl
     * @param hints
     * @return
     * @throws Exception
     */
    public static Collection<Candidate> generateCandidates(final DesignerInfo info, final AccessGraph agraph, final Column catalog_col, final DesignerHints hints) throws Exception {
        if (trace.get()) LOG.trace(String.format("Generating Vertical Partitioning candidates based on using %s as the partitioning attribute",
                                                 catalog_col.fullName()));
        final Table catalog_tbl = catalog_col.getParent();
        Map<Set<Column>, Candidate> candidates = new HashMap<Set<Column>, Candidate>();
        
        // For the given Column object, figure out what are the potential vertical partitioning candidates
        // if we assume that the Table is partitioned on that Column
        for (Procedure catalog_proc : CatalogUtil.getReferencingProcedures(catalog_tbl)) {
            
            // Look for a query on this table that does not use the target column in the predicate
            // But does return it in its output
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                // We can only look at SELECT statements because we have know way to know the correspondence
                // between the candidate partitioning column and our target column
                if (catalog_stmt.getQuerytype() != QueryType.SELECT.getValue()) continue;
                Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
                if (output_cols.contains(catalog_col) == false) continue;
                // Skip if this thing is just dumping out all columns
                if (output_cols.size() == catalog_tbl.getColumns().size()) continue;
                
                // The referenced columns are the columns that are used in the predicate
                Collection<Column> predicate_cols = CatalogUtil.getReferencedColumns(catalog_stmt);
                if (predicate_cols.contains(catalog_col)) continue;
                
                // Vertical Partition Columns
                Set<Column> vp_cols = new TreeSet<Column>();
                vp_cols.addAll(predicate_cols);
                vp_cols.add(catalog_col);
                
                // Check whether we already have a candidate with these columns
                Candidate vpc = candidates.get(vp_cols);
                if (vpc == null) {
                    vpc = new Candidate(vp_cols);
                    candidates.put(vp_cols, vpc);
                }
                vpc.getStatements().add(catalog_stmt);
                if (debug.get()) LOG.debug(String.format("%s: Output%s All%s\n%s",
                                                         catalog_stmt.fullName(), output_cols, predicate_cols, vpc));
            } // FOR (stmt)
        } // FOR (proc)
        
        return (candidates.values());
        
    }
}
