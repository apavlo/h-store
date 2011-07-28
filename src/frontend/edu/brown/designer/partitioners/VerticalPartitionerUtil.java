package edu.brown.designer.partitioners;

import java.util.Collection;
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
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.plannodes.PlanNodeUtil;
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
     * 
     * @param catalog_db
     * @param candidates
     * @throws Exception
     */
    public static void compileOptimizedStatements(Database catalog_db, Collection<VerticalPartitionColumn> candidates) throws Exception {
        if (debug.get()) LOG.debug("Computing vertical partition query plans for " + candidates.size() + " candidates");
        for (VerticalPartitionColumn c : candidates) {
            Table catalog_tbl = c.getParent();
            assert(CatalogUtil.getVerticalPartition(catalog_tbl) == null);
            List<String> columnNames = c.getVerticalPartitionColumnNames();
            VoltCompiler.addVerticalPartition(catalog_db, catalog_tbl.getName(), columnNames, true);
            VerticalPartitionPlanner vp_planner = new VerticalPartitionPlanner(catalog_db);
            Map<Statement, Statement> optimized = null;
            try {
                optimized = vp_planner.generateOptimizedStatements();
            } catch (Throwable ex) {
                LOG.error("Failed to generate optimized query plans:\n" + c);
                throw new Exception(ex);
            }
            c.addOptimizedQueries(optimized);
            
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
    public static Collection<VerticalPartitionColumn> generateCandidates(final DesignerInfo info, final AccessGraph agraph, final Column hp_col, final DesignerHints hints) throws Exception {
        if (trace.get()) LOG.trace(String.format("Generating Vertical Partitioning candidates based on using %s as the horizontal partitioning attribute",
                                                 hp_col.fullName()));
        final Table catalog_tbl = hp_col.getParent();
        Set<VerticalPartitionColumn> candidates = new ListOrderedSet<VerticalPartitionColumn>();
        
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
                if (hp_col instanceof MultiColumn) {
                    if (output_cols.containsAll((MultiColumn)hp_col) == false) continue;
                } else if (output_cols.contains(hp_col) == false) continue;
                
                // Skip if this thing is just dumping out all columns
                if (output_cols.size() == catalog_tbl.getColumns().size()) continue;
                
                // The referenced columns are the columns that are used in the predicate
                Collection<Column> predicate_cols = CatalogUtil.getReferencedColumns(catalog_stmt);
                if (predicate_cols.contains(hp_col)) continue;
                
                // We only support single-table queries now
                Collection<Table> stmt_tables = CatalogUtil.getReferencedTables(catalog_stmt);
                if (stmt_tables.size() > 1) continue;
                
                // Vertical Partition Columns
                Set<Column> all_cols = new TreeSet<Column>();
                all_cols.addAll(predicate_cols);
                if (hp_col instanceof MultiColumn) {
                    all_cols.addAll(((MultiColumn)hp_col).getAttributes());    
                } else {
                    all_cols.add(hp_col);
                }
                
                MultiColumn vp_col = MultiColumn.get(all_cols.toArray(new Column[all_cols.size()]));
                VerticalPartitionColumn vpc = VerticalPartitionColumn.get(hp_col, vp_col);
                assert(vpc != null) : String.format("Failed to get VerticalPartition column for <%s, %s>", hp_col, vp_col);
                vpc.getStatements().add(catalog_stmt);
                candidates.add(vpc);
                
                if (trace.get()) {
                    Map<String, Object> m = new ListOrderedMap<String, Object>();
                    m.put("Output Columns", output_cols);
                    m.put("Predicate Columns", predicate_cols);
                    m.put("Horizontal Partitioning", hp_col.fullName());
                    m.put("Vertical Partitioning", vp_col.fullName());
                    LOG.trace(catalog_stmt.fullName() + "\n" + StringUtil.formatMaps(m));
                }
            } // FOR (stmt)
        } // FOR (proc)
        
        return (candidates);
        
    }
}
