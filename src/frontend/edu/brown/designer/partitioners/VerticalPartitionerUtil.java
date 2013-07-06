package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.planner.VerticalPartitionPlanner;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.designer.MemoryEstimator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.StringUtil;

public abstract class VerticalPartitionerUtil {
    private static final Logger LOG = Logger.getLogger(VerticalPartitionerUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * @param vp_col
     * @param stats
     * @return
     */
    public static TableStatistics computeTableStatistics(VerticalPartitionColumn vp_col, WorkloadStatistics stats) {
        MaterializedViewInfo catalog_view = vp_col.createMaterializedView();
        Table view_tbl = catalog_view.getDest();
        assert (view_tbl != null) : "Destination table for " + catalog_view + " is null?";
        TableStatistics tbl_stats = stats.getTableStatistics(view_tbl);
        if (tbl_stats == null) {
            tbl_stats = new TableStatistics(view_tbl);
            stats.addTableStatistics(view_tbl, tbl_stats);

            Table orig_tbl = catalog_view.getParent();
            TableStatistics orig_tbl_stats = stats.getTableStatistics(orig_tbl);
            assert (orig_tbl_stats != null) : "Missing TableStatistics " + orig_tbl;

            tbl_stats.readonly = true;
            tbl_stats.tuple_count_total = orig_tbl_stats.tuple_count_total;

            long tuple_size = MemoryEstimator.estimateTupleSize(view_tbl);
            tbl_stats.tuple_size_avg = tuple_size;
            tbl_stats.tuple_size_max = tuple_size;
            tbl_stats.tuple_size_min = tuple_size;
            tbl_stats.tuple_size_total = tbl_stats.tuple_count_total * tuple_size;

            if (debug.val)
                LOG.debug("Added TableStatistics for vertical partition replica table " + view_tbl);
        }
        return (tbl_stats);

    }

    /**
     * Generate all of the potential VerticalPartitionColumn candidates based on
     * the given horizontal partition column. Each VerticalPartitionColumn
     * candidate will contain the optimized queries that we compute with the
     * VerticalPartitionPlanner.
     * 
     * @param stats
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    public static Collection<VerticalPartitionColumn> generateCandidates(final Column partition_col, final WorkloadStatistics stats) throws Exception {
        final Table catalog_tbl = partition_col.getParent();
        final Database catalog_db = catalog_tbl.getParent();
        final Set<VerticalPartitionColumn> candidates = new ListOrderedSet<VerticalPartitionColumn>();

        // If the horizontal partition column is null, then there can't be any
        // vertical partition columns
        if (partition_col.getNullable()) {
            if (debug.val)
                LOG.warn("The horizontal partition column " + partition_col.fullName() + " is nullable. Skipping candidate generation");
            return (candidates);
        }

        // Get all the read-only columns for this table
        Collection<Column> readOnlyColumns = CatalogUtil.getReadOnlyColumns(catalog_tbl, true);

        // For the given Column object, figure out what are the potential
        // vertical partitioning candidates
        // if we assume that the Table is partitioned on that Column
        if (debug.val) {
            LOG.debug(String.format("Generating VerticalPartitionColumn candidates based on using %s as the horizontal partitioning attribute", partition_col.fullName()));
            LOG.trace(catalog_tbl + " Read-Only Columns: " + CatalogUtil.debug(readOnlyColumns));
        }
        for (Procedure catalog_proc : CatalogUtil.getReferencingProcedures(catalog_tbl)) {
            // Look for a query on this table that does not use the target
            // column in the predicate
            // But does return it in its output
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                // We can only look at SELECT statements because we have know
                // way to know the correspondence
                // between the candidate partitioning column and our target
                // column
                if (catalog_stmt.getQuerytype() != QueryType.SELECT.getValue())
                    continue;
                Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
                if (partition_col instanceof MultiColumn) {
                    if (output_cols.containsAll((MultiColumn) partition_col) == false)
                        continue;
                } else if (output_cols.contains(partition_col) == false)
                    continue;

                // Skip if this thing is just dumping out all columns
                if (output_cols.size() == catalog_tbl.getColumns().size())
                    continue;

                // We only support single-table queries now
                Collection<Table> stmt_tables = CatalogUtil.getReferencedTables(catalog_stmt);
                if (stmt_tables.size() > 1)
                    continue;

                // The referenced columns are the columns that are used in the
                // predicate and order bys
                Collection<Column> stmt_cols = CollectionUtils.union(CatalogUtil.getReferencedColumns(catalog_stmt), CatalogUtil.getOrderByColumns(catalog_stmt));
                if (stmt_cols.contains(partition_col))
                    continue;

                // Vertical Partition Columns
                Set<Column> all_cols = new TreeSet<Column>();
                all_cols.addAll(stmt_cols);
                if (partition_col instanceof MultiColumn) {
                    all_cols.addAll(((MultiColumn) partition_col).getAttributes());
                } else {
                    all_cols.add(partition_col);
                }

                // Include any read-only output columns
                for (Column col : output_cols) {
                    if (readOnlyColumns.contains(col))
                        all_cols.add(col);
                } // FOR

                if (partition_col instanceof MultiColumn) {
                    MultiColumn mc = (MultiColumn) partition_col;
                    if (mc.size() == all_cols.size()) {
                        boolean foundAll = true;
                        for (Column col : mc) {
                            foundAll = all_cols.contains(col) && foundAll;
                        } // FOR
                        if (foundAll)
                            continue;
                        // assert(foundAll) : mc + "\n" + all_cols;
                    }
                }
                
                if (all_cols.size() > 1) {
                    MultiColumn vp_col = MultiColumn.get(all_cols.toArray(new Column[all_cols.size()]));
                    assert (partition_col.equals(vp_col) == false) : vp_col;
                    VerticalPartitionColumn vpc = VerticalPartitionColumn.get(partition_col, vp_col);
                    assert (vpc != null) : String.format("Failed to get VerticalPartition column for <%s, %s>", partition_col, vp_col);
                    candidates.add(vpc);
    
                    if (debug.val) {
                        Map<String, Object> m = new ListOrderedMap<String, Object>();
                        m.put("Output Columns", output_cols);
                        m.put("Predicate Columns", stmt_cols);
                        m.put("Horizontal Partitioning", partition_col.fullName());
                        m.put("Vertical Partitioning", vp_col.fullName());
                        LOG.debug("Vertical Partition Candidate: " + catalog_stmt.fullName() + "\n" + StringUtil.formatMaps(m));
                    }
                }
            } // FOR (stmt)
        } // FOR (proc)

        if (debug.val && candidates.size() > 0)
            LOG.debug("Computing vertical partition query plans for " + candidates.size() + " candidates");
        Set<VerticalPartitionColumn> final_candidates = new HashSet<VerticalPartitionColumn>();
        for (VerticalPartitionColumn vpc : candidates) {
            // Make sure our WorkloadStatistics have something for this
            // MaterializedViewInfo
            if (stats != null)
                VerticalPartitionerUtil.computeTableStatistics(vpc, stats);

            if (vpc.hasOptimizedQueries()) {
                if (debug.val)
                    LOG.debug("Skipping candidate that already has optimized queries\n" + vpc.toString());
                final_candidates.add(vpc);
            } else if (generateOptimizedQueries(catalog_db, vpc)) {
                final_candidates.add(vpc);
            }
        } // FOR

        return (final_candidates);

    }

    /**
     * @param catalog_db
     * @param c
     * @return
     */
    public static boolean generateOptimizedQueries(Database catalog_db, VerticalPartitionColumn c) {
        boolean ret = false;
        MaterializedViewInfo catalog_view = CatalogUtil.getVerticalPartition((Table) c.getParent());
        if (catalog_view == null)
            catalog_view = c.createMaterializedView();
        assert (catalog_view != null);
        assert (catalog_view.getGroupbycols().isEmpty() == false) : String.format("Missing columns for VerticalPartition view %s\n%s", catalog_view.fullName(), c);

        List<String> columnNames = c.getVerticalPartitionColumnNames();
        VerticalPartitionPlanner vp_planner = new VerticalPartitionPlanner(catalog_db, catalog_view);
        Map<Statement, Statement> optimized = null;
        try {
            optimized = vp_planner.generateOptimizedStatements();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to generate optimized query plans:\n" + c, ex);
        }
        if (optimized != null) {
            c.addOptimizedQueries(optimized);
            ret = true;
            if (debug.val)
                LOG.debug(String.format("Generated %d optimized query plans using %s's vertical partition: %s", optimized.size(), c.getParent().getName(), columnNames));
        } else if (c.hasOptimizedQueries()) {
            ret = true;
            if (debug.val)
                LOG.debug(String.format("Using existing %d optimized query plans using %s's vertical partition", c.getOptimizedQueries().size(), c.getParent().getName()));
        } else if (debug.val) {
            LOG.warn("No optimized queries were generated for " + c.fullName());
        }
        return (ret);
    }
}
