package edu.brown.designer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.MaterializePlanNode;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.ColumnStatistics;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.StringUtil;

public class MemoryEstimator {
    private static final Logger LOG = Logger.getLogger(MemoryEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final Map<String, Long> CACHE_TABLE_ESTIMATE = new HashMap<String, Long>();

    /**
     * Table -> Tuple Size (bytes)
     */
    public static final Map<Table, Long> TABLE_TUPLE_SIZE = new HashMap<Table, Long>();

    private final WorkloadStatistics stats;
    private final AbstractHasher hasher;
    private final Map<String, ObjectHistogram<Integer>> cache_table_partition = new HashMap<String, ObjectHistogram<Integer>>();

    /**
     * Constructor
     * 
     * @param stats
     * @param hasher
     */
    public MemoryEstimator(WorkloadStatistics stats, AbstractHasher hasher) {
        this.stats = stats;
        this.hasher = hasher;
    }

    public AbstractHasher getHasher() {
        return (this.hasher);
    }

    public long estimate(Database catalog_db, int partitions) {
        return (this.estimate(catalog_db, partitions, catalog_db.getTables()));
    }

    public long estimateTotalSize(Database catalog_db) {
        return (this.estimate(catalog_db, 1));
    }

    /**
     * Return the estimated size of a single partition in the database for the
     * given tables
     * 
     * @param catalog_db
     * @param partitions
     * @param include_tables
     * @return
     */
    public long estimate(Database catalog_db, int partitions, Collection<Table> include_tables) {
        Map<String, Long> m0 = null;
        if (debug.val) {
            LOG.debug(String.format("Estimating total size of tables for %d partitions: %s", partitions, include_tables));
            m0 = new ListOrderedMap<String, Long>();
        }

        // Sanity Check: Make sure that we weren't given a table that doesn't
        // exist
        Set<Table> remaining_tables = new HashSet<Table>(include_tables);

        long bytes = 0l;
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (include_tables.contains(catalog_tbl) == false) {
                if (trace.val)
                    LOG.trace("Skipping " + catalog_tbl);
                continue;
            }

            // TABLE SIZE
            if (trace.val)
                LOG.trace("Estimating table size for " + catalog_tbl);
            long table_bytes = this.estimate(catalog_tbl, partitions);
            if (debug.val) {
                Column catalog_col = (catalog_tbl.getIsreplicated() ? ReplicatedColumn.get(catalog_tbl) : catalog_tbl.getPartitioncolumn());
                assert (catalog_col != null) : catalog_tbl;
                m0.put(catalog_col.fullName(), table_bytes);
            }
            bytes += table_bytes;

            // INDEXES (unsupported)
            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                bytes += this.estimate(catalog_idx, partitions);
            } // FOR
            remaining_tables.remove(catalog_tbl);
        } // FOR
        assert (remaining_tables.isEmpty()) : String.format("Unknown Tables: %s / %s / %s", remaining_tables, include_tables, catalog_db.getTables());
        if (debug.val) {
            Map<String, Long> m1 = new ListOrderedMap<String, Long>();
            m1.put("Total Database Size", bytes);
            LOG.debug(String.format("Memory Estimate for %d Partitions:\n%s", partitions, StringUtil.formatMaps(m0, m1)));
        }
        return (bytes);
    }

    public long estimate(Index catalog_idx, int partitions) {
        long estimate = 0;

        // TODO: We somehow need to know the cardinality of things...

        return (estimate);
    }

    /**
     * Returns the estimated size of a table fragment at a particular hash key
     * value if that table is partitioned on a column.
     * 
     * @param catalog_tbl
     * @param partition_col
     * @param partition
     * @return
     */
    public long estimate(Table catalog_tbl, Column partition_col, int partition) {
        TableStatistics table_stats = this.stats.getTableStatistics(catalog_tbl);
        ColumnStatistics col_stats = table_stats.getColumnStatistics(partition_col);
        String col_key = CatalogKey.createKey(partition_col);

        ObjectHistogram<Integer> h = this.cache_table_partition.get(col_key);
        if (h == null) {
            h = new ObjectHistogram<Integer>();
            for (Object value : col_stats.histogram.values()) {
                int hash = this.hasher.hash(value, catalog_tbl);
                h.put(hash);
            } // FOR
            this.cache_table_partition.put(col_key, h);
        }

        assert (h.values().contains(partition));
        return (h.get(partition) * table_stats.tuple_size_avg);
    }

    /**
     * Return the maximum size estimate for this table for a single
     * partition/site
     * 
     * @param catalog_tbl
     * @return
     */
    public long estimate(Table catalog_tbl, int partitions) {
        long estimate = 0;

        // For now we'll just estimate the table to be based on the maximum
        // number of
        // tuples for all possible partitions
        TableStatistics table_stats = this.stats.getTableStatistics(catalog_tbl);
        assert (table_stats != null) : "Missing statistics for " + catalog_tbl;
        if (debug.val && table_stats.tuple_size_total == 0) {
            LOG.warn(this.stats.debug(CatalogUtil.getDatabase(catalog_tbl)));
        }
        // assert(table_stats.tuple_size_total != 0) : "Size estimate for " +
        // catalog_tbl + " is zero!";

        Column catalog_col = null;
        if (catalog_tbl.getIsreplicated()) {
            estimate += table_stats.tuple_size_total;
            if (trace.val)
                catalog_col = ReplicatedColumn.get(catalog_tbl);
        } else {
            // FIXME: Assume uniform distribution for now
            estimate += table_stats.tuple_size_total / partitions;
            if (trace.val)
                catalog_col = catalog_tbl.getPartitioncolumn();
        }
        if (trace.val)
            LOG.debug(String.format("%-30s%d [total=%d]", catalog_col.fullName() + ":", estimate, table_stats.tuple_size_total));
        return (estimate);
    }

    /**
     * Returns the estimate size of a tuple in bytes
     * 
     * @param catalog_tbl
     * @return
     */
    public static Long estimateTupleSize(Table catalog_tbl, Statement catalog_stmt, Object params[]) throws Exception {
        Long bytes = null;

        // If the table contains nothing but numeral values, then we don't need
        // to loop through and calculate the estimated tuple size each time
        // around,
        // since it's always going to be the same
        bytes = TABLE_TUPLE_SIZE.get(catalog_tbl);
        if (bytes != null)
            return (bytes);

        // Otherwise, we have to calculate things.
        // Then pluck out all the MaterializePlanNodes so that we inspect the
        // tuples
        AbstractPlanNode node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        Collection<MaterializePlanNode> matched_nodes = PlanNodeUtil.getPlanNodes(node, MaterializePlanNode.class);
        if (matched_nodes.isEmpty()) {
            LOG.fatal("Failed to retrieve any MaterializePlanNodes from " + catalog_stmt);
            return 0l;
        } else if (matched_nodes.size() > 1) {
            LOG.fatal("Unexpectadly found more than one MaterializePlanNode in " + catalog_stmt);
            return 0l;
        }
        // MaterializePlanNode mat_node =
        // (MaterializePlanNode)CollectionUtil.getFirst(matched_nodes);

        // This obviously isn't going to be exact because they may be inserting
        // from a SELECT statement or the columns might complex
        // AbstractExpressions
        // That's ok really, because all we really need to do is look at size of
        // the strings
        bytes = 0l;
        boolean numerals_only = true;
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType type = VoltType.get((byte) catalog_col.getType());
            switch (type) {
                case TINYINT:
                    bytes += 1;
                    break;
                case SMALLINT:
                    bytes += 2;
                    break;
                case INTEGER:
                    bytes += 4;
                    break;
                case BIGINT:
                case FLOAT:
                case TIMESTAMP:
                    bytes += 8;
                    break;
                case STRING: {
                    numerals_only = false;
                    // if (params[catalog_col.getIndex()] != null) {
                    // bytes += 8 * ((String)
                    // params[catalog_col.getIndex()]).length();
                    // }
                    bytes += 8 * catalog_col.getSize(); // XXX

                    /*
                     * AbstractExpression root_exp =
                     * mat_node.getOutputColumnExpressions
                     * ().get(catalog_col.getIndex()); for
                     * (ParameterValueExpression value_exp :
                     * ExpressionUtil.getExpressions(root_exp,
                     * ParameterValueExpression.class)) { int param_idx =
                     * value_exp.getParameterId(); bytes += 8 *
                     * ((String)params[param_idx]).length(); } // FOR
                     */
                    break;
                }
                default:
                    LOG.warn("Unsupported VoltType: " + type);
            } // SWITCH
        } // FOR
          // If the table only has numerals, then we can store it in our cache
        if (numerals_only)
            TABLE_TUPLE_SIZE.put(catalog_tbl, bytes);

        return (bytes);
    }

    /**
     * Returns the estimate size of a tuple for the given table in bytes
     * Calculations are based on the Table's Columns specification
     * 
     * @param catalog_tbl
     * @return
     */
    public static long estimateTupleSize(Table catalog_tbl) {
        long bytes = 0;
        final String table_key = CatalogKey.createKey(catalog_tbl);

        // If the table contains nothing but numeral values, then we don't need
        // to loop
        // through and calculate the estimated tuple size each time around,
        // since it's always
        // going to be the same
        if (CACHE_TABLE_ESTIMATE.containsKey(table_key)) {
            return (CACHE_TABLE_ESTIMATE.get(table_key));
        }

        // This obviously isn't going to be exact because they may be inserting
        // from a SELECT statement or the columns might complex
        // AbstractExpressions
        // That's ok really, because all we really need to do is look at size of
        // the strings
        boolean numerals_only = true;
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType type = VoltType.get((byte) catalog_col.getType());
            switch (type) {
                case TINYINT:
                    bytes += 1;
                    break;
                case SMALLINT:
                    bytes += 2;
                    break;
                case INTEGER:
                    bytes += 4;
                    break;
                case BIGINT:
                case FLOAT:
                case TIMESTAMP:
                    bytes += 8;
                    break;
                case STRING:
                    bytes += catalog_col.getSize(); // Assume always max size
                    break;
                default:
                    LOG.fatal("Unsupported VoltType: " + type);
            } // SWITCH
        } // FOR
          // If the table only has numerals, then we can store it in our cache
        if (numerals_only)
            CACHE_TABLE_ESTIMATE.put(table_key, bytes);

        return (bytes);
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);

        MemoryEstimator estimator = new MemoryEstimator(args.stats, new DefaultHasher(args.catalogContext));
        for (Table catalog_tbl : args.catalog_db.getTables()) {
            System.out.println(catalog_tbl + ": " + estimator.estimate(catalog_tbl, args.catalogContext.numberOfPartitions));
        }
    }

}
