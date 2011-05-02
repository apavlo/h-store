package edu.brown.designer;

import java.util.*;
import org.apache.log4j.Logger;

import org.voltdb.VoltType;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.hashing.*;
import edu.brown.statistics.*;
import edu.brown.utils.*;

public class MemoryEstimator {
    private static final Logger LOG = Logger.getLogger(MemoryEstimator.class);
    private static final boolean d = LOG.isDebugEnabled();
//    private static final boolean t = LOG.isTraceEnabled();

    private static final Map<String, Long> CACHE_TABLE_ESTIMATE = new HashMap<String, Long>();
    
    private final WorkloadStatistics stats;
    private final AbstractHasher hasher;
    private final Map<String, Histogram<Integer>> cache_table_partition = new HashMap<String, Histogram<Integer>>();
    
    /**
     * Constructor
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
        HashSet<Table> all_tables = new HashSet<Table>();
        CollectionUtil.addAll(all_tables, catalog_db.getTables());
        return (this.estimate(catalog_db, partitions, all_tables));
    }
    
    public long estimateTotalSize(Database catalog_db) {
        return (this.estimate(catalog_db, 1));
    }
    
    /**
     * Return the estimated size of a single partition in the database for the given tables
     * @param catalog_db
     * @param partitions
     * @param include_tables
     * @return
     */
    public long estimate(Database catalog_db, int partitions, Collection<Table> include_tables) {
        Map<String, Long> m = null;
        if (d) {
            LOG.debug(String.format("Estimating total size of tables for %d partitions: %s", partitions, include_tables));
            m = new HashMap<String, Long>();
        }

        // Sanity Check: Make sure that we weren't given a table that doesn't exist
        Set<Table> remaining_tables = new HashSet<Table>(include_tables);
        
        long bytes = 0l;
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (!include_tables.contains(catalog_tbl)) continue;
            long table_bytes = this.estimate(catalog_tbl, partitions);
            if (d) m.put(catalog_tbl.getName(), table_bytes);
            bytes += table_bytes;
            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                bytes += this.estimate(catalog_idx, partitions);
            } // FOR
            remaining_tables.remove(catalog_tbl);
        } // FOR
        assert(remaining_tables.isEmpty()) : "Unknown Tables: " + remaining_tables;
        if (d) {
            m.put("Total Database Size", bytes);
            LOG.debug(String.format("Memory Estimate for %d Partitions:\n%s", partitions, StringUtil.formatMaps(m)));
        }
        return (bytes);
    }
    
    public long estimate(Index catalog_idx, int partitions) {
        long estimate = 0;
        
        // TODO: We somehow need to know the cardinality of things...
        
        return (estimate);
    }
    
    /**
     * Returns the estimated size of a table fragment at a particular hash key value if
     * that table is partitioned on a column.
     * @param catalog_tbl
     * @param partition_col
     * @param partition
     * @return
     */
    public long estimate(Table catalog_tbl, Column partition_col, int partition) {
        TableStatistics table_stats = this.stats.getTableStatistics(catalog_tbl);
        ColumnStatistics col_stats = table_stats.getColumnStatistics(partition_col);
        String col_key = CatalogKey.createKey(partition_col);
        
        Histogram<Integer> h = this.cache_table_partition.get(col_key);
        if (h == null) {
            h = new Histogram<Integer>();
            for (Object value : col_stats.histogram.values()) {
                int hash = this.hasher.hash(value, catalog_tbl);
                h.put(hash);
            } // FOR
            this.cache_table_partition.put(col_key, h);
        }
        
        assert(h.values().contains(partition));
        return (h.get(partition) * table_stats.tuple_size_avg);
    }
    
    /**
     * Return the maximum size estimate for this table for a single partition/site
     * @param catalog_tbl
     * @return
     */
    public long estimate(Table catalog_tbl, int partitions) {
        long estimate = 0;
        
        // For now we'll just estimate the table to be based on the maximum number of
        // tuples for all possible partitions
        TableStatistics table_stats = this.stats.getTableStatistics(catalog_tbl);
        assert(table_stats != null);
        if (table_stats.tuple_size_total == 0) {
            LOG.warn(this.stats.debug(CatalogUtil.getDatabase(catalog_tbl)));
        }
        assert(table_stats.tuple_size_total != 0) : catalog_tbl;
        
        Column catalog_col = null;
        if (catalog_tbl.getIsreplicated()) {
            estimate += table_stats.tuple_size_total;
            if (d) catalog_col = ReplicatedColumn.get(catalog_tbl);
        } else {
            // FIXME: Assume uniform distribution for now
            estimate += table_stats.tuple_size_total / partitions;
            if (d) catalog_col = catalog_tbl.getPartitioncolumn();
        }
        if (d) LOG.debug(String.format("%-30s%d [total=%d]", catalog_col.fullName() + ":", estimate, table_stats.tuple_size_total));
        return (estimate);
    }
    
    /**
     * 
     * @param catalog_tbl
     * @return
     */
    public static long estimateFromCatalog(Table catalog_tbl) {
        long bytes = 0;
        final String table_key = CatalogKey.createKey(catalog_tbl);
        
        //
        // If the table contains nothing but numeral values, then we don't need to loop
        // through and calculate the estimated tuple size each time around, since it's always
        // going to be the same
        //
        if (CACHE_TABLE_ESTIMATE.containsKey(table_key)) {
            return (CACHE_TABLE_ESTIMATE.get(table_key));
        }
        
        //
        // This obviously isn't going to be exact because they may be inserting
        // from a SELECT statement or the columns might complex AbstractExpressions
        // That's ok really, because all we really need to do is look at size of the strings
        //
        boolean numerals_only = true;
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType type = VoltType.get((byte)catalog_col.getType()); 
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
        //
        // If the table only has numerals, then we can store it in our cache
        //
        if (numerals_only) CACHE_TABLE_ESTIMATE.put(table_key, bytes);
        
        return (bytes);
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        
        MemoryEstimator estimator = new MemoryEstimator(args.stats, new DefaultHasher(args.catalog_db, CatalogUtil.getNumberOfPartitions(args.catalog_db)));
        int partitions = CatalogUtil.getNumberOfPartitions(args.catalog_db);
        for (Table catalog_tbl : args.catalog_db.getTables()) {
            System.out.println(catalog_tbl + ": " + estimator.estimate(catalog_tbl, partitions));
        }
    }
    
}
