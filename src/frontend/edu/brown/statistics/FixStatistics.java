package edu.brown.statistics;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ProjectType;

public abstract class FixStatistics {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(FixStatistics.class.getName());

    public static final class Constants {
        // Item constants
        public static final int NUM_ITEMS = 100000;

        // Warehouse constants
        public static final int NUM_WAREHOUSES = 10;

        // Stock constants
        public static final int STOCK_PER_WAREHOUSE = 100000;

        // District constants
        public static final int DISTRICTS_PER_WAREHOUSE = 10;

        // Customer constants
        public static final int CUSTOMERS_PER_DISTRICT = 3000;

        // Order constants
        public static final int INITIAL_ORDERS_PER_DISTRICT = 3000;

        // Order line constants
        public static final int INITIAL_QUANTITY = 5;

        // History constants
        public static final double INITIAL_AMOUNT = 10.00f;

        // New order constants
        public static final int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;
    }

    public static final Map<ProjectType, Map<String, Integer>> TUPLE_COUNTS = new HashMap<ProjectType, Map<String, Integer>>();
    static {
        // --------------------------------------
        // TPC-C
        // --------------------------------------
        Map<String, Integer> tuples = new HashMap<String, Integer>();
        tuples.put("ITEM", Constants.NUM_ITEMS);
        tuples.put("WAREHOUSE", 10); // FIXME
        tuples.put("STOCK", tuples.get("WAREHOUSE") * Constants.STOCK_PER_WAREHOUSE);
        tuples.put("DISTRICT", tuples.get("WAREHOUSE") * Constants.DISTRICTS_PER_WAREHOUSE);
        tuples.put("CUSTOMER", tuples.get("DISTRICT") * Constants.CUSTOMERS_PER_DISTRICT);
        tuples.put("ORDERS", tuples.get("DISTRICT") * Constants.INITIAL_ORDERS_PER_DISTRICT);
        tuples.put("ORDER_LINE", tuples.get("ORDERS") * Constants.INITIAL_QUANTITY);
        tuples.put("HISTORY", tuples.get("CUSTOMER"));
        tuples.put("NEW_ORDER", tuples.get("DISTRICT") * Constants.INITIAL_NEW_ORDERS_PER_DISTRICT);
        TUPLE_COUNTS.put(ProjectType.TPCC, tuples);
    }

    /**
     * @param catalog_db
     * @throws Exception
     */
    public static void populateStatistics(ProjectType project_type, Database catalog_db, WorkloadStatistics stats) throws Exception {
        assert (TUPLE_COUNTS.containsKey(project_type));
        // Map<String, Integer> tuples = TUPLE_COUNTS.get(project_type);

        //
        // We first need to go through all the partitions and figure out the
        // tuple count skew
        //
        /*
         * int num_partitions =
         * stats.getTableStatistics(catalog_db.getTables().get
         * (0)).partition_count; Map<Integer, Long> partition_total_count = new
         * HashMap<Integer, Long>(); for (int i = 0; i < num_partitions; i++) {
         * partition_total_count.put(i, 0l); } // FOR // // Go through each
         * partition and count the total number of tuples stored there // When
         * we then go through and update the number of tuples we'll use the
         * ratio // of the number tuples on each partition compared to the total
         * number of tuples // to determine the skew of the data. // This is all
         * super hackish and we should just figure out how to really get the
         * number of // tuples first loaded into the system... // long
         * orig_total_tuples = 0; for (Table catalog_tbl :
         * catalog_db.getTables()) { String table_key =
         * CatalogUtil.createKey(catalog_tbl); String table_name =
         * catalog_tbl.getName(); TableStatistics table_stats =
         * stats.getTableStatistics(table_key); assert(table_stats != null);
         * assert(tuples.containsKey(table_name));
         * assert(table_stats.partition_count == num_partitions); boolean
         * has_data = false; for (Integer partition_idx :
         * table_stats.tuple_count_partitions.keySet()) { long stats_cnt =
         * table_stats.tuple_count_partitions.get(partition_idx); if (stats_cnt
         * > 0) { long new_cnt = partition_total_count.get(partition_idx);
         * partition_total_count.put(partition_idx, (stats_cnt + new_cnt));
         * has_data = true; } } // FOR orig_total_tuples +=
         * table_stats.tuple_count_total; } // FOR // // Update missing table
         * information // for (Table catalog_tbl : catalog_db.getTables()) {
         * String table_key = CatalogUtil.createKey(catalog_tbl); String
         * table_name = catalog_tbl.getName(); TableStatistics table_stats =
         * stats.getTableStatistics(table_key); assert(table_stats != null);
         * assert(tuples.containsKey(table_name)); int num_tuples =
         * tuples.get(table_name); assert(num_tuples >= 0); // // Tuple Counts
         * // table_stats.tuple_count_total += num_tuples; for (Integer
         * partition_idx : table_stats.tuple_count_partitions.keySet()) { // We
         * have to account for skew double skew =
         * partition_total_count.get(partition_idx) / orig_total_tuples;
         * table_stats.tuple_count_partitions.put(partition_idx,
         * Math.round(table_stats.tuple_count_total * skew)); } // FOR long
         * tuples_per_partition = (catalog_tbl.getIsreplicated() ? num_tuples :
         * (num_tuples / num_partitions)); if (table_stats.tuple_count_min == 0)
         * table_stats.tuple_count_min = tuples_per_partition; if
         * (table_stats.tuple_count_max == 0) table_stats.tuple_count_max =
         * tuples_per_partition; if (table_stats.tuple_count_avg == 0)
         * table_stats.tuple_count_avg = tuples_per_partition;
         */

        //
        // Tuple Sizes
        //
        /*
         * int size_tuple = table_stats.tuple_size_avg; int
         * orig_tuple_size_total = table_stats.tuple_size_total;
         * table_stats.tuple_size_total += table_stats.tuple_count_total *
         * size_tuple; for (Integer partition_idx :
         * table_stats.tuple_size_partitions.keySet()) { // We have to account
         * for skew int size =
         * table_stats.tuple_size_partitions.get(partition_idx); double skew =
         * size / orig_tuple_size_total;
         * table_stats.tuple_size_partitions.put(partition_idx,
         * (int)Math.round(table_stats.tuple_size_total * skew)); } // FOR
         */
        // } // FOR
        return;
    }

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        assert (args.stats != null);

        String output_path = args.getParam(ArgumentsParser.PARAM_STATS_OUTPUT);
        assert (output_path != null);
        ProjectType project_type = args.catalog_type;

        // Fix the catalog!
        populateStatistics(project_type, args.catalog_db, args.stats);

        //
        // We need to write this things somewhere now...
        //
        System.out.println(args.stats.debug(args.catalog_db));
        // args.stats.save(output_path);
        LOG.info("Wrote updated statistics to '" + output_path + "'");

        return;
    }

}
