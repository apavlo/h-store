package edu.brown.benchmark.tpce.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class TableStatistics {

    // Hack for now...
    private static final Map<String, Long> TUPLE_COUNT = new HashMap<String, Long>();

    public static void addTupleCount(String table_name, long count) {
        Long total = TUPLE_COUNT.get(table_name);
        if (total == null)
            total = 0l;
        total += count;
        TUPLE_COUNT.put(table_name, total);
    }

    public static long getTupleCount(String table_name) {
        return (TUPLE_COUNT.get(table_name));
    }

    public static Set<String> getTables() {
        return (TUPLE_COUNT.keySet());
    }

    public static String debug() {
        String ret = "";
        for (String table_name : TUPLE_COUNT.keySet()) {
            ret += String.format("%-16s", table_name + ":") + TUPLE_COUNT.get(table_name) + "\n";
        }
        return (ret);
    }

}
