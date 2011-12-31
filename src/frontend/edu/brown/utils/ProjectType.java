package edu.brown.utils;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ProjectType {
    TPCC        ("TPC-C",       "org.voltdb.benchmark.tpcc"),
    TPCE        ("TPC-E",       "edu.brown.benchmark.tpce"),
    TM1         ("TM1",         "edu.brown.benchmark.tm1"),
    SIMPLE      ("Simple",      null),
    AIRLINE     ("Airline",     "edu.brown.benchmark.airline"),
    MARKOV      ("Markov",      "edu.brown.benchmark.markov"),
    BINGO       ("Bingo",       null),
    AUCTIONMARK ("AuctionMark", "edu.brown.benchmark.auctionmark"),
    LOCALITY    ("Locality",    "edu.brown.benchmark.locality"),
    MAPREDUCE   ("MapReduce",   "edu.brown.benchmark.mapreduce"),
    EXAMPLE     ("Example",     "edu.brown.benchmark.example"),
    TEST        ("Test",        null),
    ;
    
    private final String package_name;
    private final String benchmark_name;
    
    private ProjectType(String benchmark_name, String package_name) {
        this.benchmark_name = benchmark_name;
        this.package_name = package_name;
    }
    
    public String getBenchmarkName() {
        return (this.benchmark_name);
    }
    
    public String getBenchmarkPrefix() {
        return (this.benchmark_name.replace("-", ""));
    }
    
    /**
     * Returns the package name for where this 
     * We need this because we need to be able to dynamically reference various things
     * from the 'src/frontend' directory before we compile the 'tests/frontend' directory
     * @return
     */
    public String getPackageName() {
        return (this.package_name);
    }
    
    protected static final Map<Integer, ProjectType> idx_lookup = new HashMap<Integer, ProjectType>();
    protected static final Map<String, ProjectType> name_lookup = new HashMap<String, ProjectType>();
    static {
        for (ProjectType vt : EnumSet.allOf(ProjectType.class)) {
            ProjectType.idx_lookup.put(vt.ordinal(), vt);
            ProjectType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        } // FOR
    }
    
    public static ProjectType get(Integer idx) {
        return (ProjectType.idx_lookup.get(idx));
    }
    
    public static ProjectType get(String name) {
        return (ProjectType.name_lookup.get(name.toLowerCase().intern()));
    }
}
