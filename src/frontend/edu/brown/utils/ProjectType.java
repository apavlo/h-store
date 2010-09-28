package edu.brown.utils;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ProjectType {
    TPCC,
    TPCE,
    TM1,
    SIMPLE,
    AIRLINE,
    EBAY,
    MARKOV,
    BINGO,
    AUCTION;
    
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
