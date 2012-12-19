package edu.brown.api;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * These represent the different states that the BenchmarkComponent's ControlPipe
 * could be in. 
 */
public enum ControlState {
    PREPARING,
    READY,
    RUNNING,
    DUMPING,
    PAUSED,
    ERROR;

    protected static final Map<Integer, ControlState> idx_lookup = new HashMap<Integer, ControlState>();
    protected static final Map<String, ControlState> name_lookup = new HashMap<String, ControlState>();
    static {
        for (ControlState vt : EnumSet.allOf(ControlState.class)) {
            ControlState.idx_lookup.put(vt.ordinal(), vt);
            ControlState.name_lookup.put(vt.name().toUpperCase(), vt);
        } // FOR
    }
    
    public static ControlState get(String name) {
        return (ControlState.name_lookup.get(name.trim().toUpperCase()));
    }
}