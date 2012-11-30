package edu.brown.api;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * These are the commands that the BenchmarkController will send to each
 * BenchmarkComponent in order to coordinate the benchmark's execution
 */
public enum ControlCommand {
    /** Start executing transactions and recording stats */
    START,
    /** Clear internal counters and stats */
    CLEAR,
    /** Pause invoking new txn requests */
    PAUSE,
    /** Get the execution stats since the last poll */
    POLL,
    /** Get the complete list of transaction response entries */
    DUMP_TXNS,
    /** Stop this BenchmarkComponent instance */
    STOP,
    /**
     * This is the same as STOP except that the BenchmarkComponent will
     * tell the cluster to shutdown first before it exits 
     */
    SHUTDOWN,
    ;

    protected static final Map<Integer, ControlCommand> idx_lookup = new HashMap<Integer, ControlCommand>();
    protected static final Map<String, ControlCommand> name_lookup = new HashMap<String, ControlCommand>();
    static {
        for (ControlCommand vt : EnumSet.allOf(ControlCommand.class)) {
            ControlCommand.idx_lookup.put(vt.ordinal(), vt);
            ControlCommand.name_lookup.put(vt.name().toUpperCase(), vt);
        } // FOR
    }
    
    public static ControlCommand get(String name) {
        return (ControlCommand.name_lookup.get(name.trim().toUpperCase()));
    }
}