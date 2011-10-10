package edu.mit.hstore;

import org.voltdb.VoltTable;

public abstract class HStoreConstants {

    /**
     * Just an empty VoltTable array that we can reuse all around the system
     */
    public static final VoltTable EMPTY_RESULT[] = new VoltTable[0];
    
    /**
     * When an HStoreSite is ready to start processing transactions, it will print
     * this message. The BenchmarkController will be waiting for this output.
     */
    public static final String SITE_READY_MSG = "Site is ready for action";

    /**
     * Represents a null dependency id
     */
    public static final int NULL_DEPENDENCY_ID = -1;

    /**
     * Default token used to indicate that a txn is not using undo buffers
     * when executing PlanFragments in the EE
     */
    public static final long DISABLE_UNDO_LOGGING_TOKEN = Long.MAX_VALUE;

}
