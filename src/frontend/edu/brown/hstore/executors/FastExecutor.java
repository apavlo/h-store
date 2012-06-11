package edu.brown.hstore.executors;

import org.voltdb.VoltTable;
import org.voltdb.catalog.PlanFragment;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.txns.LocalTransaction;

public abstract class FastExecutor {

    protected final PartitionExecutor executor;
    
    /**
     * Constructor
     * @param executor
     */
    public FastExecutor(PartitionExecutor executor) {
        this.executor = executor;
    }
    
    /**
     * Execute a Java-only operation to generate the output of a PlanFragment for 
     * the given transaction without needing to go down in to ExecutionEngine
     * @param ts 
     * @param catalog_frag
     * @param input
     * @return
     */
    public abstract VoltTable execute(LocalTransaction ts, PlanFragment catalog_frag, VoltTable input[]);
    
}
