package edu.brown.hstore.executors;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencySet;
import org.voltdb.VoltTable;

import edu.brown.hstore.PartitionExecutor;

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
    public abstract DependencySet execute(int id, Map<Integer, List<VoltTable>> tmp_dependencies);
    
}
