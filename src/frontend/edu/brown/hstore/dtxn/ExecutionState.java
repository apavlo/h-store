package edu.brown.hstore.dtxn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * The internal state of a transaction while it is running at a PartitionExecutor
 * This will be removed from the LocalTransaction once its control code is finished executing 
 * @author pavlo
 */
public class ExecutionState {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL PARTITION+DEPENDENCY KEY
    // ----------------------------------------------------------------------------

    private static final int KEY_MAX_VALUE = 65535; // 2^16 - 1
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The ExecutionSite that this TransactionState is tied to
     */
    protected final PartitionExecutor executor;
    
    /**
     * A special lock for the critical sections of the LocalTransaction
     * This is only to handle messages coming from the HStoreCoordinator or from other
     * PartitionExecutors that are executing on this txn's behalf 
     */
    protected final ReentrantLock lock = new ReentrantLock();

    // ----------------------------------------------------------------------------
    // ROUND DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * This latch will block until all the Dependency results have returned
     * Generated in startRound()
     */
    protected CountDownLatch dependency_latch;
    
    /**
     * Mapping from DependencyId to the corresponding DependencyInfo object
     * Map<DependencyId, DependencyInfo>
     */
    protected final Map<Integer, DependencyInfo> dependencies = new HashMap<Integer, DependencyInfo>();
    
    /**
     * Final result output dependencies. Each position in the list represents a single Statement
     */
    protected final List<Integer> output_order = new ArrayList<Integer>();
    
    /**
     * As information come back to us, we need to keep track of what SQLStmt we are storing 
     * the data for. Note that we have to maintain two separate lists for results and responses
     * Partition-DependencyId Key -> Next SQLStmt Index
     */
    protected final Map<Integer, Queue<Integer>> results_dependency_stmt_ctr = new HashMap<Integer, Queue<Integer>>();

    /**
     * Sometimes we will get results back while we are still queuing up the rest of the tasks and
     * haven't started the next round. So we need a temporary space where we can put these guys until 
     * we start the round. Otherwise calculating the proper latch count is tricky
     * Partition-DependencyId Key -> VoltTable
     */
    protected final Map<Integer, VoltTable> queued_results = new LinkedHashMap<Integer, VoltTable>();
    
    /**
     * Blocked FragmentTaskMessages
     */
    protected final List<WorkFragment> blocked_tasks = new ArrayList<WorkFragment>();
    
    /**
     * Unblocked FragmentTaskMessages
     * The VoltProcedure thread will block on this queue waiting for tasks to execute inside of ExecutionSite
     * This has to be a set so that we make sure that we only submit a single message that contains all of the tasks to the Dtxn.Coordinator
     */
    protected final LinkedBlockingDeque<Collection<WorkFragment>> unblocked_tasks = new LinkedBlockingDeque<Collection<WorkFragment>>(); 
    
    /**
     * Whether the current transaction still has outstanding WorkFragments that it
     * needs to execute or get back dependencies from
     */
    protected boolean still_has_tasks = true;
    
    /**
     * Number of SQLStmts in the current batch
     */
    protected int batch_size = 0;
    
    /**
     * The total # of dependencies this Transaction is waiting for in the current round
     */
    protected int dependency_ctr = 0;
    
    /**
     * The total # of dependencies received thus far in the current round
     */
    protected int received_ctr = 0;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    public ExecutionState(PartitionExecutor executor) {
        this.executor = executor;
        
//        int max_batch = HStoreConf.singleton().site.planner_max_batch_size;
//        this.dependencies = (Map<Integer, DependencyInfo>[])new Map<?, ?>[max_batch];
//        for (int i = 0; i < this.dependencies.length; i++) {
//            this.dependencies[i] = new HashMap<Integer, DependencyInfo>();
//        } // FOR
    }
    
    public void clear() {
        if (debug.get()) LOG.debug("Clearing ExecutionState at partition " + this.executor.getPartitionId());
        this.dependency_latch = null;
        this.clearRound();
    }
    
    /**
     * Return a single key that encodes the partition id and dependency id
     * @param partition_id
     * @param dependency_id
     * @return
     */
    protected int createPartitionDependencyKey(int partition_id, int dependency_id) {
        int key = partition_id | dependency_id<<16;
        return (key);
    }
    
    /**
     * For the given encoded Partition+DependencyInfo key, populate the given array
     * with the partitionId first, then the dependencyId second
     * @param key
     * @param values
     */
    protected void getPartitionDependencyFromKey(int key, int values[]) {
        assert(values.length == 2);
        values[0] = key>>0 & KEY_MAX_VALUE;     // PartitionId
        values[1] = key>>16 & KEY_MAX_VALUE;    // DependencyId
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    public void clearRound() {
        this.dependencies.clear();
        this.output_order.clear();
        this.queued_results.clear();
        this.blocked_tasks.clear();
        this.unblocked_tasks.clear();
        this.still_has_tasks = true;

        // Note that we only want to clear the queues and not the whole maps
        for (Queue<Integer> q : this.results_dependency_stmt_ctr.values()) {
            q.clear();
        } // FOR
        
        this.batch_size = 0;
        this.dependency_ctr = 0;
        this.received_ctr = 0;
    }
}
