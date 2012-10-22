package edu.brown.hstore.txns;

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
import org.voltdb.utils.Pair;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.util.ParameterSetArrayCache;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;

/**
 * The internal state of a transaction while it is running at a PartitionExecutor
 * This will be removed from the LocalTransaction once its control code is finished executing 
 * @author pavlo
 */
public class ExecutionState implements Poolable {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
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
    // TEMPORARY DATA COLLECTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Reusable cache of ParameterSet arrays for VoltProcedures
     */
    public final ParameterSetArrayCache procParameterSets = new ParameterSetArrayCache(10);
    
    /**
     * 
     */
    public final List<WorkFragment.Builder> tmp_partitionFragments = new ArrayList<WorkFragment.Builder>(); 
    
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
     * PartitionId -> DependencyId -> Next SQLStmt Index
     */
    protected final Map<Pair<Integer, Integer>, Queue<Integer>> results_dependency_stmt_ctr = new HashMap<Pair<Integer,Integer>, Queue<Integer>>();
    
    /**
     * Sometimes we will get results back while we are still queuing up the rest of the tasks and
     * haven't started the next round. So we need a temporary space where we can put these guys until 
     * we start the round. Otherwise calculating the proper latch count is tricky
     * Partition-DependencyId Key -> VoltTable
     */
    protected final Map<Pair<Integer, Integer>, VoltTable> queued_results = new LinkedHashMap<Pair<Integer,Integer>, VoltTable>();
    
    /**
     * Blocked FragmentTaskMessages
     */
    protected final List<WorkFragment.Builder> blocked_tasks = new ArrayList<WorkFragment.Builder>();
    
    /**
     * Unblocked FragmentTaskMessages
     * The VoltProcedure thread will block on this queue waiting for tasks to execute inside of ExecutionSite
     * This has to be a set so that we make sure that we only submit a single message that contains all of the tasks to the Dtxn.Coordinator
     */
    protected final LinkedBlockingDeque<Collection<WorkFragment.Builder>> unblocked_tasks = new LinkedBlockingDeque<Collection<WorkFragment.Builder>>(); 
    
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
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    protected Thread getExecutionThread() {
        return (this.executor.getExecutionThread());
    }
    
    public LinkedBlockingDeque<Collection<WorkFragment.Builder>> getUnblockedWorkFragmentsQueue() {
        return (this.unblocked_tasks);
    }
    
    
    /**
     * Return the latch that will block the PartitionExecutor's thread until
     * all of the query results have been retrieved for this transaction's
     * current SQLStmt batch
     */
    public CountDownLatch getDependencyLatch() {
        return this.dependency_latch;
    }
    
    public int getDependencyCount() { 
        return (this.dependency_ctr);
    }
    
    /**
     * Returns true if this transaction still has WorkFragments
     * that need to be dispatched to the appropriate PartitionExecutor 
     * @return
     */
    public boolean stillHasWorkFragments() {
        return (this.still_has_tasks);
    }

    // ----------------------------------------------------------------------------
    // TESTING STUFF
    // ----------------------------------------------------------------------------
    
    protected Collection<WorkFragment.Builder> getBlockedWorkFragments() {
        return (this.blocked_tasks);
    }
    
    
    protected List<Integer> getOutputOrder() {
        return (this.output_order);
    }
    
    /**
     * Return the number of statements that have been queued up in the last batch
     * @return
     */
    protected int getStatementCount() {
        return (this.batch_size);
    }
    
    protected Map<Integer, DependencyInfo> getStatementDependencies(int stmt_index) {
        return (this.dependencies); // [stmt_index]);
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

    @Override
    public boolean isInitialized() {
        return (true);
    }

    @Override
    public void finish() {
        this.procParameterSets.reset();
    }
}
