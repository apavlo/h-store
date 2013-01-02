package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.utils.Pair;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.util.ParameterSetArrayCache;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.interfaces.DebugContext;
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
     * Reusable list of WorkFragment builders. The builders are not reusable
     * but the list is.
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
     * Internal cache of the result queues that were used by the txn in this round.
     * This is so that we don't have to clear all of the queues in the entire results_dependency_stmt_ctr cache. 
     */
    private final Set<Queue<Integer>> results_queue_cache = new HashSet<Queue<Integer>>();
    
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
        if (debug.val) LOG.debug("Clearing ExecutionState at partition " + this.executor.getPartitionId());
        this.dependency_latch = null;
        this.clearRound();
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Keep track of a new output dependency from the given partition that corresponds
     * to the SQL statement executed at the given offset.
     * @param partition
     * @param output_dep_id
     * @param stmt_index
     */
    public void addResultDependencyStatement(int partition, int output_dep_id, int stmt_index) {
        Pair<Integer, Integer> key = Pair.of(partition, output_dep_id);
        Queue<Integer> rest_stmt_ctr = this.results_dependency_stmt_ctr.get(key);
        if (rest_stmt_ctr == null) {
            rest_stmt_ctr = new LinkedList<Integer>();
            this.results_dependency_stmt_ctr.put(key, rest_stmt_ctr);
        }
        rest_stmt_ctr.add(stmt_index);
        this.results_queue_cache.add(rest_stmt_ctr);
        if (debug.val) LOG.debug(String.format("%s - Set Dependency Statement Counters for <%d %d>: %s",
                                 this, partition, output_dep_id, rest_stmt_ctr));
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
    
    /**
     * Returns true if this transaction still has WorkFragments
     * that need to be dispatched to the appropriate PartitionExecutor 
     * @return
     */
    public boolean stillHasWorkFragments() {
        return (this.still_has_tasks);
    }

    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    public void initRound(int batchSize) {
        this.batch_size = batchSize;
    }
    
    public void clearRound() {
        this.dependencies.clear();
        this.output_order.clear();
        this.queued_results.clear();
        this.blocked_tasks.clear();
        this.unblocked_tasks.clear();
        this.still_has_tasks = true;

        // Note that we only want to clear the queues and not the whole maps
        for (Queue<Integer> q : this.results_queue_cache) {
            q.clear();
        } // FOR
        this.results_queue_cache.clear();
        
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
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    public class Debug implements DebugContext {
        public int getDependencyCount() { 
            return (dependency_ctr);
        }
        public Collection<WorkFragment.Builder> getBlockedWorkFragments() {
            return (blocked_tasks);
        }
        public List<Integer> getOutputOrder() {
            return (output_order);
        }
        /**
         * Return the number of statements that have been queued up in the last batch
         * @return
         */
        public int getStatementCount() {
            return (batch_size);
        }
        public Map<Integer, DependencyInfo> getStatementDependencies(int stmt_index) {
            return (dependencies); // [stmt_index]);
        }
    }
    
    private Debug cachedDebugContext;
    public Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new Debug();
        }
        return this.cachedDebugContext;
    }
}
