package edu.brown.profilers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * Container for internal profiling metrics for the behavior of transactions.
 * @author pavlo
 */
public class TransactionProfiler extends AbstractProfiler implements Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionProfiler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ---------------------------------------------------------------
    // INTERNAL STATE
    // ---------------------------------------------------------------

    /**
     * The current stack of this profiler.
     * This makes it easier for us to pop out of a mode when the txn enters 
     * the next phase of its life. It will also automatically update a history
     * log of the states that we entered.
     */
    @SuppressWarnings("serial")
    private final Stack<ProfileMeasurement> stack = new Stack<ProfileMeasurement>() {
        public ProfileMeasurement push(ProfileMeasurement item) {
            TransactionProfiler.this.history.add(item);
            return super.push(item);
        }
    };

    private final List<ProfileMeasurement> history = new ArrayList<ProfileMeasurement>();

    /**
     * Whether we need to disable all profiling for the current txn
     */
    private transient boolean disabled = false;

    /**
     * Whether the current txn for this profiler is marked as single-partitioned. 
     */
    private boolean singlePartitioned;
    
    // ---------------------------------------------------------------
    // COUNTERS
    // ---------------------------------------------------------------
    
    /**
     * The total number of queries that this txn invoked
     */
    private int num_queries = 0;
    
    /**
     * The total number of queries that this txn invoked that 
     * needed to be executed on remote partitions.
     */
    private int num_remote_queries = 0;
    
    /**
     * The total number of batches that this txn invoked
     */
    private int num_batches = 0;
    
    /**
     * The number of queries that were dispatched as prefetched
     */
    private int num_prefetched = 0;
    
    /**
     * The number of queries that were dispatched as prefetched but never used.
     */
    private int num_prefetched_unused = 0;
    
    /**
     * The number of transactions that were executed speculatively while this
     * transaction was stalled. Note that only distributed transactions 
     * will have speculative txns interleaved with it.
     */
//    private int num_speculative = 0;
    
    /**
     * Early 2PC Optimization Partitions
     */
    private final PartitionSet early_2pc_partitions = new PartitionSet();
    
    // ---------------------------------------------------------------
    // INTERNAL HELPER METHODS
    // ---------------------------------------------------------------
    
    /**
     * 
     * @param expected_parent
     *            - The expected parent
     * @param next
     * @param stopParent
     *            TODO
     */
    private long startInner(ProfileMeasurement expected_parent, ProfileMeasurement next, boolean stopParent) {
        if (debug.val)
            LOG.debug(String.format("Start PARENT[%s] -> NEXT[%s]", expected_parent, next));
        assert(this.stack.size() > 0);
        assert(this.stack.peek() == expected_parent) :
            String.format("Unexpected state %s: PARENT[%s] -> NEXT[%s]\n%s",
                          this.stack.peek(), expected_parent.getName(),
                          next.getName(), StringUtil.join("\n", this.stack));
        long timestamp = ProfileMeasurement.getTime();
        if (stopParent) {
            ProfileMeasurementUtil.swap(timestamp, expected_parent, next);
        } else {
            next.start(timestamp);
        }
        this.stack.push(next);
        return (timestamp);
    }

    private long stopInner(ProfileMeasurement expected_current, ProfileMeasurement next, boolean startNext) {
        if (debug.val)
            LOG.debug(String.format("Stop PARENT[%s] <- CURRENT[%s]", next, expected_current));
        assert(this.stack.size() > 0);
        ProfileMeasurement pm = this.stack.pop();
        assert(pm == expected_current) :
            String.format("Expected current state %s but was %s! [expectedParent=%s]\n%s",
                          expected_current, pm, next, this.stack);
        assert(next == this.stack.peek()) :
            String.format("Expected current parent %s but was %s! [inner=%s]",
                          next, this.stack.peek(), expected_current);
        long timestamp = ProfileMeasurement.getTime();
        if (startNext) {
            ProfileMeasurementUtil.swap(timestamp, expected_current, next);
        } else {
            pm.stop(timestamp);
        }
        return (timestamp);
    }
    
    private ProfileMeasurement popStack(ProfileMeasurement search, long timestamp) {
        ProfileMeasurement pm = null;
        while (this.stack.isEmpty() == false) {
            pm = this.stack.pop();
            assert(pm != null);
            if (debug.val)
                LOG.debug("STOP " + pm.getName());
            if (pm == search) break;
            pm.stop(timestamp);
        } // WHILE
        assert(pm == search);
        return (pm);
    }
    
    
    // private void startGlobal(ProfileMeasurement global_pm) {
    // assert(this.stack.size() > 0);
    // ProfileMeasurement parent = this.stack.peek();
    // ProfileMeasurement.swap(parent, global_pm);
    // this.stack.push(global_pm);
    // }
    //
    // private void stopGlobal(ProfileMeasurement global_pm) {
    // assert(this.stack.size() > 0);
    // ProfileMeasurement pop = this.stack.pop();
    // assert(global_pm == pop);
    // ProfileMeasurement.swap(global_pm, this.stack.peek());
    // }

    // ---------------------------------------------------------------
    // GLOBAL METHODS
    // ---------------------------------------------------------------

    /**
     * Total time spent executing the transaction This time starts from when the txn first arrives in
     * HStoreSite.procedureInvocation() until it is completely removed in HStoreSite.completeTranasction()
     */
    protected final ProfileMeasurement pm_total = new ProfileMeasurement("TOTAL");

    protected final ProfileMeasurement pm_serialize = new ProfileMeasurement("SERIALIZE");
    protected final ProfileMeasurement pm_deserialize = new ProfileMeasurement("DESERIALIZE");

    /**
     * This is the amount time from when the txn acquires all of the locks for the partitions that it needs
     * to when invokes the first query that needs to get sent to a remote partition.
     */
    protected final ProfileMeasurement pm_first_remote_query = new ProfileMeasurement("FIRST_REMOTE_QUERY");

    /**
     * The amount of time from after the first remote query is invoked to when it arrives back at the
     * transaction's base partition.
     */
    protected final ProfileMeasurement pm_first_remote_query_result = new ProfileMeasurement("FIRST_REMOTE_QUERY_RESULT");
    
    /**
     * The amount of time after the transaction executes the first remote query until it attempts to use the results
     * from that query
     */
    protected final ProfileMeasurement pm_first_remote_query_access = new ProfileMeasurement("FIRST_REMOTE_QUERY_ACCESS");
    

    public void startTransaction(long timestamp) {
        if (this.disabled)
            return;
        if (debug.val)
            LOG.debug(String.format("START %s -> %s", this.pm_total.getName(), this.pm_init_total.getName()));
        this.pm_total.start(timestamp);
        this.pm_init_total.start(timestamp);
        this.stack.push(this.pm_total);
        this.stack.push(this.pm_init_total);
    }

    public void stopTransaction() {
        if (this.disabled)
            return;
        this.disabled = true;
        long timestamp = ProfileMeasurement.getTime();
        while (this.stack.isEmpty() == false) {
            ProfileMeasurement pm = this.stack.pop();
            assert(pm != null);
            if (debug.val)
                LOG.debug("STOP " + pm.getName());
            assert(pm.isStarted()) : pm.debug();
            pm.stop(timestamp);
            assert(pm.isStarted() == false) : pm.debug();
        } // WHILE
        assert(this.stack.isEmpty());
        assert(this.isStopped());

        // Decrement POST_EE/POST_CLIENT from POST_FINISH
        for (ProfileMeasurement pm : new ProfileMeasurement[] { pm_post_ee, pm_post_client }) {
            if (pm.getInvocations() > 0) {
                this.pm_post_finish.decrementTime(pm);
            }
        } // FOR
    }

    public void startSerialization() {
        if (this.disabled) return;
        this.pm_serialize.start();
        // this.startGlobal(this.pm_serialize);
    }

    public void stopSerialization() {
        if (this.disabled) return;
        this.pm_serialize.stop();
        // this.stopGlobal(this.pm_serialize);
    }

    public void startDeserialization() {
        if (this.disabled) return;
        this.pm_deserialize.start();
        // this.startGlobal(this.pm_deserialize);
    }

    public void stopDeserialization() {
        if (this.disabled) return;
        this.pm_deserialize.stop();
        // this.stopGlobal(this.pm_deserialize);
    }

    // ---------------------------------------------------------------
    // INITIALIZATION METHODS
    // ---------------------------------------------------------------

    /**
     * The time spent setting up the transaction before it is queued in either an PartitionExecutor 
     */
    protected final ProfileMeasurement pm_init_total = new ProfileMeasurement("INIT_TOTAL");
    /**
     * The amount of time spent estimating what the transaction will do in the initialization
     */
    protected final ProfileMeasurement pm_init_est = new ProfileMeasurement("INIT_EST");
    /**
     * Time spent waiting in the initialization queue
     */
    protected final ProfileMeasurement pm_init_queue = new ProfileMeasurement("INIT_QUEUE");

    /**
     * 
     */
    public void startInitEstimation() {
        if (this.disabled) return;
        this.startInner(this.pm_init_total, this.pm_init_est, false);
    }

    public void stopInitEstimation() {
        if (this.disabled) return;
        this.stopInner(this.pm_init_est, this.pm_init_total, false);
    }

    public void startInitQueue() {
        if (this.disabled) return;
        this.startInner(this.pm_init_total, this.pm_init_queue, false);
    }

    /**
     * Mark that this txn is requesting a query to be executed on a remote partition.
     * This can be safely invoked multiple times but it will only stop recording 
     * on the first invocation.
     */
    public void markRemoteQuery() {
        assert(this.singlePartitioned == false);
        if (this.pm_first_remote_query.isStarted()) {
            if (trace.val) LOG.trace("Marking first remove query for transaction");
            long timestamp = ProfileMeasurement.getTime();
            this.pm_first_remote_query.stop(timestamp);
            this.pm_first_remote_query_access.start(timestamp);
            this.pm_first_remote_query_result.start(timestamp);
        }
    }
    
    /**
     * Mark that the results needed from a remote query has arrived.
     */
    public void markRemoteQueryResult() {
        assert(this.singlePartitioned == false);
        if (this.pm_first_remote_query_result.isStarted()) {
            this.pm_first_remote_query_result.stop();
        }
    }

    /**
     * Mark that the txn has attempted to access the results returned
     * from a remote query.
     */
    public void markRemoteQueryAccess() {
        assert(this.singlePartitioned == false);
        if (this.pm_first_remote_query_access.isStarted()) {
            this.pm_first_remote_query_access.stop();
        }
    }
    
    // ---------------------------------------------------------------
    // QUEUE METHODS
    // ---------------------------------------------------------------

    /**
     * Time spent waiting in the PartitionLockQueue
     */
    protected final ProfileMeasurement pm_queue_lock = new ProfileMeasurement("QUEUE_LOCK");
    
    /**
     * Time spent waiting in the PartitionExecutor queue
     */
    protected final ProfileMeasurement pm_queue_exec = new ProfileMeasurement("QUEUE_EXEC");
    
    public void startQueueLock() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        assert(this.stack.peek() != this.pm_queue_lock) : "Duplicate calls for " + this.pm_queue_lock;
        long timestamp = ProfileMeasurement.getTime();
        ProfileMeasurement pm = this.popStack(this.pm_init_total, timestamp);
        if (debug.val) LOG.debug("START " + this.pm_queue_lock.getName());
        ProfileMeasurementUtil.swap(timestamp, pm, this.pm_queue_lock);
        this.stack.push(this.pm_queue_lock);
    }
    
    public void startQueueExec() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        assert(this.stack.peek() != this.pm_queue_exec) : "Duplicate calls for " + this.pm_queue_exec;
        long timestamp = ProfileMeasurement.getTime();
        ProfileMeasurement pm = this.popStack(this.pm_queue_lock, timestamp);
        if (debug.val) LOG.debug("START " + this.pm_queue_exec.getName());
        ProfileMeasurementUtil.swap(timestamp, pm, this.pm_queue_exec);
        this.stack.push(this.pm_queue_exec);
    }

    // ---------------------------------------------------------------
    // EXECUTION TIMES
    // ---------------------------------------------------------------

    /**
     * The total time spent executing the transaction This starts when the transaction is removed from the
     * PartitionExecutor's queue until it finishes
     */
    protected final ProfileMeasurement pm_exec_total = new ProfileMeasurement("EXEC_TOTAL");
    /**
     * The amount of time spent executing the Java-portion of the stored procedure
     */
    protected final ProfileMeasurement pm_exec_java = new ProfileMeasurement("EXEC_JAVA");
    /**
     * Time spent blocked waiting for a TransactionWorkResponse to come back
     */
    protected final ProfileMeasurement pm_exec_dtxn_work = new ProfileMeasurement("EXEC_DTXN_WORK");
    /**
     * The amount of time spent planning the transaction
     */
    protected final ProfileMeasurement pm_exec_planner = new ProfileMeasurement("EXEC_PLANNER");
    /**
     * The amount of time spent executing in the plan fragments
     */
    protected final ProfileMeasurement pm_exec_ee = new ProfileMeasurement("EXEC_EE");
    /**
     * The amount of time spent estimating what the transaction will do
     */
    protected final ProfileMeasurement pm_exec_est = new ProfileMeasurement("EXEC_EST");

    /**
     * Invoked when the txn has been removed from the queue and is starting to execute at a local PartitionExecutor
     */
    public void startExec() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        ProfileMeasurement current = this.stack.pop();
        assert(current != this.pm_exec_total) : 
            "Trying to start txn execution twice!";
        assert(current == this.pm_queue_exec || current == this.pm_queue_lock) : 
            "Trying to start execution before txn was queued (" + current + ")";
        long timestamp = ProfileMeasurement.getTime();
        ProfileMeasurementUtil.swap(timestamp, current, this.pm_exec_total);
        this.stack.push(this.pm_exec_total);
        if (this.singlePartitioned == false) {
            this.pm_first_remote_query.start(timestamp);
        }
    }
    
    /**
     * Invoked when the txn is going to be speculatively executed at a local PartitionExecutor
     */
    public void startSpecExec() {
        if (this.disabled) return;
        this.pm_queue_exec.start().stopIfStarted();
        this.startExec();
    }

    public void startExecJava() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_java, false);
    }

    public void stopExecJava() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_java, this.pm_exec_total, false);
    }

    public void startExecPlanning() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_planner, false);
    }

    public void stopExecPlanning() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_planner, this.pm_exec_total, false);
    }

    public void startExecEstimation() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_est, false);
    }

    public void stopExecEstimation() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_est, this.pm_exec_total, false);
    }

    public void startExecDtxnWork() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_dtxn_work, false);
    }

    public void stopExecDtxnWork() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_dtxn_work, this.pm_exec_total, false);
    }

    public void startExecEE() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_ee, false);
    }

    public void stopExecEE() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_ee, this.pm_exec_total, false);
    }

    // ---------------------------------------------------------------
    // CLEAN-UP TIMES
    // ---------------------------------------------------------------

    /**
     * Time spent getting the response back to the client
     */
    protected final ProfileMeasurement pm_post_total = new ProfileMeasurement("POST_TOTAL");
    /**
     * 2PC-PREPARE
     */
    protected final ProfileMeasurement pm_post_prepare = new ProfileMeasurement("POST_PREPARE");
    /**
     * 2PC-FINISH
     */
    protected final ProfileMeasurement pm_post_finish = new ProfileMeasurement("POST_FINISH");
    /**
     * The amount of time spent committing or aborting a txn in the EE
     */
    protected final ProfileMeasurement pm_post_ee = new ProfileMeasurement("POST_EE");
    /**
     * The amount of time spent sending back the ClientResponse
     */
    protected final ProfileMeasurement pm_post_client = new ProfileMeasurement("POST_CLIENT");

    /**
     * Indicate that the txn is the post-processing stage. This should only be called after startExec() has been invoked
     */
    public void startPost() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        ProfileMeasurement current = null;
        while ((current = this.stack.pop()) != this.pm_exec_total) {
            // Keep this ball rollin'
            current.stop();
            if (trace.val)
                LOG.trace("-> STOPPED: " + current + "[" + current.hashCode() + "]");
        } // WHILE
        assert(current == this.pm_exec_total) : "Unexpected " + current;
        if (trace.val)
            LOG.trace("STATUS: " + current.debug() + "[" + current.hashCode() + "]");
        if (current.isStarted() == false) {
            this.pm_post_total.start();
        } else {
            ProfileMeasurementUtil.swap(current, this.pm_post_total);
        }
        this.stack.push(this.pm_post_total);
    }

    public void startPostPrepare() {
        if (this.disabled) return;
        this.startInner(this.pm_post_total, this.pm_post_prepare, false);
    }

    public void stopPostPrepare() {
        if (this.disabled) return;
        this.stopInner(this.pm_post_prepare, this.pm_post_total, false);
    }

    public void startPostFinish() {
        if (this.disabled) return;
        this.startInner(this.pm_post_total, this.pm_post_finish, false);
    }

    public void stopPostFinish() {
        if (this.disabled) return;
        this.stopInner(this.pm_post_finish, this.pm_post_total, false);
    }

    public void startPostEE() {
        if (this.disabled) return;
        this.pm_post_ee.start();
        // ProfileMeasurement parent = this.stack.peek();
        // this.startInner(parent, this.pm_post_ee, false);
    }

    public void stopPostEE() {
        if (this.disabled) return;
        this.pm_post_ee.stop();
        // ProfileMeasurement parent = this.stack.elementAt(this.stack.size() - 2);
        // this.stopInner(this.pm_post_ee, parent, false);
    }

    public void startPostClient() {
        if (this.disabled) return;
        this.pm_post_client.start();
        // ProfileMeasurement parent = this.stack.peek();
        // this.startInner(parent, this.pm_post_client, false);
    }

    public void stopPostClient() {
        if (this.disabled) return;
        this.pm_post_client.stop();
        // ProfileMeasurement parent = this.stack.elementAt(this.stack.size() - 2);
        // this.stopInner(this.pm_post_client, parent, false);
    }

    // ---------------------------------------------------------------
    // QUERY COUNTERS 
    // ---------------------------------------------------------------
    
    public void addBatch(int num_queries) {
        this.num_batches++;
        this.num_queries += num_queries;
    }
    
    public void addRemoteQuery(int num_queries) {
        this.num_remote_queries += num_queries;
    }
    public void addPrefetchQuery(int num_queries) {
        this.num_prefetched += num_queries;
    }
    public void addPrefetchUnusedQuery(int num_queries) {
        this.num_prefetched_unused += num_queries;
    }
//    public void addSpeculativeTransaction(int num_txns) {
//        this.num_speculative += num_txns;
//    }
    
    public int getBatchCount() {
        return (this.num_batches);
    }
    public int getQueryCount() {
        return (this.num_queries);
    }
    public int getRemoteQueryCount() {
        return (this.num_remote_queries);
    }
    public int getPrefetchQueryCount() {
        return (this.num_prefetched);
    }
    public int getPrefetchQueryUnusedCount() {
        return (this.num_prefetched_unused);
    }
//    public int getSpeculativeTransactionCount() {
//        return (this.num_speculative);
//    }
    
    // ---------------------------------------------------------------
    // UTILITY METHODS
    // ---------------------------------------------------------------

    @Override
    public long[] getTuple() {
        // If the txn never actually submitted a remote query, then
        // we'll want to stop the timer now and clear it out.
        if (this.pm_first_remote_query.isStarted()) {
            this.pm_first_remote_query.clear();
        }
        return super.getTuple();
    }

    @Override
    public void copy(AbstractProfiler other) {
        assert(other instanceof TransactionProfiler);
        super.copy(other);

        // Stop anything that is already started
        long timestamp = -1;
        for (ProfileMeasurement pm : this.getProfileMeasurements()) {
            if (pm.isStarted() && pm != this.pm_total && pm != this.pm_exec_total) {
                if (timestamp == -1)
                    timestamp = ProfileMeasurement.getTime();
                pm.stop(timestamp);
            }
        } // FOR
        this.pm_total.reset();
        this.pm_exec_total.reset();

        TransactionProfiler otherProfiler = (TransactionProfiler) other;
        this.startTransaction(otherProfiler.pm_total.getMarker());
        this.setSingledPartitioned(otherProfiler.singlePartitioned);
    }

    @Override
    public void finish() {
        for (ProfileMeasurement pm : this.getProfileMeasurements()) {
            pm.clear();
        } // FOR
        this.stack.clear();
        this.history.clear();
        this.disabled = false;
        this.num_batches = 0;
        this.num_queries = 0;
        this.num_remote_queries = 0;
        this.num_prefetched = 0;
        this.num_prefetched_unused = 0;
//        this.num_speculative = 0;
    }

    /**
     * Disable all profiling for this transaction
     */
    public void disableProfiling() {
        if (trace.val) LOG.trace("Disabling transaction profiling");
        this.disabled = true;
    }

    /**
     * Enable profiling for this transaction This should only be invoked before the txn starts
     */
    public void enableProfiling() {
        if (trace.val) LOG.trace("Enabling transaction profiling");
        this.disabled = false;
    }

    public boolean isDisabled() {
        return (this.disabled);
    }

    /**
     * Return the topmost ProfileMeasurement handle on this profiler's stack
     */
    public ProfileMeasurement current() {
        return (this.stack.peek());
    }

    /**
     * Returns the history of actions for this profiler
     */
    public Collection<ProfileMeasurement> history() {
        return (Collections.unmodifiableCollection(this.history));
    }

    public boolean isStopped() {
        return (this.pm_total.isStarted() == false);
    }

    public void setSingledPartitioned(boolean val) {
        this.singlePartitioned = val;
    }

    public boolean isSinglePartitioned() {
        return (this.singlePartitioned);
    }
    
    public void markEarly2PCPartition(int partition) {
        this.early_2pc_partitions.add(partition);
    }
    public PartitionSet getEarlyPreparePartitions() {
        return (this.early_2pc_partitions);
    }
    
    @Override
    public boolean isInitialized() {
        return true;
    }
    
    @Override
    public Map<String, Object> debugMap() {
        Map<String, Object> m = super.debugMap();
        m.put("Single-Partitioned", this.singlePartitioned);
        m.put("# of Batches", this.num_batches);
        m.put("# of Queries", this.num_queries);
        m.put("# of Remote Queries", this.num_queries);
        m.put("# of Prefetched Queries", this.num_prefetched);
        m.put("# of Unused Prefetched Queries", this.num_prefetched_unused);
//        m.put("# of Speculative Txns", this.num_speculative);

        // HISTORY
        String history = "";
        int i = 0;
        for (ProfileMeasurement pm : this.history) {
            String label = pm.getName();
            if (pm.isStarted()) {
                label += " *ACTIVE*";
            }
            history += String.format("[%02d] %s\n", i++, label);
        } // FOR
        m.put("History", history);

        return (m);
    }
}