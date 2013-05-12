package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableNonBlocking;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction.RoundState;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

public class DependencyTracker {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    
    /**
     * Special set to indicate that there are no more WorkFragments to be executed
     */
    private static final Set<WorkFragment.Builder> EMPTY_FRAGMENT_SET = Collections.emptySet();

    /**
     * Internal Dependency Information
     */
    private class TransactionState {
        
        // ----------------------------------------------------------------------------
        // GLOBAL DATA MEMBERS
        // ----------------------------------------------------------------------------
        
        /**
         * The id of the current transaction that holds this state handle
         */
        private Long txn_id;
        
        // ----------------------------------------------------------------------------
        // ROUND DATA MEMBERS
        // ----------------------------------------------------------------------------
        
        /**
         * This latch will block until all the Dependency results have returned
         * Generated in startRound()
         */
        private CountDownLatch dependency_latch;
        
        /**
         * Mapping from DependencyId to the corresponding DependencyInfo object
         * Map<DependencyId, DependencyInfo>
         */
        private final Map<Integer, DependencyInfo> dependencies = new HashMap<Integer, DependencyInfo>();
        
        /**
         * Final result output dependencies. Each position in the list represents a single Statement
         */
        private final List<Integer> output_order = new ArrayList<Integer>();
        
        /**
         * As information come back to us, we need to keep track of what SQLStmt we are storing 
         * the data for. Note that we have to maintain two separate lists for results and responses
         * PartitionId -> DependencyId -> Next SQLStmt Index
         */
        private final Map<Pair<Integer, Integer>, Queue<Integer>> results_dependency_stmt_ctr = new HashMap<Pair<Integer,Integer>, Queue<Integer>>();
        
        /**
         * Internal cache of the result queues that were used by the txn in this round.
         * This is so that we don't have to clear all of the queues in the entire results_dependency_stmt_ctr cache. 
         */
        private final Collection<Queue<Integer>> results_queue_cache = new ArrayList<Queue<Integer>>();
        
        /**
         * Sometimes we will get results back while we are still queuing up the rest of the tasks and
         * haven't started the next round. So we need a temporary space where we can put these guys until 
         * we start the round. Otherwise calculating the proper latch count is tricky
         * Partition-DependencyId Key -> VoltTable
         */
        private final Map<Pair<Integer, Integer>, VoltTable> queued_results = new LinkedHashMap<Pair<Integer,Integer>, VoltTable>();
        
        /**
         * Blocked FragmentTaskMessages
         */
        private final List<WorkFragment.Builder> blocked_tasks = new ArrayList<WorkFragment.Builder>();
        
        /**
         * Unblocked FragmentTaskMessages
         * The VoltProcedure thread will block on this queue waiting for tasks to execute inside of ExecutionSite
         * This has to be a set so that we make sure that we only submit a single message that contains all of the tasks to the Dtxn.Coordinator
         */
        private final LinkedBlockingDeque<Collection<WorkFragment.Builder>> unblocked_tasks = new LinkedBlockingDeque<Collection<WorkFragment.Builder>>(); 
        
        /**
         * Whether the current transaction still has outstanding WorkFragments that it
         * needs to execute or get back dependencies from
         */
        private boolean still_has_tasks = true;
        
        /**
         * The total # of dependencies this Transaction is waiting for in the current round
         */
        private int dependency_ctr = 0;
        
        /**
         * The total # of dependencies received thus far in the current round
         */
        private int received_ctr = 0;
        
        
        private TransactionState(LocalTransaction ts) {
            this.txn_id = ts.getTransactionId();
        }
        
        
        /**
         * 
         * @param d_id Output Dependency Id
         * @return
         */
        protected DependencyInfo getDependencyInfo(int d_id) {
            return (this.dependencies.get(d_id));
        }
        
        public void clear() {
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
            
            this.dependency_ctr = 0;
            this.received_ctr = 0;
        }
        
    }
    
    private final PartitionExecutor executor;
    private final Map<LocalTransaction, TransactionState> txnStates = new IdentityHashMap<>();
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public DependencyTracker(PartitionExecutor executor) {
        this.executor = executor;
    }
    
    public void addTransaction(LocalTransaction ts) {
        // FIXME
        TransactionState state = new TransactionState(ts);
        this.txnStates.put(ts, state);
    }
    
    public void removeTransaction(LocalTransaction ts) {
        // FIXME
        this.txnStates.remove(ts);
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    protected void initRound(LocalTransaction ts) {
        TransactionState state = this.txnStates.get(ts);
        assert(state != null) :
            String.format("Unexpected null %s handle for %s",
                          TransactionState.class.getSimpleName(), ts);
        
        assert(state.queued_results.isEmpty()) : 
            String.format("Trying to initialize ROUND #%d for %s but there are %d queued results",
                           ts.getCurrentRound(ts.getBasePartition()),
                           ts, state.queued_results.size());
        // if (this.getLastUndoToken(partition) != HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
        state.clear();
        // }
    }
    
    protected void startRound(LocalTransaction ts) {
        TransactionState state = this.txnStates.get(ts);
        assert(state != null) :
            String.format("Unexpected null %s handle for %s",
                          TransactionState.class.getSimpleName(), ts);
        
        final int basePartition = ts.getBasePartition();
        final int currentRound = ts.getCurrentRound(basePartition);
        final int batch_size = ts.getCurrentBatchSize();
        
        // Create our output counters
        assert(state.output_order.isEmpty());
        for (int stmt_index = 0; stmt_index < batch_size; stmt_index++) {
            if (trace.val)
                LOG.trace(String.format("%s - Examining %d dependencies at stmt_index %d",
                          ts, state.dependencies.size(), stmt_index));
            for (DependencyInfo dinfo : state.dependencies.values()) {
                // Add this DependencyInfo our output list if it's being used in this round for this txn
                // and if it is not an internal dependency
                if (dinfo.inSameTxnRound(ts.getTransactionId(), currentRound) &&
                    dinfo.isInternal() == false && dinfo.getStatementIndex() == stmt_index) {
                    state.output_order.add(dinfo.getDependencyId());
                }
            } // FOR
        } // FOR
        assert(batch_size == state.output_order.size()) :
            String.format("%s - Expected %d output dependencies but we queued up %d\n%s",
                          ts, batch_size, state.output_order.size(),
                          StringUtil.join("\n", state.output_order));
        
        // Release any queued responses/results
        if (state.queued_results.isEmpty() == false) {
            if (trace.val)
                LOG.trace(String.format("%s - Releasing %d queued results",
                          ts, state.queued_results.size()));
            for (Entry<Pair<Integer, Integer>, VoltTable> e : state.queued_results.entrySet()) {
                this.addResult(ts, e.getKey(), e.getValue(), true);
            } // FOR
            state.queued_results.clear();
        }
        
        // Now create the latch
        int count = state.dependency_ctr - state.received_ctr;
        assert(count >= 0);
        assert(state.dependency_latch == null) : "This should never happen!\n" + ts.debug();
        state.dependency_latch = new CountDownLatch(count);
    }
    
    protected void finishRound(LocalTransaction ts) {
        TransactionState state = this.txnStates.get(ts);
        assert(state != null) :
            String.format("Unexpected null %s handle for %s",
                          TransactionState.class.getSimpleName(), ts);
        
        assert(state.dependency_ctr == state.received_ctr) :
            String.format("Trying to finish ROUND #%d on partition %d for %s before it was started",
                          ts.getCurrentRound(ts.getBasePartition()),
                          ts.getBasePartition(), ts);
        assert(state.queued_results.isEmpty()) :
            String.format("Trying to finish ROUND #%d on partition %d for %s but there are %d queued results",
                          ts.getCurrentRound(ts.getBasePartition()),
                          ts.getBasePartition(), ts, state.queued_results.size());
        
        // Reset our initialization flag so that we can be ready to run more stuff the next round
        if (state.dependency_latch != null) {
            assert(state.dependency_latch.getCount() == 0);
            if (trace.val)
                LOG.debug("Setting CountDownLatch to null for " + ts);
            state.dependency_latch = null;
        }
        state.clear();
    }
    
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------

    private TransactionState getState(LocalTransaction ts) {
        TransactionState state = this.txnStates.get(ts);
        assert(state != null) :
            String.format("Unexpected null %s handle for %s",
                          TransactionState.class.getSimpleName(), ts);
        return (state);
    }
    
    /**
     * 
     * @param state
     * @param currentRound
     * @param stmt_index
     * @param dep_id
     * @return
     */
    private DependencyInfo getOrCreateDependencyInfo(TransactionState state,
                                                     int currentRound,
                                                     int stmt_index,
                                                     Integer dep_id) {
        Map<Integer, DependencyInfo> stmt_dinfos = state.dependencies;
        DependencyInfo dinfo = stmt_dinfos.get(dep_id);
        
        if (dinfo != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Reusing DependencyInfo[%d] for %s. " +
                          "Checking whether it needs to be reset [currentRound=%d / lastRound=%d lastTxn=%s]",
                          this, dinfo.hashCode(), TransactionUtil.debugStmtDep(stmt_index, dep_id),
                          currentRound, dinfo.getRound(), dinfo.getTransactionId()));
            if (dinfo.inSameTxnRound(state.txn_id, currentRound) == false) {
                if (debug.val) LOG.debug(String.format("%s - Clearing out DependencyInfo[%d].",
                                         this, dinfo.hashCode()));
                dinfo.finish();
            }
        } else {
            dinfo = new DependencyInfo(this.executor.getCatalogContext());
            stmt_dinfos.put(dep_id, dinfo);
            if (debug.val)
                LOG.debug(String.format("%s - Created new DependencyInfo for %s [hashCode=%d]",
                          this, TransactionUtil.debugStmtDep(stmt_index, dep_id), dinfo.hashCode()));
        }
        if (dinfo.isInitialized() == false) {
            dinfo.init(state.txn_id, currentRound, stmt_index, dep_id.intValue());
        }
        
        return (dinfo);
    }
    
    /**
     * Keep track of a new output dependency from the given partition that corresponds
     * to the SQL statement executed at the given offset.
     * @param partition
     * @param output_dep_id
     * @param stmt_index
     */
    private void addResultDependencyStatement(TransactionState state, int partition, int output_dep_id, int stmt_index) {
        Pair<Integer, Integer> key = Pair.of(partition, output_dep_id);
        Queue<Integer> rest_stmt_ctr = state.results_dependency_stmt_ctr.get(key);
        if (rest_stmt_ctr == null) {
            rest_stmt_ctr = new LinkedList<Integer>();
            state.results_dependency_stmt_ctr.put(key, rest_stmt_ctr);
        }
        rest_stmt_ctr.add(stmt_index);
        state.results_queue_cache.add(rest_stmt_ctr);
        if (debug.val)
            LOG.debug(String.format("%d - Set Dependency Statement Counters for <%d %d>: %s",
                      state.txn_id, partition, output_dep_id, rest_stmt_ctr));
    }
    
    // ----------------------------------------------------------------------------
    // DEPENDENCY TRACKING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Get the final results of the last round of execution for this Transaction
     * This should only be called to get the VoltTables that you want to send into
     * the Java stored procedure code (e.g., the return value for voltExecuteSql())
     * @return
     */
    public VoltTable[] getResults(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        final VoltTable results[] = new VoltTable[state.output_order.size()];
        if (debug.val)
            LOG.debug(String.format("%s - Generating output results with %d tables",
                      this, results.length));
        
        HStoreConf hstore_conf = this.executor.getHStoreConf();
        boolean nonblocking = (hstore_conf.site.specexec_nonblocking &&
                               ts.isSysProc() == false &&
                               ts.profiler != null);
        for (int stmt_index = 0; stmt_index < results.length; stmt_index++) {
            Integer dependency_id = state.output_order.get(stmt_index);
            assert(dependency_id != null) :
                "Null output dependency id for Statement index " + stmt_index + " in txn #" + state.txn_id;
//            assert(this.state.dependencies[stmt_index] != null) :
//                "Missing dependency set for stmt_index #" + stmt_index + " in txn #" + this.txn_id;
            assert(state.dependencies.containsKey(dependency_id)) :
                String.format("Missing info for %s in %s",
                              TransactionUtil.debugStmtDep(stmt_index, dependency_id), this); 
            
            VoltTable vt = state.dependencies.get(dependency_id).getResult();

            // Special Non-Blocking Wrapper
            if (nonblocking) {
                VoltTableNonBlocking vtnb = new VoltTableNonBlocking(hstore_conf.site.txn_profiling ? ts.profiler : null);
                if (vt != null) vtnb.setRealTable(vt);
                results[stmt_index] = vtnb;
            } else {
                assert(vt != null) : 
                    String.format("Null output result for Statement index %d in %s", stmt_index, this); 
                results[stmt_index] = vt;
            }
        } // FOR
        return (results);
    }
    
    /**
     * Queues up a WorkFragment for this txn. If the return value is true, 
     * then the WorkFragment is blocked waiting for dependencies.
     * If the return value is false, then the WorkFragment can be executed 
     * immediately (either locally or on at a remote partition).
     * @param ts
     * @param fragment
     * @return
     */
    public boolean addWorkFragment(LocalTransaction ts, WorkFragment.Builder fragment) {
        final TransactionState state = this.getState(ts);
        assert(ts.getCurrentRoundState(ts.getBasePartition()) == RoundState.INITIALIZED) :
            String.format("Invalid round state %s for %s at partition %d",
                          ts.getCurrentRoundState(ts.getBasePartition()),
                          this, ts.getBasePartition());
        
        boolean blocked = false;
        final int partition = fragment.getPartitionId();
        final int num_fragments = fragment.getFragmentIdCount();
        final int currentRound = ts.getCurrentRound(ts.getBasePartition());
        
        if (debug.val)
            LOG.debug(String.format("%s - Adding %s for partition %d with %d fragments",
                      this, fragment.getClass().getSimpleName(), partition, num_fragments));
        
        // PAVLO: 2011-12-10
        // We moved updating the exec_touchedPartitions histogram into the
        // BatchPlanner so that we won't increase the counter for a partition
        // if we read from a replicated table at the local partition
        // this.state.exec_touchedPartitions.put(partition, num_fragments);
        
        // PAVLO 2011-12-20
        // I don't know why, but before this loop used to be synchronized
        // It definitely does not need to be because this is only invoked by the
        // transaction's base partition PartitionExecutor
        for (int i = 0; i < num_fragments; i++) {
            int stmt_index = fragment.getStmtIndex(i);
//            int param_index = fragment.getParamIndex(i);
            
            // If this task produces output dependencies, then we need to make 
            // sure that the txn wait for it to arrive first
            int output_dep_id = fragment.getOutputDepId(i);
            if (output_dep_id != HStoreConstants.NULL_DEPENDENCY_ID) {
                DependencyInfo dinfo = this.getOrCreateDependencyInfo(state, currentRound, stmt_index, output_dep_id);
                dinfo.addPartition(partition);
                if (debug.val)
                    LOG.debug(String.format("%s - Adding new DependencyInfo %s for PlanFragment %d at Partition %d [ctr=%d]\n%s",
                              this, TransactionUtil.debugStmtDep(stmt_index, output_dep_id),
                              fragment.getFragmentId(i), state.dependency_ctr,
                              partition, dinfo.toString()));
                // Store the stmt_index of when this dependency will show up
                state.dependency_ctr++;
                this.addResultDependencyStatement(state, partition, output_dep_id, stmt_index);
            } // IF
            
            // If this WorkFragment needs an input dependency, then we need to make sure it arrives at
            // the executor before it is allowed to start executing
            if (fragment.getNeedsInput()) {
                int dependency_id = fragment.getInputDepId(i);
                if (dependency_id != HStoreConstants.NULL_DEPENDENCY_ID) {
                    DependencyInfo dinfo = this.getOrCreateDependencyInfo(state, currentRound, stmt_index, dependency_id);
                    dinfo.addBlockedWorkFragment(fragment);
                    dinfo.markInternal();
                    if (blocked == false) {
                        state.blocked_tasks.add(fragment);
                        blocked = true;   
                    }
                    if (debug.val)
                        LOG.debug(String.format("%s - Created internal input dependency %d for PlanFragment %d\n%s", 
                                  this, dependency_id, fragment.getFragmentId(i), dinfo.toString()));
                }
            }
            
            // *********************************** DEBUG ***********************************
            if (trace.val) {
                StringBuilder sb = new StringBuilder();
                int output_ctr = 0;
                int dep_ctr = 0;
                for (DependencyInfo dinfo : state.dependencies.values()) {
                    if (dinfo.getStatementIndex() == stmt_index) dep_ctr++;
                    if (dinfo.isInternal() == false) {
                        output_ctr++;
                        sb.append("  Output -> " + dinfo.toString());
                    }
                } // FOR
                LOG.trace(String.format("%s - Number of Output Dependencies for StmtIndex #%d: %d out of %d\n%s", 
                          this, stmt_index, output_ctr, dep_ctr, sb));
            }
            // *********************************** DEBUG ***********************************
            
        } // FOR

        // *********************************** DEBUG ***********************************
        if (debug.val) {
            CatalogType catalog_obj = null;
            if (ts.isSysProc()) {
                catalog_obj = ts.getProcedure();
            } else {
                for (int i = 0; i < num_fragments; i++) {
                    int frag_id = fragment.getFragmentId(i);
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(ts.getProcedure(), frag_id);
                    catalog_obj = catalog_frag.getParent();
                    if (catalog_obj != null) break;
                } // FOR
            }
            LOG.debug(String.format("%s - Queued up %s WorkFragment for partition %d and marked as %s [fragIds=%s]",
                      this, catalog_obj, partition,
                      (blocked ? "blocked" : "not blocked"),
                      fragment.getFragmentIdList()));
            if (trace.val)
                LOG.trace("WorkFragment Contents for " + ts + ":\n" + fragment);
        }
        // *********************************** DEBUG ***********************************
        
        return (blocked);
    }
    
    /**
     * 
     * @param partition
     * @param dependency_id
     * @param result
     */
    public void addResult(LocalTransaction ts, int partition, int dependency_id, VoltTable result) {
        assert(result != null) :
            String.format("%s - The result for DependencyId %d from partition %d is null",
                          ts, dependency_id, partition);
        this.addResult(ts, Pair.of(partition, dependency_id), result, false);
    }

    /**
     * Store a VoltTable result that this transaction is waiting for.
     * @param key The hackish partition+dependency key
     * @param result The actual data for the result
     * @param force If false, then we will check to make sure the result isn't a duplicate
     * @param partition The partition id that generated the result
     * @param dependency_id The dependency id that this result corresponds to
     */
    private void addResult(final LocalTransaction ts,
                           final Pair<Integer, Integer> key,
                           final VoltTable result,
                           final boolean force) {
        final TransactionState state = this.getState(ts);
        assert(result != null);
        
        final ReentrantLock stateLock = ts.getTransactionLock();
        final int base_partition = ts.getBasePartition();
        final int partition = key.getFirst().intValue();
        final int dependency_id = key.getSecond().intValue();
        final RoundState roundState = ts.getCurrentRoundState(base_partition); 
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        
        assert(roundState == RoundState.INITIALIZED || roundState == RoundState.STARTED) :
            String.format("Invalid round state %s for %s at partition %d",
                          roundState, ts, base_partition);
        
        if (debug.val)
            LOG.debug(String.format("%s - Attemping to add new result for %s [numRows=%d]",
                      ts, TransactionUtil.debugPartDep(partition, dependency_id), result.getRowCount()));
        
        // If the txn is still in the INITIALIZED state, then we just want to queue up the results
        // for now. They will get released when we switch to STARTED 
        // This is the only part that we need to synchonize on
        if (force == false) {
            if (singlePartitioned == false) stateLock.lock();
            try {
                if (roundState == RoundState.INITIALIZED) {
                    assert(state.queued_results.containsKey(key) == false) : 
                        String.format("%s - Duplicate result %s",
                                      ts, TransactionUtil.debugPartDep(partition, dependency_id));
                    state.queued_results.put(key, result);
                    if (debug.val)
                        LOG.debug(String.format("%s - Queued result %s until the round is started",
                                  ts, TransactionUtil.debugPartDep(partition, dependency_id)));
                    return;
                }
                if (debug.val) {
                    LOG.debug(String.format("%s - Storing new result for key %s", ts, key));
                    // if (trace.val) LOG.trace("Result stmt_ctr(key=" + key + "): " + this.state.results_dependency_stmt_ctr.get(key));
                }
            } finally {
                if (singlePartitioned == false) stateLock.unlock();
            } // SYNCH
        }
            
        // Each partition+dependency_id should be unique within the Statement batch.
        // So as the results come back to us, we have to figure out which Statement it belongs to
        DependencyInfo dinfo = null;
        Queue<Integer> queue = null;
        int stmt_index;
        try {
            queue = state.results_dependency_stmt_ctr.get(key);
            assert(queue != null) :
                String.format("Unexpected %s in %s / %s\n%s",
                              TransactionUtil.debugPartDep(partition, dependency_id), ts,
                              key, state.results_dependency_stmt_ctr);
            assert(queue.isEmpty() == false) :
                String.format("No more statements for %s in %s\nresults_dependency_stmt_ctr = %s",
                              TransactionUtil.debugPartDep(partition, dependency_id), ts,
                              state.results_dependency_stmt_ctr);

            stmt_index = queue.remove().intValue();
            dinfo = state.getDependencyInfo(dependency_id);
            assert(dinfo != null) :
                String.format("Unexpected %s for %s [stmt_index=%d]\n%s",
                              TransactionUtil.debugPartDep(partition, dependency_id), ts, stmt_index, result);
        } catch (NullPointerException ex) {
            // HACK: IGNORE!
        }
        if (dinfo == null) {
            // HACK: IGNORE!
            return;
        }
        dinfo.addResult(partition, result);
        
        if (singlePartitioned == false) stateLock.lock();
        try {
            state.received_ctr++;
            
            // Check whether we need to start running stuff now
            // 2011-12-31: This needs to be synchronized because they might check
            //             whether there are no more blocked tasks before we 
            //             can add to_unblock to the unblocked_tasks queue
            if (state.blocked_tasks.isEmpty() == false && dinfo.hasTasksReady()) {
                Collection<WorkFragment.Builder> to_unblock = dinfo.getAndReleaseBlockedWorkFragments();
                assert(to_unblock != null);
                assert(to_unblock.isEmpty() == false);
                if (debug.val)
                    LOG.debug(String.format("%s - Got %d WorkFragments to unblock that were waiting for DependencyId %d",
                               ts, to_unblock.size(), dinfo.getDependencyId()));
                state.blocked_tasks.removeAll(to_unblock);
                state.unblocked_tasks.addLast(to_unblock);
            }
            else if (debug.val) {
                LOG.debug(String.format("%s - No WorkFragments to unblock after storing %s " +
                          "[blockedTasks=%d, hasTasksReady=%s]",
                          ts, TransactionUtil.debugPartDep(partition, dependency_id),
                          state.blocked_tasks.size(), dinfo.hasTasksReady()));
            }
        
            if (state.dependency_latch != null) {    
                state.dependency_latch.countDown();
                    
                // HACK: If the latch is now zero, then push an EMPTY set into the unblocked queue
                // This will cause the blocked PartitionExecutor thread to wake up and realize that he's done
                if (state.dependency_latch.getCount() == 0) {
                    if (debug.val)
                        LOG.debug(String.format("%s - Pushing EMPTY_SET to PartitionExecutor at partition %d " +
                                  "because all the dependencies have arrived!",
                                  ts, partition));
                    state.unblocked_tasks.addLast(EMPTY_FRAGMENT_SET);
                }
                if (debug.val)
                    LOG.debug(String.format("%s - Setting CountDownLatch to %d for partition %d ",
                              ts, state.dependency_latch.getCount(), partition));
            }

            state.still_has_tasks = (state.blocked_tasks.isEmpty() == false ||
                                     state.unblocked_tasks.isEmpty() == false);
        } finally {
            if (singlePartitioned == false) stateLock.unlock();
        } // SYNCH
        
        if (debug.val) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Blocked Tasks", (state != null ? state.blocked_tasks.size() : null));
            m.put("DependencyInfo", dinfo.toString());
            m.put("hasTasksReady", dinfo.hasTasksReady());
            LOG.debug(this + " - Status Information\n" + StringUtil.formatMaps(m));
            if (trace.val) LOG.trace(ts.debug());
        }
    }

    /**
     * Populate the given map with the the dependency results that are used for
     * internal plan execution. Note that these are not the results that should be
     * sent to the client.
     * @param fragment
     * @param results
     * @return
     */
    public Map<Integer, List<VoltTable>> removeInternalDependencies(final LocalTransaction ts,
                                                                    final WorkFragment fragment,
                                                                    final Map<Integer, List<VoltTable>> results) {
        if (debug.val)
            LOG.debug(String.format("%s - Retrieving %d internal dependencies for %s WorkFragment:\n%s",
                      ts, fragment.getInputDepIdCount(), fragment));

        final TransactionState state = this.getState(ts);
        for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
            int stmt_index = fragment.getStmtIndex(i);
            int input_d_id = fragment.getInputDepId(i);
            if (input_d_id == HStoreConstants.NULL_DEPENDENCY_ID) continue;
            
            DependencyInfo dinfo = state.getDependencyInfo(input_d_id);
            assert(dinfo != null);
            assert(dinfo.getPartitionCount() == dinfo.getResultsCount()) :
                String.format("%s - Number of results retrieved for %s is %d " +
                              "but we were expecting %d\n%s\n%s\n%s",
                              ts, TransactionUtil.debugStmtDep(stmt_index, input_d_id),
                              dinfo.getResultsCount(), dinfo.getPartitionCount(),
                              fragment.toString(),
                              StringUtil.SINGLE_LINE, ts.debug()); 
            results.put(input_d_id, dinfo.getResults());
            if (debug.val)
                LOG.debug(String.format("%s - %s -> %d VoltTables",
                          ts, TransactionUtil.debugStmtDep(stmt_index, input_d_id),
                          results.get(input_d_id).size()));
        } // FOR
        return (results);
    }
    
    /**
     * 
     * @param ts
     * @param input_d_id
     * @return
     */
    public List<VoltTable> getInternalDependency(final LocalTransaction ts, final Integer input_d_id) {
        if (debug.val)
            LOG.debug(String.format("%s - Retrieving internal dependencies for Dependency %d",
                      ts, input_d_id));

        final TransactionState state = this.getState(ts);
        DependencyInfo dinfo = state.getDependencyInfo(input_d_id);
        assert(dinfo != null) :
            String.format("No DependencyInfo object for Dependency %d in %s",
                          input_d_id, this);
        assert(dinfo.isInternal()) :
            String.format("The DependencyInfo for Dependency %s in %s is not marked as internal",
                          input_d_id, this);
        assert(dinfo.getPartitionCount() == dinfo.getResultsCount()) :
                    String.format("Number of results from partitions retrieved for Dependency %s " +
                                  "is %d but we were expecting %d in %s\n%s\n%s%s", 
                                  input_d_id, dinfo.getResultsCount(), dinfo.getPartitionCount(), ts,
                                  this.toString(), StringUtil.SINGLE_LINE, ts.debug()); 
        return (dinfo.getResults());
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    public void unblock(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        try {
            // And then shove an empty result at them
            state.unblocked_tasks.addLast(EMPTY_FRAGMENT_SET);
            
            // Spin through this so that the waiting thread wakes up and sees that they got an error
            if (state.dependency_latch != null) {
                while (state.dependency_latch.getCount() > 0) {
                    state.dependency_latch.countDown();
                } // WHILE
            }
        } catch (NullPointerException ex) {
            // HACK!
        }
    }
    
    
    public LinkedBlockingDeque<Collection<WorkFragment.Builder>> getUnblockedWorkFragmentsQueue(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        return (state.unblocked_tasks);
    }
    
    
    /**
     * Return the latch that will block the PartitionExecutor's thread until
     * all of the query results have been retrieved for this transaction's
     * current SQLStmt batch
     */
    public CountDownLatch getDependencyLatch(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        return state.dependency_latch;
    }
    
    /**
     * Returns true if this transaction still has WorkFragments
     * that need to be dispatched to the appropriate PartitionExecutor 
     * @return
     */
    public boolean stillHasWorkFragments(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        return (state.still_has_tasks);
    }
    
    /**
     * Returns true if the given WorkFragment is currently set as blocked for this txn
     * @param ftask
     * @return
     */
    public boolean isBlocked(LocalTransaction ts, WorkFragment.Builder ftask) {
        final TransactionState state = this.getState(ts);
        return (state.blocked_tasks.contains(ftask));
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG STUFF
    // ----------------------------------------------------------------------------
    
    public class Debug implements DebugContext {
        public DependencyInfo getDependencyInfo(LocalTransaction ts, int d_id) {
            final TransactionState state = getState(ts);
            return (state.dependencies.get(d_id));
        }
        public Collection<DependencyInfo> getAllDependencies(LocalTransaction ts) {
            final TransactionState state = getState(ts);
            return (state.dependencies.values());
        }
        public int getDependencyCount(LocalTransaction ts) { 
            final TransactionState state = getState(ts);
            return (state.dependency_ctr);
        }
        public Collection<WorkFragment.Builder> getBlockedWorkFragments(LocalTransaction ts) {
            final TransactionState state = getState(ts);
            return (state.blocked_tasks);
        }
        public List<Integer> getOutputOrder(LocalTransaction ts) {
            final TransactionState state = getState(ts);
            return (state.output_order);
        }
        public Map<Integer, DependencyInfo> getStatementDependencies(LocalTransaction ts, int stmt_index) {
            final TransactionState state = getState(ts);
            return (state.dependencies);
        }
        
        public Map<String, Object> getDebugMap(LocalTransaction ts) {
            final TransactionState state = getState(ts);
            
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Dependency Ctr", state.dependency_ctr);
            m.put("Received Ctr", state.received_ctr);
            m.put("CountdownLatch", state.dependency_latch);
            m.put("# of Blocked Tasks", state.blocked_tasks.size());
            m.put("# of Statements", ts.getCurrentBatchSize());
            m.put("Expected Results", state.results_dependency_stmt_ctr.keySet());
            
            return (m);
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
