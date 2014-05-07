package edu.brown.hstore.txns;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableNonBlocking;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction.RoundState;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

/**
 * This class is responsible for managing the input and output dependencies of distributed
 * transactions. It contains logic to creating blocking data structures that are released
 * once the appropriate VoltTables arrive for queries executed on remote partitions.
 * @author pavlo
 *
 */
public class DependencyTracker {
    private static final Logger LOG = Logger.getLogger(DependencyTracker.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
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
        private final BlockingDeque<Collection<WorkFragment.Builder>> unblocked_tasks = new LinkedBlockingDeque<Collection<WorkFragment.Builder>>(); 
        
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
        
        // ----------------------------------------------------------------------------
        // PREFETCH QUERY DATA
        // ----------------------------------------------------------------------------
        
        // private QueryTracker prefetch_tracker;
        
        /**
         * SQLStmt Counter -> FragmentId -> DependencyInfo
         */
        private Map<Integer, Map<Integer, DependencyInfo>> prefetch_dependencies;
        
        /**
         * The total # of WorkFragments that the txn prefetched
         */
        private int prefetch_ctr = 0;
        
        // ----------------------------------------------------------------------------
        // INITIALIZATION
        // ----------------------------------------------------------------------------
        
        private TransactionState(LocalTransaction ts) {
            this.txn_id = ts.getTransactionId();
            
            if (ts.hasPrefetchQueries()) {
//                this.prefetch_tracker = new QueryTracker();
                this.prefetch_dependencies = new HashMap<Integer, Map<Integer,DependencyInfo>>();
            }
        }
        
        
        /**
         * 
         * @param d_id Output Dependency Id
         * @return
         */
        protected DependencyInfo getDependencyInfo(int d_id) {
            return (this.dependencies.get(d_id));
        }
        
        /**
         * Clear the dependency information for a single SQLStmt batch round.
         * We will clear out the prefetch information because we need that
         * until the transaction is finished.
         */
        public void clear() {
            if (trace.val)
                LOG.trace("Clearing out internal state for " + this);
            
            this.dependencies.clear();
            this.output_order.clear();
            this.queued_results.clear();
            this.blocked_tasks.clear();
            this.unblocked_tasks.clear();
            this.still_has_tasks = true;

            this.dependency_ctr = 0;
            this.received_ctr = 0;
        }
        
        @Override
        public String toString() {
            return String.format("%s{#%d}", this.getClass().getSimpleName(), this.txn_id);
        }
        
        public Map<String, Object> debugMap() {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            for (Field f : this.getClass().getDeclaredFields()) {
                Object obj = null;
                try {
                    obj = f.get(this);
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException(ex);
                }
                // Skip parent reference
                if (obj instanceof DependencyTracker) continue;
                
                if (obj != null && obj == this.dependencies) {
                    Map<Integer, Object> inner = new TreeMap<Integer, Object>();
                    for (Entry<Integer, DependencyInfo> e : this.dependencies.entrySet()) {
                        inner.put(e.getKey(), e.getValue().debug());
                    }
                    obj = inner;
                }
                else if (obj != null && obj == this.prefetch_dependencies) {
                    Map<Integer, Object> inner = new TreeMap<Integer, Object>();
                    for (Integer stmtCounter : this.prefetch_dependencies.keySet()) {
                        Map<Integer, Object> stmtDeps = new LinkedHashMap<Integer, Object>();
                        for (Entry<Integer, DependencyInfo> e : this.prefetch_dependencies.get(stmtCounter).entrySet()) {
                            stmtDeps.put(e.getKey(), e.getValue().debug());
                        } // FOR
                        inner.put(stmtCounter, stmtDeps);
                    } // FOR
                    obj = inner;
                }
                m.put(StringUtil.title(f.getName().replace("_", " ")), obj);
            } // FOR
            return (m);
        }
    } // CLASS
    
    private final PartitionExecutor executor;
    private final CatalogContext catalogContext;
    private final Map<Long, TransactionState> txnStates = new ConcurrentHashMap<Long, TransactionState>();
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public DependencyTracker(PartitionExecutor executor) {
        this.executor = executor;
        this.catalogContext = this.executor.getCatalogContext();
    }
    
    public void addTransaction(LocalTransaction ts) {
        if (this.txnStates.containsKey(ts.getTransactionId())) {
            return;
        }
        
        // FIXME
        TransactionState state = new TransactionState(ts);
        this.txnStates.put(ts.getTransactionId(), state);
        if (trace.val)
            LOG.trace(String.format("Added %s to %s", ts, this));
    }
    
    public void removeTransaction(LocalTransaction ts) {
        // FIXME
        TransactionState state = this.txnStates.remove(ts.getTransactionId());
        if (trace.val && state != null) {
            LOG.trace(String.format("Removed %s from %s", ts, this));
        }
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    protected void initRound(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        assert(state.queued_results.isEmpty()) : 
            String.format("Trying to initialize ROUND #%d for %s but there are %d queued results",
                           ts.getCurrentRound(ts.getBasePartition()),
                           ts, state.queued_results.size());
        if (ts.getCurrentRound(ts.getBasePartition()) != 0) state.clear();
    }
    
    protected void startRound(LocalTransaction ts) {
        if (trace.val)
            LOG.trace(String.format("%s - Start round", ts));
        
        final TransactionState state = this.getState(ts);
        final int basePartition = ts.getBasePartition();
        final int currentRound = ts.getCurrentRound(basePartition);
        final int batch_size = ts.getCurrentBatchSize();
        
        // Create our output counters
        assert(state.output_order.isEmpty());
        for (int stmtIndex = 0; stmtIndex < batch_size; stmtIndex++) {
            if (trace.val)
                LOG.trace(String.format("%s - Examining %d dependencies [stmtIndex=%d, currentRound=%d]",
                          ts, state.dependencies.size(), stmtIndex, currentRound));
            for (DependencyInfo dinfo : state.dependencies.values()) {
                if (trace.val)
                    LOG.trace(String.format("%s - Checking %s", ts, dinfo));
                
                // Add this DependencyInfo our output list if it's being used in this round for this txn
                // and if it is not an internal dependency
                if (dinfo.inSameTxnRound(ts.getTransactionId(), currentRound) &&
                        dinfo.isInternal() == false && dinfo.getStatementIndex() == stmtIndex) {
                    state.output_order.add(dinfo.getDependencyId());
                }
            } // FOR
        } // FOR
        
        // XXX Disable assert - for SnapshotRestore test
        /*
        assert(batch_size == state.output_order.size()) :
            String.format("%s - Expected %d output dependencies but we queued up %d " +
                          "[outputOrder=%s / numDependencies=%d]",
                          ts, batch_size, state.output_order.size(),
                          state.output_order, state.dependencies.size());
        */
        
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
        if (debug.val)
            LOG.debug(String.format("%s - Created %s with dependency counter set to %d",
                      ts, state.dependency_latch.getClass().getSimpleName(), count));
    }
    
    protected void finishRound(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
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
                LOG.trace("Setting CountDownLatch to null for " + ts);
            state.dependency_latch = null;
        }
        state.clear();
    }
    
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    private TransactionState getState(LocalTransaction ts) {
        TransactionState state = this.txnStates.get(ts.getTransactionId());
        assert(state != null) :
            String.format("Unexpected null %s handle for %s at %s",
                          TransactionState.class.getSimpleName(), ts, this);
        return (state);
    }
    
    /**
     * 
     * @param state
     * @param currentRound
     * @param stmtCounter
     * @param paramsHash TODO
     * @param fragmentId TODO
     * @param dep_id
     * @return
     */
    private DependencyInfo getOrCreateDependencyInfo(LocalTransaction ts,
                                                     TransactionState state,
                                                     int currentRound,
                                                     int stmtCounter,
                                                     int stmtIndex,
                                                     int paramsHash,
                                                     int fragmentId,
                                                     Integer dep_id) {
        DependencyInfo dinfo = state.dependencies.get(dep_id);
        
        if (dinfo != null) {
            if (trace.val)
                LOG.trace(String.format("%s - Reusing DependencyInfo[hashCode=%d] for %s. " +
                          "Checking whether it needs to be reset " +
                          "[currentRound=%d, lastRound=%d, lastTxn=%s]",
                          ts, dinfo.hashCode(), TransactionUtil.debugStmtDep(stmtCounter, dep_id),
                          currentRound, dinfo.getRound(), dinfo.getTransactionId()));
            if (dinfo.inSameTxnRound(state.txn_id, currentRound) == false) {
                if (trace.val)
                    LOG.trace(String.format("%s - Clearing out DependencyInfo[%d].",
                              state.txn_id, dinfo.hashCode()));
                dinfo.finish();
            }
        } else {
            dinfo = new DependencyInfo(this.catalogContext);
            state.dependencies.put(dep_id, dinfo);
            if (trace.val)
                LOG.trace(String.format("%s - Created new DependencyInfo for %s " +
                		  "[stmtIndex=%d, fragmentId=%d, paramsHash=%d]",
                          ts, TransactionUtil.debugStmtDep(stmtCounter, dep_id),
                          stmtIndex, fragmentId, paramsHash));
        }
        if (dinfo.isInitialized() == false) {
            if (debug.val)
                LOG.debug(String.format("%s - Initializing DependencyInfo for %s " +
                          "[stmtIndex=%d, fragmentId=%d, paramsHash=%d]",
                          ts, TransactionUtil.debugStmtDep(stmtCounter, dep_id),
                          stmtIndex, fragmentId, paramsHash));
            dinfo.init(state.txn_id, currentRound, stmtCounter, stmtIndex, paramsHash, dep_id.intValue());
        }
        
        return (dinfo);
    }

    /**
     * Check to see whether there is already a prefetched query queued up for the
     * given WorkFragment information.
     * @param state
     * @param round
     * @param stmtCounter
     * @param partitionId TODO
     * @param paramsHash
     * @param fragmentId
     * @param dependencyId
     * @return
     */
    private DependencyInfo getPrefetchDependencyInfo(TransactionState state,
                                                     int round,
                                                     int stmtCounter,
                                                     int stmtIndex,
                                                     int partitionId,
                                                     int paramsHash,
                                                     int fragmentId,
                                                     int dependencyId) {
        Map<Integer, DependencyInfo> stmt_deps = state.prefetch_dependencies.get(stmtCounter);
        if (stmt_deps == null) {
            if (trace.val)
                LOG.trace(String.format("%s - Invalid prefetch query for %s." +
                          "No StmtCounter match.",
                          state, TransactionUtil.debugStmtDep(stmtCounter, dependencyId)));
            return (null);
        }
        DependencyInfo dinfo = stmt_deps.get(fragmentId);
        if (dinfo == null) {
            if (trace.val)
                LOG.trace(String.format("%s - Invalid prefetch query for %s. " +
                          "No FragmentID match. [%d]",
                          state, TransactionUtil.debugStmtDep(stmtCounter, dependencyId),
                          fragmentId));
            return (null);
        }
        
        if (dinfo.getParameterSetHash() != paramsHash) {
            if (trace.val)
                LOG.trace(String.format("%s - Invalid prefetch query for %s. " +
                          "Parameter hash mismatch [%d != %d]",
                          state, TransactionUtil.debugStmtDep(stmtCounter, dependencyId),
                          dinfo.getParameterSetHash(), paramsHash));
            return (null);
        }
        else if (dinfo.getExpectedPartitions().contains(partitionId) == false) {
            if (trace.val)
                LOG.trace(String.format("%s - Invalid prefetch query for %s. " +
                          "Partition mismatch [%d != %d]",
                          state, TransactionUtil.debugStmtDep(stmtCounter, dependencyId),
                          partitionId, dinfo.getExpectedPartitions()));
            return (null);
        }
        
        // IMPORTANT: We have to update this DependencyInfo's output id 
        // so that the blocked WorkFragment can retrieve it properly when it
        // runs. This is necessary because we don't know what the PlanFragment's
        // output id will be before it runs...
        if (debug.val && dinfo.isPrefetch() == false) {
            LOG.debug(String.format("%s - Converting prefetch %s into regular result\n%s",
                      state, dinfo.getClass().getSimpleName(), dinfo));
        }
        dinfo.prefetchOverride(round, dependencyId, stmtIndex);
        state.dependencies.put(dependencyId, dinfo);
        
        return (dinfo);
    }
    
    /**
     * Update internal state information after a new result was added to a DependencyInfo.
     * This may cause the next round of blocked WorkFragments to get released.
     * @param ts
     * @param state
     * @param dinfo
     */
    private void updateAfterNewResult(final LocalTransaction ts,
                                      final TransactionState state,
                                      final DependencyInfo dinfo) {
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
            LOG.debug(String.format("%s - No WorkFragments to unblock after storing result for DependencyId %d " +
                      "[blockedTasks=%d, hasTasksReady=%s]",
                      ts, dinfo.getDependencyId(), state.blocked_tasks.size(), dinfo.hasTasksReady()));
        }
    
        if (state.dependency_latch != null) {    
            state.dependency_latch.countDown();
            if (debug.val)
                LOG.debug(String.format("%s - Decremented %s to %d for partition %d ",
                          ts, state.dependency_latch.getClass().getSimpleName(),
                          state.dependency_latch.getCount(), ts.getBasePartition()));
                
            // HACK: If the latch is now zero, then push an EMPTY set into the unblocked queue
            // This will cause the blocked PartitionExecutor thread to wake up and realize that he's done
            if (state.dependency_latch.getCount() == 0) {
                if (debug.val)
                    LOG.debug(String.format("%s - Pushing EMPTY_FRAGMENT_SET to PartitionExecutor " +
                    		  "at partition %d because all of the dependencies have arrived!",
                              ts, ts.getBasePartition()));
                state.unblocked_tasks.addLast(EMPTY_FRAGMENT_SET);
            }
        }

        state.still_has_tasks = (state.blocked_tasks.isEmpty() == false ||
                                 state.unblocked_tasks.isEmpty() == false);
    }
    
    // ----------------------------------------------------------------------------
    // DEPENDENCY TRACKING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Get the final results of the last round of execution for the given txn.
     * This should only be called to get the VoltTables that you want to send into
     * the Java stored procedure code (e.g., the return value for voltExecuteSql())
     * @return
     */
    public VoltTable[] getResults(LocalTransaction ts) {
        final TransactionState state = this.getState(ts);
        final VoltTable results[] = new VoltTable[state.output_order.size()];
        if (debug.val)
            LOG.debug(String.format("%s - Generating output results with %d tables",
                      ts, results.length));
        
        HStoreConf hstore_conf = this.executor.getHStoreConf();
        boolean nonblocking = (hstore_conf.site.specexec_nonblocking &&
                               ts.isSysProc() == false &&
                               ts.profiler != null);
        for (int stmtIndex = 0; stmtIndex < results.length; stmtIndex++) {
            Integer dependency_id = state.output_order.get(stmtIndex);
            assert(dependency_id != null) :
                "Null output dependency id for Statement index " + stmtIndex + " in txn #" + state.txn_id;
            assert(state.dependencies.containsKey(dependency_id)) :
                String.format("Missing info for %s in %s",
                              TransactionUtil.debugStmtDep(stmtIndex, dependency_id), ts); 
            
            VoltTable vt = state.dependencies.get(dependency_id).getResult();

            // Special Non-Blocking Wrapper
            if (nonblocking) {
                VoltTableNonBlocking vtnb = new VoltTableNonBlocking(hstore_conf.site.txn_profiling ? ts.profiler : null);
                if (vt != null) vtnb.setRealTable(vt);
                results[stmtIndex] = vtnb;
            } else {
                assert(vt != null) : 
                    String.format("Null output result for Statement index %d in %s", stmtIndex, this); 
                results[stmtIndex] = vt;
            }
        } // FOR
        return (results);
    }
    
    /**
     * Queues up a WorkFragment for this txn.
     * If the return value is true, then the WorkFragment can be executed
     * immediately (either locally or on at a remote partition).
     * If the return value is false, then the WorkFragment is blocked waiting for dependencies.
     * @param ts
     * @param fragment
     * @return true if the WorkFragment should be dispatched right now 
     */
    public boolean addWorkFragment(LocalTransaction ts, WorkFragment.Builder fragment, ParameterSet batchParams[]) {
        final TransactionState state = this.getState(ts);
        assert(ts.getCurrentRoundState(ts.getBasePartition()) == RoundState.INITIALIZED) :
            String.format("Invalid round state %s for %s at partition %d",
                          ts.getCurrentRoundState(ts.getBasePartition()),
                          ts, ts.getBasePartition());
        
        boolean blocked = false;
        final int partition = fragment.getPartitionId();
        final int num_fragments = fragment.getFragmentIdCount();
        final int currentRound = ts.getCurrentRound(ts.getBasePartition());
        
        if (debug.val)
            LOG.debug(String.format("%s - Adding %s for partition %d with %d fragments",
                      ts, WorkFragment.class.getSimpleName(), partition, num_fragments));
        
        // PAVLO: 2011-12-10
        // We moved updating the exec_touchedPartitions histogram into the
        // BatchPlanner so that we won't increase the counter for a partition
        // if we read from a replicated table at the local partition
        // this.state.exec_touchedPartitions.put(partition, num_fragments);
        
        // PAVLO 2011-12-20
        // I don't know why, but before this loop used to be synchronized
        // It definitely does not need to be because this is only invoked by the
        // transaction's base partition PartitionExecutor
        int output_dep_id, input_dep_id;
        int ignore_ctr = 0;
        for (int i = 0; i < num_fragments; i++) {
            int partitionId = fragment.getPartitionId();
            int fragmentId = fragment.getFragmentId(i);
            int stmtCounter = fragment.getStmtCounter(i);
            int stmtIndex = fragment.getStmtIndex(i);
            int paramsHash = batchParams[fragment.getParamIndex(i)].hashCode();
            
            // If this task produces output dependencies, then we need to make 
            // sure that the txn wait for it to arrive first
            if ((output_dep_id = fragment.getOutputDepId(i)) != HStoreConstants.NULL_DEPENDENCY_ID) {
                DependencyInfo dinfo = null;
                boolean prefetch = false;
                
                // Check to see whether there is a already a prefetch WorkFragment for
                // this same query invocation.
                if (state.prefetch_ctr > 0) {
                    dinfo = this.getPrefetchDependencyInfo(state, currentRound,
                                                           stmtCounter, stmtIndex, partitionId,
                                                           paramsHash, fragmentId, output_dep_id);
                    prefetch = (dinfo != null);
                    
                }
                if (dinfo == null) {
                    dinfo = this.getOrCreateDependencyInfo(ts, state, currentRound,
                                                           stmtCounter, stmtIndex, paramsHash,
                                                           fragmentId, output_dep_id);
                }
                
                // Store the stmtIndex of when this dependency will show up
                dinfo.addPartition(partition);
                state.dependency_ctr++;
//                this.addResultDependencyStatement(ts, state, partition, output_dep_id, stmtIndex);
                
                if (trace.val)
                    LOG.trace(String.format("%s - Added new %s %s for PlanFragment %d at partition %d " +
                              "[depCtr=%d, prefetch=%s]\n%s",
                              ts, dinfo.getClass().getSimpleName(),
                              TransactionUtil.debugStmtDep(stmtCounter, output_dep_id),
                              fragment.getFragmentId(i),
                              partition,
                              state.dependency_ctr, prefetch,
                              dinfo.debug()));
                
                // If this query was prefetched, we need to push its results through the 
                // the tracker so that it can update counters
                if (prefetch) {
                    // We also need a way to mark this entry in the WorkFragment as 
                    // unnecessary and make sure that we don't actually send it out
                    // if there is no new work to be done.
                    fragment.setStmtIgnore(i, true);
                    ignore_ctr++;
                    
                    ts.getTransactionLock().lock();
                    try {
                        // Switch the DependencyInfo out of prefetch mode
                        // This means that all incoming results (if any) will be 
                        // added to TransactionState just like any other regular query.
                        dinfo.resetPrefetch();
                        
                        // Now update the internal state just as if these new results 
                        // arrived for this query.
                        state.received_ctr += dinfo.getResultsCount();
                        this.updateAfterNewResult(ts, state, dinfo);
                    } finally {
                        ts.getTransactionLock().unlock();
                    } // SYNCH
                }

            } // IF
            
            // If this WorkFragment needs an input dependency, then we need to make sure it arrives at
            // the executor before it is allowed to start executing
            if (fragment.getNeedsInput()) {
                input_dep_id = fragment.getInputDepId(i);
                if (input_dep_id != HStoreConstants.NULL_DEPENDENCY_ID) {
                    DependencyInfo dinfo = null;
                    
                    // Check to see whether there is already a prefetch WorkFragment that will
                    // generate this result for us.
                    if (state.prefetch_ctr > 0) {
                        dinfo = this.getPrefetchDependencyInfo(state, currentRound,
                                                               stmtCounter, stmtIndex, partitionId,
                                                               paramsHash, fragmentId, input_dep_id);
                    }
                    if (dinfo == null) {
                        dinfo = this.getOrCreateDependencyInfo(ts, state, currentRound,
                                                               stmtCounter, stmtIndex, paramsHash,
                                                               fragmentId, input_dep_id);
                    }
                    dinfo.addBlockedWorkFragment(fragment);
                    dinfo.markInternal();
                    if (blocked == false) {
                        state.blocked_tasks.add(fragment);
                        blocked = true;   
                    }
                    if (trace.val)
                        LOG.trace(String.format("%s - Created internal input dependency %d for PlanFragment %d\n%s", 
                                  ts, input_dep_id, fragment.getFragmentId(i), dinfo.debug()));
                }
            }
            
            // *********************************** DEBUG ***********************************
            if (trace.val) {
                int output_ctr = 0;
                int dep_ctr = 0;
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                for (DependencyInfo dinfo : state.dependencies.values()) {
                    if (dinfo.getStatementCounter() == stmtCounter) dep_ctr++;
                    if (dinfo.isInternal() == false) {
                        m.put(String.format("Output[%02d]", output_ctr++), dinfo.debug());
                    }
                } // FOR
                LOG.trace(String.format("%s - Number of Output Dependencies for StmtCounter #%d: " +
                          "%d out of %d\n%s", 
                          ts, stmtCounter, output_ctr, dep_ctr, StringUtil.formatMaps(m)));
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
            LOG.debug(String.format("%s - Queued up %s %s for partition %d and marked as %s [fragIds=%s]",
                      ts, catalog_obj, WorkFragment.class.getSimpleName(), partition,
                      (blocked ? "blocked" : "not blocked"),
                      fragment.getFragmentIdList()));
        }
        // *********************************** DEBUG ***********************************
        
        if (ignore_ctr == num_fragments) {
            return (false);
        }
        
        return (blocked == false);
    }
    
    /**
     * Store an output dependency result for a transaction. This corresponds to the 
     * execution of a single WorkFragment somewhere in the cluster. If there are other
     * WorkFragments to become unblocked and be ready to execute.
     * @param ts
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
        
        final ReentrantLock txnLock = ts.getTransactionLock();
        final int base_partition = ts.getBasePartition();
        final int partition = key.getFirst().intValue();
        final int dependency_id = key.getSecond().intValue();
        final RoundState roundState = ts.getCurrentRoundState(base_partition); 
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        
        assert(roundState == RoundState.INITIALIZED || roundState == RoundState.STARTED) :
            String.format("Invalid round state %s for %s at partition %d",
                          roundState, ts, base_partition);
        
        if (debug.val)
            LOG.debug(String.format("%s - Attemping to add new result with %d rows for %s",
                      ts, result.getRowCount(), TransactionUtil.debugPartDep(partition, dependency_id)));
        
        // If the txn is still in the INITIALIZED state, then we just want to queue up the results
        // for now. They will get released when we switch to STARTED 
        // This is the only part that we need to synchonize on
        if (force == false) {
            if (singlePartitioned == false) txnLock.lock();
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
            } finally {
                if (singlePartitioned == false) txnLock.unlock();
            } // SYNCH
        }
            
        // Each partition+dependency_id should be unique within the Statement batch.
        // So as the results come back to us, we have to figure out which Statement it belongs to
        DependencyInfo dinfo = null;
        try {
            dinfo = state.getDependencyInfo(dependency_id);
        } catch (NullPointerException ex) {
            // HACK: IGNORE!
        }
        if (dinfo == null) {
            // HACK: IGNORE!
            return;
        }

        if (singlePartitioned == false) txnLock.lock();
        try {
            // 2013-05-12: DependencyInfo.addResult() must definitely be synchronized!!!
            //             There is a weird race condition where the inner PartitionSet is not
            //             updated properly. 
            dinfo.addResult(partition, result);
            state.received_ctr++;
            this.updateAfterNewResult(ts, state, dinfo);
        } finally {
            if (singlePartitioned == false) txnLock.unlock();
        } // SYNCH
        
        if (debug.val)
            LOG.debug(String.format("%s - Stored new result for %s",
                      ts, TransactionUtil.debugPartDep(partition, dependency_id)));
        
        if (trace.val) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Blocked Tasks", (state != null ? state.blocked_tasks.size() : null));
            m.put("DependencyInfo", dinfo.debug());
            m.put("hasTasksReady", dinfo.hasTasksReady());
            m.put("Dependency Latch", state.dependency_latch);
            LOG.trace(this + " - Status Information\n" + StringUtil.formatMaps(m));
            // if (trace.val) LOG.trace(ts.debug());
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
            int stmtCounter = fragment.getStmtCounter(i);
            int input_d_id = fragment.getInputDepId(i);
            if (input_d_id == HStoreConstants.NULL_DEPENDENCY_ID) continue;
            
            DependencyInfo dinfo = state.getDependencyInfo(input_d_id);
            assert(dinfo != null);
            assert(dinfo.getPartitionCount() == dinfo.getResultsCount()) :
                String.format("%s - Number of results retrieved for %s is %d " +
                              "but we were expecting %d\n%s\n%s\n%s",
                              ts, TransactionUtil.debugStmtDep(stmtCounter, input_d_id),
                              dinfo.getResultsCount(), dinfo.getPartitionCount(),
                              fragment.toString(),
                              StringUtil.SINGLE_LINE, ts.debug()); 
            results.put(input_d_id, dinfo.getResults());
            if (trace.val)
                LOG.trace(String.format("%s - %s -> %d VoltTables",
                          ts, TransactionUtil.debugStmtDep(stmtCounter, input_d_id),
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
                          input_d_id, ts);
        assert(dinfo.isInternal()) :
            String.format("The DependencyInfo for Dependency %s in %s is not marked as internal",
                          input_d_id, ts);
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
    
    
    public BlockingDeque<Collection<WorkFragment.Builder>> getUnblockedWorkFragmentsQueue(LocalTransaction ts) {
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
    // QUERY PREFETCHING
    // ----------------------------------------------------------------------------

    /**
     * Inform this tracker the txn is requesting the given WorkFragment to be
     * prefetched on a remote partition.
     * @param ts
     * @param fragment
     * @return
     */
    public void addPrefetchWorkFragment(LocalTransaction ts, WorkFragment.Builder fragment, ParameterSet batchParams[]) {
        assert(fragment.getPrefetch());
        
        final TransactionState state = this.getState(ts);
        final int num_fragments = fragment.getFragmentIdCount();
        final int partition = fragment.getPartitionId();
        
        for (int i = 0; i < num_fragments; i++) {
            final int fragmentId = fragment.getFragmentId(i);
            final int stmtCounter = fragment.getStmtCounter(i);
            final int stmtIndex = fragment.getStmtIndex(i);
            final int paramHash = batchParams[fragment.getParamIndex(i)].hashCode();
            
            // A prefetched query must *always* produce an output!
            int output_dep_id = fragment.getOutputDepId(i);
            assert(output_dep_id != HStoreConstants.NULL_DEPENDENCY_ID);
            
            // But should never have an input dependency!
            assert(fragment.getNeedsInput() == false);

            // Note that we need to do a lookup in the map based on the StmtCounter
            // and not its StmtIndex. This is because the StmtCounter is global for the entire
            // transaction whereas the StmtIndex is unique for a single SQLStmt batch.
            Map<Integer, DependencyInfo> stmt_deps = state.prefetch_dependencies.get(stmtCounter);
            if (stmt_deps == null) {
                stmt_deps = new HashMap<Integer, DependencyInfo>();
                state.prefetch_dependencies.put(stmtCounter, stmt_deps);
            }
            
            DependencyInfo dinfo = stmt_deps.get(fragmentId);
            if (dinfo == null) {
                dinfo = new DependencyInfo(this.catalogContext);
                dinfo.init(state.txn_id, -1, stmtCounter, stmtIndex, paramHash, output_dep_id);
                dinfo.markPrefetch();
            }
            dinfo.addPartition(partition);
            stmt_deps.put(fragmentId, dinfo);
            state.prefetch_ctr++;
            
            if (debug.val) {
                String msg = String.format("%s - Adding prefetch %s %s at partition %d for %s",
                                           ts, dinfo,
                                           TransactionUtil.debugStmtDep(stmtCounter, output_dep_id), partition,
                                           CatalogUtil.getPlanFragment(catalogContext.catalog, fragment.getFragmentId(i)).fullName());
                if (trace.val)
                    msg += "\n" + String.format("ProcedureParams = %s\n" +
                                                "ParameterSet[%d] = %s\n%s",
                                                ts.getProcedureParameters(),
                                                fragment.getParamIndex(i), batchParams[fragment.getParamIndex(i)],
                                                dinfo.debug());
                LOG.debug(msg);
            }
        } // FOR
        
        return;
    }
    
    /**
     * Store a new prefetch result for a transaction
     * @param txnId
     * @param stmtCounter
     * @param fragmentId
     * @param partitionId
     * @param params
     * @param result
     */
    public void addPrefetchResult(LocalTransaction ts,
                                  int stmtCounter,
                                  int fragmentId,
                                  int partitionId,
                                  int paramsHash,
                                  VoltTable result) {
        assert(ts.hasPrefetchQueries());
        if (debug.val)
            LOG.debug(String.format("%s - Adding prefetch result %s with %d rows from partition %d [paramsHash=%d]",
                      ts, TransactionUtil.debugStmtFrag(stmtCounter, fragmentId),
                      result.getRowCount(), partitionId, paramsHash));
        
        final TransactionState state = this.getState(ts);
        if (state == null) {
            LOG.error(String.format("Missing %s for %s. Unable to store prefetch result from partition %d",
                      TransactionState.class.getSimpleName(), ts, partitionId));
            return;
        }
        
        // Find the corresponding DependencyInfo
        Map<Integer, DependencyInfo> stmt_deps = state.prefetch_dependencies.get(stmtCounter);
        if (stmt_deps == null) {
            String msg = String.format("Unexpected prefetch result for %s from partition %d - " +
                                       "Invalid SQLStmt index '%d'",
                                       ts, partitionId, stmtCounter);
            throw new ServerFaultException(msg, ts.getTransactionId());
        }
        
        DependencyInfo dinfo = stmt_deps.get(fragmentId);
        if (dinfo == null) {
            String msg = String.format("Unexpected prefetch result for %s from partition %d - " +
                                       "Invalid PlanFragment id '%d'",
                                       ts, partitionId, fragmentId);
            throw new ServerFaultException(msg, ts.getTransactionId());
        }
        assert(dinfo.getParameterSetHash() == paramsHash) :
            String.format("%s - ParameterSet Mismatch in %s for %s [%d != %d]",
                          ts, dinfo, TransactionUtil.debugStmtFrag(stmtCounter, fragmentId),
                          dinfo.getParameterSetHash(), paramsHash);
        assert(dinfo.getExpectedPartitions().contains(partitionId));
        
        // Always add it to our DependencyInfo handle and then check to see whether we have 
        // all of the results that we need for it.
        // If we do, then we need to check to see whether the txn needs the results
        // right now.
        final ReentrantLock txnLock = ts.getTransactionLock();
        txnLock.lock();
        try {
            // Check to see whether we should adding this through
            // the normal channels or whether we are still in "prefetch" mode
            if (dinfo.isPrefetch() == false) {
                this.addResult(ts, partitionId, dinfo.getDependencyId(), result);
            }
            else {
                dinfo.addResult(partitionId, result);    
            }
        } finally {
            txnLock.unlock();
        }
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG STUFF
    // ----------------------------------------------------------------------------
    
    @Override
    public String toString() {
        return String.format("%s{Partition=%d / Hash=%d}",
                             this.getClass().getSimpleName(),
                             this.executor.getPartitionId(),
                             this.hashCode());
    }
    
    public class Debug implements DebugContext {
        public boolean hasTransactionState(LocalTransaction ts) {
            try {
                return (getState(ts) != null);
            } catch (AssertionError ex) {
                return (false);
            }
        }
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
        public Map<Integer, DependencyInfo> getStatementDependencies(LocalTransaction ts, int stmtIndex) {
            final TransactionState state = getState(ts);
            return (state.dependencies);
        }
        public int getPrefetchCounter(LocalTransaction ts) {
            final TransactionState state = getState(ts);
            return (state.prefetch_ctr);
        }
        /**
         * Returns the number of outstanding prefetch DependencyInfo with 
         * results that were not utilized by the txn's regular query invocations.
         * If there is no TransactionState for the given txn handle or the txn did
         * not execute with prefetch queries, then the return result will be null.
         * @param ts
         * @return
         */
        public Integer getUnusedPrefetchResultCount(LocalTransaction ts) {
            TransactionState state = null;
            try {
                state = getState(ts);
            } catch (AssertionError ex) {
                // IGNORE
            }
            if (state == null || state.prefetch_dependencies == null) {
                return (null);
            }
            int ctr = 0;
            if (state.prefetch_dependencies != null) {
                for (Map<Integer, DependencyInfo> m : state.prefetch_dependencies.values()) {
                    for (DependencyInfo dinfo : m.values()) {
                        if (dinfo.isPrefetch() && dinfo.hasAllResults()) ctr++;
                    } // FOR
                } // FOR
            }
            return (ctr);
        }
        
        public Map<String, Object> debugMap(LocalTransaction ts) {
            try {
                TransactionState state = getState(ts);
                return state.debugMap();
            } catch (AssertionError ex) {
                // IGNORE
            }
            return (null);
            
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
