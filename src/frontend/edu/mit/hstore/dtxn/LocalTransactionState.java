/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.mit.hstore.dtxn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.BatchPlanner.BatchPlan;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.dtxn.Dtxn;

/**
 * 
 * @author pavlo
 */
public class LocalTransactionState extends TransactionState {
    protected static final Logger LOG = Logger.getLogger(LocalTransactionState.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();

    private static final Set<FragmentTaskMessage> EMPTY_SET = new HashSet<FragmentTaskMessage>();
    
    // ----------------------------------------------------------------------------
    // INTERNAL PARTITION+DEPENDENCY KEY
    // ----------------------------------------------------------------------------

    private static final int KEY_MAX_VALUE = 65535; // 2^16 - 1
    
    /**
     * Return a single key that encodes the partition id and dependency id
     * @param partition_id
     * @param dependency_id
     * @return
     */
    protected int createPartitionDependencyKey(int partition_id, int dependency_id) {
        Integer key = new Integer(partition_id | dependency_id<<16);
        this.partition_dependency_keys.add(key);
        int idx = this.partition_dependency_keys.indexOf(key);
        return (idx);
    }
    
    /**
     * For the given encoded Partition+DependencyInfo key, populate the given array
     * with the partitionid first, then the dependencyid second
     * @param key
     * @param values
     */
    protected void getPartitionDependencyFromKey(int idx, int values[]) {
        assert(values.length == 2);
        int key = this.partition_dependency_keys.get(idx).intValue();
        values[0] = key>>0 & KEY_MAX_VALUE;     // PartitionId
        values[1] = key>>16 & KEY_MAX_VALUE;    // DependencyId
    }
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * 
     */
    private final Object lock = new Object();

    private final List<BatchPlanner.BatchPlan> batch_plans = new ArrayList<BatchPlanner.BatchPlan>();

    private final Set<DependencyInfo> all_dependencies = new HashSet<DependencyInfo>();
    
    /**
     * LocalTransactionState Factory
     */
    public static class Factory extends CountingPoolableObjectFactory<LocalTransactionState> {
        private final ExecutionSite executor;
        
        public Factory(ExecutionSite executor, boolean enable_tracking) {
            super(enable_tracking);
            this.executor = executor;
        }
        @Override
        public LocalTransactionState makeObjectImpl() throws Exception {
            return new LocalTransactionState(this.executor);
        }
    };
    
    // ----------------------------------------------------------------------------
    // TRANSACTION INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Callback to the coordinator for txns that are running on this partition
     */
    private RpcCallback<Dtxn.FragmentResponse> coordinator_callback;
    
    /**
     * List of encoded Partition/Dependency keys
     */
    private ListOrderedSet<Integer> partition_dependency_keys = new ListOrderedSet<Integer>();

    public InitiateTaskMessage init_wrapper = null;
    
    // ----------------------------------------------------------------------------
    // HSTORE SITE DATA MEMBERS
    // ----------------------------------------------------------------------------

    private Long orig_txn_id;
    
    private Procedure catalog_proc;
    
    /**
     * The partitions that we told the Dtxn.Coordinator that we were done with
     */
    private final Set<Integer> done_partitions = new HashSet<Integer>();

    /**
     * Whether this is a sysproc
     */
    public boolean sysproc;
    /**
     * Whether this txn isn't use the Dtxn.Coordinator
     */
    public boolean ignore_dtxn = false;

    // ----------------------------------------------------------------------------
    // EXECUTION FLAGS
    // ----------------------------------------------------------------------------
    
    /** What partitions has this txn touched */
    private final Histogram<Integer> exec_touchedPartitions = new Histogram<Integer>();
    
    /** Whether this txn is being executed specutatively */
    private boolean exec_speculative = false;
    
    /**
     * The original StoredProcedureInvocation request that was sent to the HStoreSite
     */
    public StoredProcedureInvocation invocation;
    /**
     * Initialization Barrier
     * This ensures that a transaction does not start until the Dtxn.Coordinator has returned the acknowledgement
     * that from our call from procedureInvocation()->execute()
     * This probably should be pushed further into the ExecutionSite so that we can actually invoke the procedure
     * but just not send any data requests.
     */
    public CountDownLatch init_latch;
    /**
     * Final RpcCallback to the client
     */
    public RpcCallback<byte[]> client_callback;
    
    public final ProtoRpcController rpc_request_init = new ProtoRpcController();
    public final ProtoRpcController rpc_request_work = new ProtoRpcController();
    public final ProtoRpcController rpc_request_finish = new ProtoRpcController();
    
    // ----------------------------------------------------------------------------
    // ROUND DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * Temporary space used in ExecutionSite.waitForResponses
     */
    public final List<FragmentTaskMessage> remote_fragment_list = new ArrayList<FragmentTaskMessage>();
    public final List<FragmentTaskMessage> local_fragment_list = new ArrayList<FragmentTaskMessage>();
    
    /**
     * Temporary space used when calling removeInternalDependencies()
     */
    public final HashMap<Integer, List<VoltTable>> remove_dependencies_map = new HashMap<Integer, List<VoltTable>>();
    
    /**
     * This latch will block until all the Dependency results have returned
     * Generated in startRound()
     */
    private CountDownLatch dependency_latch;
    
    /**
     * SQLStmt Index -> DependencyId -> DependencyInfo
     */
    private final Map<Integer, DependencyInfo> dependencies[];
    
    /**
     * Final result output dependencies. Each position in the list represents a single Statement
     */
    private final List<Integer> output_order = new ArrayList<Integer>();
    
    /**
     * As information come back to us, we need to keep track of what SQLStmt we are storing 
     * the data for. Note that we have to maintain two separate lists for results and responses
     * Partition-DependencyId Key Offset -> Next SQLStmt Index
     */
    private final Map<Integer, Queue<Integer>> results_dependency_stmt_ctr = new ConcurrentHashMap<Integer, Queue<Integer>>();
    private final Map<Integer, Queue<Integer>> responses_dependency_stmt_ctr = new ConcurrentHashMap<Integer, Queue<Integer>>();

    /**
     * Sometimes we will get responses/results back while we are still queuing up the rest of the tasks and
     * haven't started the next round. So we need a temporary space where we can put these guys until 
     * we start the round. Otherwise calculating the proper latch count is tricky
     */
    private final Set<Integer> queued_responses = new ListOrderedSet<Integer>();
    private final Map<Integer, VoltTable> queued_results = new ListOrderedMap<Integer, VoltTable>();
    
    /**
     * Blocked FragmentTaskMessages
     */
    private final Set<FragmentTaskMessage> blocked_tasks = new HashSet<FragmentTaskMessage>();
    
    /**
     * Unblocked FragmentTaskMessages
     * The VoltProcedure thread will block on this queue waiting for tasks to execute inside of ExecutionSite
     * This has to be a set so that we make sure that we only submit a single message that contains all of the tasks to the Dtxn.Coordinator
     */
    private final LinkedBlockingDeque<Collection<FragmentTaskMessage>> unblocked_tasks = new LinkedBlockingDeque<Collection<FragmentTaskMessage>>(); 
    
    /**
     * These are the DependencyIds that we don't bother returning to the ExecutionSite
     */
    private final Set<Integer> internal_dependencies = new HashSet<Integer>();

    /**
     * Number of SQLStmts in the current batch
     */
    private int batch_size = 0;
    /**
     * The total # of dependencies this Transaction is waiting for in the current round
     */
    private int dependency_ctr = 0;
    /**
     * The total # of dependencies received thus far in the current round
     */
    private int received_ctr = 0;
    /**
     * TransctionEstimator State Handle
     */
    private TransactionEstimator.State estimator_state;
    /**
     * 
     */
    private final ConcurrentLinkedQueue<DependencyInfo> reusable_dependencies = new ConcurrentLinkedQueue<DependencyInfo>(); 

    /**
     * 
     */
    public final TransactionProfile profiler;


    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public LocalTransactionState(ExecutionSite executor) {
        super(executor);
        
        this.dependencies = (Map<Integer, DependencyInfo>[])new Map<?, ?>[BatchPlanner.MAX_BATCH_SIZE];
        for (int i = 0; i < this.dependencies.length; i++) {
            this.dependencies[i] = new HashMap<Integer, DependencyInfo>();
        } // FOR
        
        if (this.executor.getHStoreConf().site.txn_profiling) {
            this.profiler = new TransactionProfile();
        } else {
            this.profiler = null;
        }
    }
    
    @SuppressWarnings("unchecked")
    public LocalTransactionState init(long txnId, long clientHandle, int source_partition,
                                      boolean predict_singlePartitioned, boolean predict_readOnly, boolean predict_abortable) {
        return ((LocalTransactionState)super.init(txnId, clientHandle, source_partition,
                                                  predict_singlePartitioned, predict_readOnly, predict_abortable, true));
    }
    
    /**
     * 
     * @param txnId
     * @param clientHandle
     * @param base_partition
     * @param predictSinglePartition
     * @param predictReadOnly
     * @param predict_canAbort
     * @param estimator_state
     * @param catalog_proc
     * @param invocation
     * @param client_callback
     * @return
     */
    public LocalTransactionState init(long txnId, long clientHandle, int base_partition,
                                      boolean predictSinglePartition, boolean predictReadOnly, boolean predict_canAbort, TransactionEstimator.State estimator_state,
                                      Procedure catalog_proc, StoredProcedureInvocation invocation, RpcCallback<byte[]> client_callback) {
        
        this.estimator_state = estimator_state;
        this.catalog_proc = catalog_proc;
        this.sysproc = catalog_proc.getSystemproc();
        this.invocation = invocation;
        this.client_callback = client_callback;
        this.init_latch = (predictSinglePartition == false ? new CountDownLatch(1) : null);
        
        return ((LocalTransactionState)super.init(txnId, clientHandle, base_partition,
                                                  predictSinglePartition, predictReadOnly, predict_canAbort,
                                                  true));
    }
    
    /**
     * Initialization that copies information from the mispredicted original TransactionState 
     * @param txnId
     * @param base_partition
     * @param orig
     * @return
     */
    public LocalTransactionState init(long txnId, int base_partition, LocalTransactionState orig, boolean predict_singlePartitioned, boolean predict_readOnly, boolean predict_abortable) {
        this.orig_txn_id = orig.getTransactionId();
        this.catalog_proc = orig.catalog_proc;
        this.sysproc = orig.sysproc;
        this.invocation = orig.invocation;
        this.client_callback = orig.client_callback;
        this.init_latch = (this.isPredictSinglePartition() == false ? new CountDownLatch(1) : null);
        // this.estimator_state = orig.estimator_state;
        
        // Append the profiling times
//        if (this.executor.getEnableProfiling()) {
//            this.total_time.appendTime(orig.total_time);
//            this.java_time.appendTime(orig.java_time);
//            this.coord_time.appendTime(orig.coord_time);
//            this.ee_time.appendTime(orig.ee_time);
//            this.est_time.appendTime(orig.est_time);
//        }
        
        return (this.init(txnId, orig.client_handle, base_partition, predict_singlePartitioned, predict_readOnly, predict_abortable));
    }
    
    @Override
    public boolean isInitialized() {
        return (this.catalog_proc != null);
    }
    
    @Override
    public void finish() {
        super.finish();

        try {
            // Return all of our BatchPlans (if we have any)
            if (this.batch_plans.isEmpty() == false) {
                for (BatchPlanner.BatchPlan plan : this.batch_plans) {
                    plan.getPlanner().getBatchPlanPool().returnObject(plan);
                } // FOR
                this.batch_plans.clear();
            }

            // Return all of our DependencyInfos
            for (DependencyInfo d : this.all_dependencies) {
                DependencyInfo.INFO_POOL.returnObject(d);
            } // FOR
        
            // Return our TransactionEstimator.State handle
            if (this.estimator_state != null) {
                TransactionEstimator.getStatePool().returnObject(this.estimator_state);
                this.estimator_state = null;
            }
            
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        // Important! Call clearRound() before we clear out our other junk, otherwise
        // are are going to have leftover DependencyInfos
        this.clearRound();
        
        this.rpc_request_init.reset();
        this.rpc_request_work.reset();
        this.rpc_request_finish.reset();
        
        this.init_wrapper = null;
        
        this.orig_txn_id = null;
        this.catalog_proc = null;
        this.sysproc = false;
        
        this.exec_speculative = false;
        this.ignore_dtxn = false;
        this.done_partitions.clear();
        this.exec_touchedPartitions.clear();
        this.coordinator_callback = null;
        this.dependency_latch = null;
        this.all_dependencies.clear();
        this.reusable_dependencies.clear();
        
        if (this.profiler != null) this.profiler.finish();
    }
    
    private void clearRound() {
        this.partition_dependency_keys.clear();
        this.output_order.clear();
        this.queued_responses.clear();
        this.queued_results.clear();
        this.blocked_tasks.clear();
        this.internal_dependencies.clear();
        this.remote_fragment_list.clear();
        this.local_fragment_list.clear();

        // Note that we only want to clear the queues and not the whole maps
        for (Queue<Integer> q : this.results_dependency_stmt_ctr.values()) {
            q.clear();
        } // FOR
        for (Queue<Integer> q : this.responses_dependency_stmt_ctr.values()) {
            q.clear();
        } // FOR
        
        for (int i = 0; i < this.batch_size; i++) {
            this.reusable_dependencies.addAll(this.dependencies[i].values());
            this.dependencies[i].clear();
        } // FOR
        this.batch_size = 0;
        this.dependency_ctr = 0;
        this.received_ctr = 0;
    }
    
    
    public void setTransactionId(long txn_id) { 
        this.txn_id = txn_id;
    }
    
    /**
     * 
     * @param error
     * @param wakeThread
     */
    public void setPendingError(RuntimeException error, boolean wakeThread) {
        boolean spin_latch = (this.pending_error == null);
        super.setPendingError(error);
        if (wakeThread == false) return;
        
        // Spin through this so that the waiting thread wakes up and sees that they got an error
        if (spin_latch) {
            while (this.dependency_latch.getCount() > 0) {
                this.dependency_latch.countDown();
            } // WHILE
        }        
    }
    
    @Override
    public synchronized void setPendingError(RuntimeException error) {
        this.setPendingError(error, true);
    }
    
    @Override
    public void initRound(long undoToken) {
        assert(this.queued_responses.isEmpty()) : String.format("Trying to initialize round for txn #%d but there are %d queued responses",
                                                                this.txn_id, this.queued_responses.size());
        assert(this.queued_results.isEmpty()) : String.format("Trying to initialize round for txn #%d but there are %d queued results",
                                                              this.txn_id, this.queued_results.size());
        
        synchronized (this.lock) {
            super.initRound(undoToken);
            // Reset these guys here so that we don't waste time in the last round
            if (this.last_undo_token != null) this.clearRound();
        } // SYNCHRONIZED
    }
    
    public void fastInitRound(long undoToken) {
        super.initRound(undoToken);
    }
    
    @Override
    public void startRound() {
        assert(this.output_order.isEmpty());
        assert(this.batch_size > 0);
        if (d) LOG.debug("Starting round for local txn #" + this.txn_id + " with " + this.batch_size + " queued Statements");
        
        synchronized (this.lock) {
            super.startRound();
            
            // Create our output counters
            for (int stmt_index = 0; stmt_index < this.batch_size; stmt_index++) {
                for (DependencyInfo dinfo : this.dependencies[stmt_index].values()) {
                    if (this.internal_dependencies.contains(dinfo.dependency_id) == false) this.output_order.add(dinfo.dependency_id);
                } // FOR
            } // FOR
            assert(this.batch_size == this.output_order.size()) :
                "Expected " + this.getStatementCount() + " output dependencies but we queued up " + this.output_order.size();
            
            // Release any queued responses/results
            if (t) LOG.trace("Releasing " + this.queued_responses.size() + " queued responses");
            int key[] = new int[2];
            for (Integer response : this.queued_responses) {
                this.getPartitionDependencyFromKey(response.intValue(), key);
                this.processResultResponse(key[0], key[1], response.intValue(), null);
            } // FOR
            if (t) LOG.trace("Releasing " + this.queued_results.size() + " queued results");
            
            for (Entry<Integer, VoltTable> e : this.queued_results.entrySet()) {
                this.getPartitionDependencyFromKey(e.getKey().intValue(), key);
                this.processResultResponse(key[0], key[1], e.getKey().intValue(), e.getValue());
            } // FOR
            this.queued_responses.clear();
            this.queued_results.clear();
            
            // Now create the latch
            int count = this.dependency_ctr - this.received_ctr;
            assert(count >= 0);
            assert(this.dependency_latch == null) : "This should never happen!\n" + this.toString();
            this.dependency_latch = new CountDownLatch(count);
        }
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound() {
        assert(this.dependency_ctr == this.received_ctr) : "Trying to finish round for txn #" + this.txn_id + " before it was started"; 
        assert(this.queued_responses.isEmpty()) : "Trying to finish round for txn #" + this.txn_id + " but there are " + this.queued_responses.size() + " queued responses";
        assert(this.queued_results.isEmpty()) : "Trying to finish round for txn #" + this.txn_id + " but there are " + this.queued_results.size() + " queued results";
        
        if (d) LOG.debug("Finishing " + (this.exec_local ? "" : "non-") + "local round for txn #" + this.txn_id);
        synchronized (this.lock) {
            super.finishRound();
            
            // Reset our initialization flag so that we can be ready to run more stuff the next round
            if (this.dependency_latch != null) {
                assert(this.dependency_latch.getCount() == 0);
                if (t) LOG.debug("Setting CountDownLatch to null for txn #" + this.txn_id);
                this.dependency_latch = null;
            }
        } // SYNCHRONIZED
    }
    
    /**
     * Quickly finish this round. Assumes that everything executed locally
     */
    public void fastFinishRound() {
        this.round_state = RoundState.STARTED;
        super.finishRound();
    }
    
    public void setBatchSize(int batchSize) {
        this.batch_size = batchSize;
    }
    
    public StoredProcedureInvocation getInvocation() {
        return invocation;
    }
    public RpcCallback<byte[]> getClientCallback() {
        return client_callback;
    }
    public CountDownLatch getInitializationLatch() {
        return (this.init_latch);
    }
    
    /**
     * Return the original txn id that this txn was restarted for (after a mispredict)
     * @return
     */
    public Long getOriginalTransactionId() {
        return (this.orig_txn_id);
    }
    
    /**
     * 
     * @return
     */
    public Set<Integer> getDonePartitions() {
        return done_partitions;
    }
    public Histogram<Integer> getTouchedPartitions() {
        return (this.exec_touchedPartitions);
    }
    
    public String getProcedureName() {
        return (this.catalog_proc != null ? this.catalog_proc.getName() : null);
    }
    
    /**
     * Return the underlying procedure catalog object
     * The VoltProcedure must have already been set
     * @return
     */
    public Procedure getProcedure() {
        return (this.catalog_proc);
//        if (this.volt_procedure != null) {
//            return (this.volt_procedure.getProcedure());
//        }
//        return (null);
    }
//    public VoltProcedure getVoltProcedure() {
//        return this.volt_procedure;
//    }
//    public void setVoltProcedure(VoltProcedure voltProcedure) {
//        this.volt_procedure = voltProcedure;
//    }
    
    public int getDependencyCount() { 
        return (this.dependency_ctr);
    }
    public int getBlockedFragmentTaskMessageCount() {
        return (this.blocked_tasks.size());
    }
    protected Set<FragmentTaskMessage> getBlockedFragmentTaskMessages() {
        return (this.blocked_tasks);
    }
    public LinkedBlockingDeque<Collection<FragmentTaskMessage>> getUnblockedFragmentTaskMessageQueue() {
        return (this.unblocked_tasks);
    }
    
    public TransactionEstimator.State getEstimatorState() {
        return (this.estimator_state);
    }
    
    public void setEstimatorState(TransactionEstimator.State state) {
        this.estimator_state = state;
    }
    
    /**
     * 
     * @return
     */
    public CountDownLatch getDependencyLatch() {
        return this.dependency_latch;
    }
    
    /**
     * Return the number of statements that have been queued up in the last batch
     * @return
     */
    protected int getStatementCount() {
        return (this.batch_size);
    }
    protected Map<Integer, DependencyInfo> getStatementDependencies(int stmt_index) {
        return (this.dependencies[stmt_index]);
    }
    /**
     * 
     * @param stmt_index Statement Index
     * @param d_id Output Dependency Id
     * @return
     */
    protected DependencyInfo getDependencyInfo(int stmt_index, int d_id) {
        return (this.dependencies[stmt_index].get(d_id));
    }
    
    
    protected List<Integer> getOutputOrder() {
        return (this.output_order);
    }
    
    public Set<Integer> getInternalDependencyIds() {
        return (this.internal_dependencies);
    }
    

    public void setSpeculative(boolean speculative) {
        this.exec_speculative = speculative;
    }
    
    /**
     * Returns true if this transaction is being executed speculatively
     * @return
     */
    public boolean isSpeculative() {
        return (this.exec_speculative);
    }
    
    
    /**
     * Returns true if this Transaction has executed only on a single-partition
     * @return
     */
    public boolean isExecSinglePartition() {
        return (this.exec_touchedPartitions.getValueCount() <= 1);
    }
    
    /**
     * Returns true if the given FragmentTaskMessage is currently set as blocked for this txn
     * @param ftask
     * @return
     */
    public boolean isBlocked(FragmentTaskMessage ftask) {
        return (this.blocked_tasks.contains(ftask));
    }
    
    /**
     * Retrieves the coordinator callback
     * @return the coordinator_callback
     */
    public RpcCallback<Dtxn.FragmentResponse> getCoordinatorCallback() {
        return (this.coordinator_callback);
    }
    
    /**
     * @param callback
     */
    public void setCoordinatorCallback(RpcCallback<Dtxn.FragmentResponse> callback) {
        // Important! We never want to overwrite this after we set it!!
        assert(this.coordinator_callback == null) : "Trying to set the Coordinator callback twice for txn #" + this.txn_id;
        this.coordinator_callback = callback;
    }
    
    
    /**
     * 
     * @param d_id
     * @return
     */
    private DependencyInfo getOrCreateDependencyInfo(int stmt_index, Integer d_id) {
        Map<Integer, DependencyInfo> stmt_dinfos = this.dependencies[stmt_index];
        if (stmt_dinfos == null) {
            stmt_dinfos = new ConcurrentHashMap<Integer, DependencyInfo>();
            this.dependencies[stmt_index] = stmt_dinfos;
        }
        DependencyInfo dinfo = stmt_dinfos.get(d_id);
        if (dinfo == null) {
            // First try to get one that we have used before in a previous round for this txn
            dinfo = this.reusable_dependencies.poll();
            if (dinfo != null) {
                dinfo.finish();
            // If there is nothing local, then we have to go get an object from the global pool
            } else {
                try {
                    dinfo = (DependencyInfo)DependencyInfo.INFO_POOL.borrowObject();
                    this.all_dependencies.add(dinfo);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            
            // Always initialize the DependencyInfo regardless of how we got it 
            dinfo.init(this, stmt_index, d_id.intValue());
            stmt_dinfos.put(d_id, dinfo);
        }
        return (dinfo);
    }
    
    /**
     * Get the final results of the last round of execution for this Transaction
     * @return
     */
    @Override
    public VoltTable[] getResults() {
        final VoltTable results[] = new VoltTable[this.output_order.size()];
        if (d) LOG.debug("Generating output results with " + results.length + " tables for txn #" + this.txn_id);
        for (int stmt_index = 0; stmt_index < results.length; stmt_index++) {
            Integer dependency_id = this.output_order.get(stmt_index);
            assert(dependency_id != null) :
                "Null output dependency id for Statement index " + stmt_index + " in txn #" + this.txn_id;
            assert(this.dependencies[stmt_index] != null) :
                "Missing dependency set for stmt_index #" + stmt_index + " in txn #" + this.txn_id;
            assert(this.dependencies[stmt_index].containsKey(dependency_id)) :
                "Missing info for DependencyId " + dependency_id + " for Statement index " + stmt_index + " in txn #" + this.txn_id;
            
            results[stmt_index] = this.dependencies[stmt_index].get(dependency_id).getResult();
            assert(results[stmt_index] != null) :
                "Null output result for Statement index " + stmt_index + " in txn #" + this.txn_id;
        } // FOR
        return (results);
    }
    
    /**
     * Queues up a FragmentTaskMessage for this txn
     * If the return value is true, then the FragmentTaskMessage is blocked waiting for dependencies
     * If the return value is false, then the FragmentTaskMessage can be executed immediately (either locally or on at a remote partition)
     * @param ftask
     */
    public boolean addFragmentTaskMessage(FragmentTaskMessage ftask) {
        assert(this.round_state == RoundState.INITIALIZED) : "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        // The partition that this task is being sent to for execution
        boolean blocked = false;
        int partition = ftask.getDestinationPartitionId();
        int num_fragments = ftask.getFragmentCount();
        this.exec_touchedPartitions.put(partition, num_fragments);
        
        // If this task produces output dependencies, then we need to make 
        // sure that the txn wait for it to arrive first
        if (ftask.hasOutputDependencies()) {
            int output_dependencies[] = ftask.getOutputDependencyIds();
            int stmt_indexes[] = ftask.getFragmentStmtIndexes();
            
            synchronized (this.lock) {
                for (int i = 0; i < num_fragments; i++) {
                    Integer dependency_id = output_dependencies[i];
                    Integer stmt_index = stmt_indexes[i];
                    
                    if (t) LOG.trace("Adding new Dependency [stmt_index=" + stmt_index + ", id=" + dependency_id + ", partition=" + partition + "] for txn #" + this.txn_id);
                    this.getOrCreateDependencyInfo(stmt_index.intValue(), dependency_id).addPartition(partition);
                    this.dependency_ctr++;
    
                    // Store the stmt_index of when this dependency will show up
                    Integer key_idx = createPartitionDependencyKey(partition, dependency_id.intValue());
    
                    Queue<Integer> rest_stmt_ctr = this.results_dependency_stmt_ctr.get(key_idx);
                    Queue<Integer> resp_stmt_ctr = this.responses_dependency_stmt_ctr.get(key_idx);
                    if (rest_stmt_ctr == null) {
                        assert(resp_stmt_ctr == null);
                        rest_stmt_ctr = new LinkedList<Integer>();
                        resp_stmt_ctr = new LinkedList<Integer>();
                        this.results_dependency_stmt_ctr.put(key_idx, rest_stmt_ctr);
                        this.responses_dependency_stmt_ctr.put(key_idx, resp_stmt_ctr);
                    }
                    rest_stmt_ctr.add(stmt_index);
                    resp_stmt_ctr.add(stmt_index);
                    if (t) LOG.trace(String.format("Set Dependency Statement Counters for <%d %d>: %s", partition, dependency_id, rest_stmt_ctr));
                    assert(resp_stmt_ctr.size() == rest_stmt_ctr.size());
                } // FOR
            } // SYNCH
        }
        
        // If this task needs an input dependency, then we need to make sure it arrives at
        // the executor before it is allowed to start executing
        if (ftask.hasInputDependencies()) {
            if (t) LOG.trace("Blocking fragments " + Arrays.toString(ftask.getFragmentIds()) + " waiting for " + ftask.getInputDependencyCount() + " dependencies in txn #" + this.txn_id + ": " + Arrays.toString(ftask.getAllUnorderedInputDepIds()));
            synchronized (this.lock) {
                for (int i = 0; i < num_fragments; i++) {
                    int dependency_id = ftask.getOnlyInputDepId(i);
                    int stmt_index = ftask.getFragmentStmtIndexes()[i];
                    this.getOrCreateDependencyInfo(stmt_index, dependency_id).addBlockedFragmentTaskMessage(ftask);
                    this.internal_dependencies.add(dependency_id);
                } // FOR
            } // SYNCH
            this.blocked_tasks.add(ftask);
            blocked = true;
        }
        if (d) {
            LOG.debug("Queued up FragmentTaskMessage in txn #" + this.txn_id + " for partition " + partition + " and marked as" + (blocked ? "" : " not") + " blocked");
            if (t) LOG.trace("FragmentTaskMessage Contents for txn #" + this.txn_id + ":\n" + ftask);
        }
        return (blocked);
    }

    /**
     * 
     * @param partition
     * @param dependency_id
     */
    public void addResponse(int partition, int dependency_id) {
        final int key = this.createPartitionDependencyKey(partition, dependency_id);
        this.processResultResponse(partition, dependency_id, key, null);
    }
    
    
    /**
     * 
     * @param partition
     * @param dependency_id
     * @param result
     */
    public void addResult(int partition, int dependency_id, VoltTable result) {
        assert(result != null) :
            "The result for DependencyId " + dependency_id + " is null in txn #" + this.txn_id;
        int key = this.createPartitionDependencyKey(partition, dependency_id);
        this.processResultResponse(partition, dependency_id, key, result);
    }

    /**
     * Add a result with a response (this is slow and should only be used for testing);
     * @param partition
     * @param dependency_id
     * @param result
     */
    public void addResultWithResponse(int partition, int dependency_id, VoltTable result) {
        int key = this.createPartitionDependencyKey(partition, dependency_id);
        this.processResultResponse(partition, dependency_id, key, null);
        this.processResultResponse(partition, dependency_id, key, result);
    }
    
    
    /**
     * 
     * @param partition
     * @param dependency_id
     * @param result
     */
    private void processResultResponse(final int partition, final int dependency_id, final int key, VoltTable result) {
        final String type = (d ? (result != null ? "RESULT" : "RESPONSE") : null);
        assert(this.round_state == RoundState.INITIALIZED || this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        assert(this.exec_local) :
            "Trying to store " + type + " for txn #" + this.txn_id + " but it is not executing locally!";

        DependencyInfo dinfo = null;
        Map<Integer, Queue<Integer>> stmt_ctr = (result != null ? this.results_dependency_stmt_ctr : this.responses_dependency_stmt_ctr);
        
        // If the txn is still in the INITIALIZED state, then we just want to queue up the results
        // for now. They will get released when we switch to STARTED 
        synchronized (this.lock) {
            if (this.round_state == RoundState.INITIALIZED) {
                if (result != null) {
                    assert(this.queued_results.containsKey(key) == false) : "Duplicate " + type + " " + key + " for txn #" + this.txn_id;
                    this.queued_results.put(key, result);
                } else {
                    assert(this.queued_responses.contains(key) == false) : "Duplicate " + type + " " + key + " for txn #" + this.txn_id;
                    this.queued_responses.add(key);
                }
                if (t) LOG.trace("Queued " + type + " " + key + " for txn #" + this.txn_id + " until the round is started");
                return;
            }

            // Each partition+dependency_id should be unique for a Statement batch.
            // So as the results come back to us, we have to figure out which Statement it belongs to
            if (t) LOG.trace("Storing new " + type + " for key " + key + " in txn #" + this.txn_id);
            Queue<Integer> queue = stmt_ctr.get(key);
            if (t) LOG.trace(type + " stmt_ctr(key=" + key + "): " + queue);
            assert(queue != null) :
                "Unexpected partition/dependency " + type + " pair " + key + " in txn #" + this.txn_id;
            assert(queue.isEmpty() == false) :
                "No more statements for partition/dependency " + type + " pair " + key + " in txn #" + this.txn_id + "\n" + this;
            
            int stmt_index = queue.remove().intValue();
            dinfo = this.getDependencyInfo(stmt_index, dependency_id);
            assert(dinfo != null) :
                "Unexpected DependencyId " + dependency_id + " from partition " + partition + " for txn #" + this.txn_id + " [stmt_index=" + stmt_index + "]\n" + result;

            final boolean complete = (result != null ? dinfo.addResult(partition, result) : dinfo.addResponse(partition));
            if (complete) {
                if (t) LOG.trace("Received all RESULTS + RESPONSES for [stmt#=" + stmt_index + ", dep#=" + dependency_id + "] for txn #" + this.txn_id);
                this.received_ctr++;
                if (this.dependency_latch != null) {
                    this.dependency_latch.countDown();
                    
                    // HACK: If the latch is now zero, then push an EMPTY set into the unblocked queue
                    long count = this.dependency_latch.getCount();
                    if (count == 0) this.unblocked_tasks.offer(EMPTY_SET);
                    if (t) LOG.trace("Setting CountDownLatch to " + count + " for txn #" + this.txn_id);
                }    
            }
        } // SYNC
        // Check whether we need to start running stuff now
        if (!this.blocked_tasks.isEmpty() && dinfo.hasTasksReady()) {
            this.executeBlockedTasks(dinfo);
        }
    }

    /**
     * Note the arrival of a new result that this txn needs
     * @param dependency_id
     * @param result
     */
    private void executeBlockedTasks(DependencyInfo dinfo) {
        Set<FragmentTaskMessage> to_unblock = dinfo.getAndReleaseBlockedFragmentTaskMessages();
        // Always double check whether somebody beat us to the punch
        if (to_unblock == null) {
            if (t) LOG.trace(String.format("No new FragmentTaskMessages available to unblock for txn #%d. Ignoring...", this.txn_id));
            return;
        }
        if (d) LOG.debug(String.format("Got %d FragmentTaskMessages to unblock for txn #%d that were waiting for DependencyId %d",
                                       to_unblock.size(), this.txn_id, dinfo.getDependencyId()));
        this.blocked_tasks.removeAll(to_unblock);
        this.unblocked_tasks.add(to_unblock);
    }

    /**
     * Retrieve the dependency results that are used for internal plan execution
     * These are not the results that should be sent to the client
     * @return
     */
    public HashMap<Integer, List<VoltTable>> removeInternalDependencies(FragmentTaskMessage ftask) {
        return (this.removeInternalDependencies(ftask, new HashMap<Integer, List<VoltTable>>()));
    }
    
    /**
     * Fast version!
     * Retrieve the dependency results that are used for internal plan execution
     * These are not the results that should be sent to the client
     * @return
     */
    public synchronized HashMap<Integer, List<VoltTable>> removeInternalDependencies(final FragmentTaskMessage ftask, final HashMap<Integer, List<VoltTable>> results) {
        if (d) LOG.debug(String.format("Retrieving %d internal dependencies for txn #%d", this.internal_dependencies.size(), this.txn_id));
        
        for (int i = 0, cnt = ftask.getFragmentCount(); i < cnt; i++) {
            int input_d_id = ftask.getOnlyInputDepId(i);
            if (input_d_id == ExecutionSite.NULL_DEPENDENCY_ID) continue;
            int stmt_index = ftask.getFragmentStmtIndexes()[i];

            DependencyInfo dinfo = this.getDependencyInfo(stmt_index, input_d_id);
            assert(dinfo != null);
            int num_tables = dinfo.results.size();
            assert(dinfo.getPartitions().size() == num_tables) :
                "Number of results retrieved for <Stmt #" + stmt_index + ", DependencyId #" + input_d_id + "> is " + num_tables +
                " but we were expecting " + dinfo.getPartitions().size() + " in txn #" + this.txn_id +
                " [" + this.getProcedureName() + "]\n" + 
                this.toString() + "\n" +
                ftask.toString();
            results.put(input_d_id, dinfo.getResults(this.base_partition, true));
            if (d) LOG.debug(String.format("<Stmt#%d, DependencyId#%d> -> %d VoltTables", stmt_index, input_d_id, results.get(input_d_id).size()));
        } // FOR
        return (results);
    }
    
    @Override
    public String toString() {
        if (this.isInitialized()) {
            return (this.getProcedureName() + " #" + this.txn_id);
        } else {
            return ("<Uninitialized>");
        }
    }
    
    @Override
    public String debug() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        ListOrderedMap<String, Object> m;
        
        // Header
        maps.add(super.getDebugMap());
        
        // Basic Info
        m = new ListOrderedMap<String, Object>();
        m.put("Procedure", this.getProcedureName());
        m.put("SysProc", this.sysproc);
        m.put("Dependency Ctr", this.dependency_ctr);
        m.put("Internal Ctr", this.internal_dependencies.size());
        m.put("Received Ctr", this.received_ctr);
        m.put("CountdownLatch", this.dependency_latch);
        m.put("# of Blocked Tasks", this.blocked_tasks.size());
        m.put("# of Statements", this.batch_size);
        m.put("Expected Results", this.results_dependency_stmt_ctr.keySet());
        m.put("Expected Responses", this.responses_dependency_stmt_ctr.keySet());
        maps.add(m);
        
        // Predictions
        m = new ListOrderedMap<String, Object>();
        m.put("Predict Single-Partitioned", this.isPredictSinglePartition());
        m.put("Predict Read Only", this.isPredictReadOnly());
        m.put("Predict Abortable", this.isPredictAbortable());
        m.put("Estimator State", this.estimator_state);
        maps.add(m);
        
        // Actual Execution
        m = new ListOrderedMap<String, Object>();
        m.put("Exec Single-Partitioned", this.isExecSinglePartition());
        m.put("Exec Read Only", this.exec_readOnly);
        m.put("Exec Locally", this.exec_local);
        m.put("Done Partitions", this.done_partitions);
        m.put("Speculative Execution", this.exec_speculative);
        m.put("Touched Partitions", this.exec_touchedPartitions);
        maps.add(m);

        // Additional Info
        m = new ListOrderedMap<String, Object>();
        m.put("Original Txn Id", this.orig_txn_id);
        m.put("Init Latch", this.init_latch);
        m.put("Client Callback", this.client_callback);
        
        m.put("Dtxn.Coordinator Callback", this.coordinator_callback);
        m.put("Ignore Dtxn.Coordinator", this.ignore_dtxn);
        maps.add(m);

        // Profile Times
        if (this.profiler != null) maps.add(this.profiler.debugMap());
        
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()])));
        sb.append(StringUtil.SINGLE_LINE);

        String stmt_debug[] = new String[this.batch_size];
        for (int stmt_index = 0; stmt_index < stmt_debug.length; stmt_index++) {
            Map<Integer, DependencyInfo> s_dependencies = new HashMap<Integer, DependencyInfo>(this.dependencies[stmt_index]); 
            Set<Integer> dependency_ids = new HashSet<Integer>(s_dependencies.keySet());
            String inner = "";
            inner += "  Statement #" + stmt_index + "\n";
            inner += "  Output Dependency Id: " + (this.output_order.contains(stmt_index) ? this.output_order.get(stmt_index) : "<NOT STARTED>") + "\n";
            
            inner += "  Dependency Partitions:\n";
            for (Integer dependency_id : dependency_ids) {
                inner += "    [" + dependency_id + "] => " + s_dependencies.get(dependency_id).partitions + "\n";
            } // FOR
            
            inner += "  Dependency Results:\n";
            for (Integer dependency_id : dependency_ids) {
                inner += "    [" + dependency_id + "] => [";
                String add = "";
                for (VoltTable vt : s_dependencies.get(dependency_id).getResults()) {
                    inner += add + (vt == null ? vt : "{" + vt.getRowCount() + " tuples}");
                    add = ",";
                }
                inner += "]\n";
            } // FOR
            
            inner += "  Dependency Responses:\n";
            for (Integer dependency_id : dependency_ids) {
                inner += "    [" + dependency_id + "] => " + s_dependencies.get(dependency_id).getResponses() + "\n";
            } // FOR
    
            inner += "  Blocked FragmentTaskMessages:\n";
            boolean none = true;
            for (Integer dependency_id : dependency_ids) {
                DependencyInfo d = s_dependencies.get(dependency_id);
                for (FragmentTaskMessage task : d.getBlockedFragmentTaskMessages()) {
                    if (task == null) continue;
                    inner += "    [" + dependency_id + "] => [";
                    String add = "";
                    for (long id : task.getFragmentIds()) {
                        inner += add + id;
                        add = ", ";
                    } // FOR
                    inner += "]";
                    if (d.hasTasksReady()) inner += " READY!"; 
                    inner += "\n";
                    none = false;
                }
            } // FOR
            if (none) inner += "    <none>\n";
            stmt_debug[stmt_index] = inner;
        } // (dependencies)
        sb.append(StringUtil.columns(stmt_debug));
        
        return (sb.toString());
    }

    @Override
    public void addFinishedBatchPlan(BatchPlan plan) {
        this.batch_plans.add(plan);
    }
}
