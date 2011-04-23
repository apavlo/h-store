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
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.BatchPlanner.BatchPlan;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FragmentTaskMessage;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ProfileMeasurement.Type;
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
     * 
     */
    private VoltProcedure volt_procedure;
    
    /**
     * List of encoded Partition/Dependency keys
     */
    private ListOrderedSet<Integer> partition_dependency_keys = new ListOrderedSet<Integer>();

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
     * What partitions has this txn touched
     */
    private final Histogram<Integer> touched_partitions = new Histogram<Integer>();
    /**
     * Whether this is a sysproc
     */
    public boolean sysproc;
    /**
     * Whether this txn isn't use the Dtxn.Coordinator
     */
    public boolean ignore_dtxn = false;

    /**
     * Whether this txn is suppose to be single-partitioned
     */
    protected boolean single_partitioned = false;
    
    /**
     * Whether this txn is being executed specutatively
     */
    protected boolean speculative = false;
    
    /**
     * Whether this txn can abort
     */
    protected boolean can_abort = true;
    
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
     */
    private final LinkedBlockingDeque<FragmentTaskMessage> unblocked_tasks = new LinkedBlockingDeque<FragmentTaskMessage>(); 
    
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

    
    // ----------------------------------------------------------------------------
    // PROFILE MEASUREMENTS
    // ----------------------------------------------------------------------------

    /**
     * Total time spent executing the transaction
     */
    public final ProfileMeasurement total_time = new ProfileMeasurement(Type.TOTAL);
    /**
     * Time spent in HStoreSite procedureInitialization
     */
    public final ProfileMeasurement init_time = new ProfileMeasurement(Type.INITIALIZATION);
    /**
     * Time spent blocked on the initialization latch
     */
    public final ProfileMeasurement blocked_time = new ProfileMeasurement(Type.BLOCKED);
    /**
     * Time spent getting the response back to the client
     */
    public final ProfileMeasurement finish_time = new ProfileMeasurement(Type.CLEANUP);
    /**
     * Time spent waiting in queue
     */
    public final ProfileMeasurement queue_time = new ProfileMeasurement(Type.QUEUE);
    /**
     * The amount of time spent executing the Java-portion of the stored procedure
     */
    public final ProfileMeasurement java_time = new ProfileMeasurement(Type.JAVA);
    /**
     * The amount of time spent coordinating the transaction
     */
    public final ProfileMeasurement coord_time = new ProfileMeasurement(Type.COORDINATOR);
    /**
     * The amount of time spent planning the transaction
     */
    public final ProfileMeasurement plan_time = new ProfileMeasurement(Type.PLANNER);
    /**
     * The amount of time spent executing in the plan fragments
     */
    public final ProfileMeasurement ee_time = new ProfileMeasurement(Type.EE);
    /**
     * The amount of time spent estimating what the transaction will do
     */
    public final ProfileMeasurement est_time = new ProfileMeasurement(Type.ESTIMATION);
    

    
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
    }
    
    @SuppressWarnings("unchecked")
    public LocalTransactionState init(long txnId, long clientHandle, int source_partition) {
        // We'll use these to reduce the number of calls to the LoggerBooleans
        // Probably doesn't make a difference but I'll take anything I can get now...
//        t = trace.get();
//        d = debug.get();
        return ((LocalTransactionState)super.init(txnId, clientHandle, source_partition, true));
    }
    
    public LocalTransactionState init(long txnId, long clientHandle, int source_partition,
                                      boolean single_partitioned, boolean can_abort, TransactionEstimator.State estimator_state,
                                      Procedure catalog_proc, StoredProcedureInvocation invocation, RpcCallback<byte[]> client_callback) {
        
        this.single_partitioned = single_partitioned;
        this.can_abort = can_abort;
        this.estimator_state = estimator_state;
        
        this.catalog_proc = catalog_proc;
        this.sysproc = catalog_proc.getSystemproc();
        this.invocation = invocation;
        this.client_callback = client_callback;
        this.init_latch = new CountDownLatch(1);
        
        return (this.init(txnId, clientHandle, source_partition));
    }
    
    public LocalTransactionState init(long txnId, int base_partition, LocalTransactionState orig) {
        this.orig_txn_id = orig.getTransactionId();
        this.catalog_proc = orig.catalog_proc;
        this.sysproc = orig.sysproc;
        this.invocation = orig.invocation;
        this.client_callback = orig.client_callback;
        this.init_latch = new CountDownLatch(1);
        // this.estimator_state = orig.estimator_state;
        
        // Append the profiling times
//        if (this.executor.getEnableProfiling()) {
//            this.total_time.appendTime(orig.total_time);
//            this.java_time.appendTime(orig.java_time);
//            this.coord_time.appendTime(orig.coord_time);
//            this.ee_time.appendTime(orig.ee_time);
//            this.est_time.appendTime(orig.est_time);
//        }
        
        return (this.init(txnId, orig.client_handle, base_partition));
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
        
        
        this.orig_txn_id = null;
        this.catalog_proc = null;
        this.sysproc = false;
        this.single_partitioned = false;
        this.speculative = false;
        this.can_abort = true;
        this.ignore_dtxn = false;
        this.done_partitions.clear();
        this.touched_partitions.clear();
        this.coordinator_callback = null;
        this.volt_procedure = null;
        this.dependency_latch = null;
        this.all_dependencies.clear();
        this.reusable_dependencies.clear();
        
        if (this.executor.getHStoreConf().enable_profiling) {
            this.total_time.reset();
            this.init_time.reset();
            this.blocked_time.reset();
            this.queue_time.reset();
            this.finish_time.reset();
            this.java_time.reset();
            this.coord_time.reset();
            this.ee_time.reset();
            this.est_time.reset();
        }
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
    
    @Override
    public synchronized void setPendingError(RuntimeException error) {
        boolean spin_latch = (this.pending_error == null);
        super.setPendingError(error);
        
        // Spin through this so that the waiting thread wakes up and sees that they got an error
        if (spin_latch) {
            while (this.dependency_latch.getCount() > 0) {
                this.dependency_latch.countDown();
            } // WHILE
        }
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
        return (this.touched_partitions);
    }
    
    public String getProcedureName() {
        return (this.catalog_proc.getName());
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
    public VoltProcedure getVoltProcedure() {
        return this.volt_procedure;
    }
    public void setVoltProcedure(VoltProcedure voltProcedure) {
        this.volt_procedure = voltProcedure;
    }
    
    public int getDependencyCount() { 
        return (this.dependency_ctr);
    }
    public int getBlockedFragmentTaskMessageCount() {
        return (this.blocked_tasks.size());
    }
    protected Set<FragmentTaskMessage> getBlockedFragmentTaskMessages() {
        return (this.blocked_tasks);
    }
    public LinkedBlockingDeque<FragmentTaskMessage> getUnblockedFragmentTaskMessageQueue() {
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
    
    public void setPredictSinglePartitioned(boolean singlePartitioned) {
        this.single_partitioned = singlePartitioned;
    }
    public void setPredictAbortable(boolean canAbort) {
        this.can_abort = canAbort;
    }
    public void setSpeculative(boolean speculative) {
        this.speculative = speculative;
    }
    
    /**
     * Returns true if this Transaction was originally predicted to be single-partitioned
     * @return
     */
    public boolean isPredictSinglePartition() {
        return (this.single_partitioned);
    }
    
    /**
     * Returns true if this transaction is being executed speculatively
     * @return
     */
    public boolean isSpeculative() {
        return (this.speculative);
    }
    
    /**
     * Returns true if this Transaction was originally predicted as being able to abort
     * @return
     */
    public boolean isPredictAbortable() {
        return can_abort;
    }
    
    /**
     * Returns true if this Transaction has executed only on a single-partition
     * @return
     */
    public boolean isExecSinglePartition() {
        return (this.touched_partitions.getValueCount() <= 1);
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
        this.touched_partitions.put(partition, num_fragments);
        
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
                if (t) LOG.trace("Received all RESULTS + RESPONSES for [stmt#" + stmt_index + ", dep#=" + dependency_id + "] for txn #" + this.txn_id);
                this.received_ctr++;
                if (this.dependency_latch != null) {
                    this.dependency_latch.countDown();
                    if (t) LOG.trace("Setting CountDownLatch to " + this.dependency_latch.getCount() + " for txn #" + this.txn_id);
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
    private synchronized void executeBlockedTasks(DependencyInfo d) {
        
        // Always double check whether somebody beat us to the punch
        if (this.blocked_tasks.isEmpty() || d.blocked_tasks_released) return;
        
//        List<FragmentTaskMessage> to_execute = new ArrayList<FragmentTaskMessage>();
        Set<FragmentTaskMessage> tasks = d.getAndReleaseBlockedFragmentTaskMessages();
        for (FragmentTaskMessage unblocked : tasks) {
            assert(unblocked != null);
            assert(this.blocked_tasks.contains(unblocked));
            if (t) LOG.trace("Unblocking FragmentTaskMessage that was waiting for DependencyId #" + d.getDependencyId() + " in txn #" + this.txn_id);
            this.unblocked_tasks.add(unblocked);
            this.blocked_tasks.remove(unblocked);
            
//            // If this task needs to execute locally, then we'll just queue up with the Executor
//            if (d.blocked_all_local) {
//                if (t) LOG.info("Sending unblocked task to local ExecutionSite for txn #" + this.txn_id);
//                this.unblocked_tasks.add(unblocked);
//                
//            // Otherwise we will push out to the coordinator to handle for us
//            // But we have to make sure that we attach the dependencies that they need 
//            // so that the other partition can get to them
//            } else {
//                if (t) LOG.trace("Sending unblocked task to partition #" + unblocked.getDestinationPartitionId() + " with attached results for txn #" + this.txn_id);
//                to_execute.add(unblocked);
//            }
        } // FOR
//        if (!to_execute.isEmpty()) {
//            if (t) LOG.info(String.format("Txn #%d is requesting %d unblocked tasks to be executed", this.txn_id, to_execute.size()));
//            try {
//                this.executor.requestWork(this, to_execute);
//            } catch (MispredictionException ex) {
//                this.setPendingError(ex);
//                if (this.dependency_latch != null) {
//                    while (this.dependency_latch.getCount() > 0) this.dependency_latch.countDown();
//                }
//            }
//        }
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
                " [" + this.executor.getRunningVoltProcedure(this.txn_id).getProcedureName() + "]\n" + 
                this.toString() + "\n" +
                ftask.toString();
            results.put(input_d_id, dinfo.getResults(this.base_partition, true));
            if (d) LOG.debug(String.format("<Stmt#%d, DependencyId#%d> -> %d VoltTables", stmt_index, input_d_id, results.get(input_d_id).size()));
        } // FOR
        return (results);
    }
    
    @Override
    public synchronized String toString() {
        Map<String, Object> header = super.getDebugMap();
        
        String proc_name = (this.volt_procedure != null ? this.volt_procedure.getProcedureName() : null);
        ListOrderedMap<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.put("Procedure", proc_name);
        m0.put("Dtxn.Coordinator Callback", this.coordinator_callback);
        m0.put("Ignore Dtxn.Coordinator", this.ignore_dtxn);
        m0.put("Dependency Ctr", this.dependency_ctr);
        m0.put("Internal Ctr", this.internal_dependencies.size());
        m0.put("Received Ctr", this.received_ctr);
        m0.put("CountdownLatch", this.dependency_latch);
        m0.put("# of Blocked Tasks", this.blocked_tasks.size());
        m0.put("# of Statements", this.batch_size);
        m0.put("Expected Results", this.results_dependency_stmt_ctr.keySet());
        m0.put("Expected Responses", this.responses_dependency_stmt_ctr.keySet());
        
        ListOrderedMap<String, Object> m1 = new ListOrderedMap<String, Object>();
        m1.put("Original Txn Id", this.orig_txn_id);
        m1.put("Init Latch", this.init_latch);
        m1.put("Client Callback", this.client_callback);
        m1.put("SysProc", this.sysproc);
        m1.put("Done Partitions", this.done_partitions);
        m1.put("Predict Single-Partitioned", this.single_partitioned);
        m1.put("Speculative Execution", this.speculative);
        m1.put("Estimator State", this.estimator_state);
        
        ListOrderedMap<String, Object> m2 = new ListOrderedMap<String, Object>();
        m2.put("Total Time", this.total_time);
        m2.put("Java Time", this.java_time);
        m2.put("Coordinator Time", this.coord_time);
        m2.put("Planner Time", this.plan_time);
        m2.put("EE Time", this.ee_time);
        m2.put("Estimator Time", this.est_time);
        
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.formatMaps(header, m0, m1, m2));
        sb.append(StringUtil.SINGLE_LINE);

        String stmt_debug[] = new String[this.batch_size];
        for (int stmt_index = 0; stmt_index < this.batch_size; stmt_index++) {
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
