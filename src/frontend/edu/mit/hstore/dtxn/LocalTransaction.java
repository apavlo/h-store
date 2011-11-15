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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FragmentTaskMessage;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreConstants;
import edu.mit.hstore.HStoreObjectPools;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionFinishCallback;
import edu.mit.hstore.callbacks.TransactionInitCallback;
import edu.mit.hstore.callbacks.TransactionPrepareCallback;

/**
 * 
 * @author pavlo
 */
public class LocalTransaction extends AbstractTransaction {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();

    private static final Set<FragmentTaskMessage> EMPTY_SET = Collections.emptySet();
    
    // ----------------------------------------------------------------------------
    // TRANSACTION INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The original StoredProcedureInvocation request that was sent to the HStoreSite
     * XXX: Why do we need to keep this?
     */
    private StoredProcedureInvocation invocation;

    /**
     * The set of partitions that we expected this partition to touch.
     */
    private Collection<Integer> predict_touchedPartitions;
    
    /**
     * The partitions that we told the Dtxn.Coordinator that we were done with
     */
    protected final Set<Integer> done_partitions = new HashSet<Integer>();
    
    /**
     * A handle to the execution state of this transaction
     * This will only get set when the transaction starts running.
     */
    private ExecutionState state;
    
    /**
     * 
     */
    private Long orig_txn_id;
    
    private short restart_ctr = 0;
    
    /**
     * 
     */
    private Procedure catalog_proc;

    /**
     * Whether this is a sysproc
     */
    public boolean sysproc;

    /**
     * Whether this txn is being executed specutatively
     */
    private boolean exec_speculative = false;

    /**
     * TransctionEstimator State Handle
     */
    private TransactionEstimator.State estimator_state;
    
    /**
     * 
     */
    public final TransactionProfile profiler;
    
    /**
     * Cached ProtoRpcControllers
     */
    public final ProtoRpcController rpc_transactionInit[];
    public final ProtoRpcController rpc_transactionWork[];
    public final ProtoRpcController rpc_transactionPrepare[];
    public final ProtoRpcController rpc_transactionFinish[];
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    /**
     * This callback is used to release the transaction once we get
     * the acknowledgments back from all of the partitions that we're going to access.
     * This is only needed for distributed transactions. 
     */
    private TransactionInitCallback init_callback;
    
    /**
     * This callback is used to keep track of what partitions have replied that they are 
     * ready to commit/abort our transaction.
     * This is only needed for distributed transactions.
     */
    private TransactionPrepareCallback prepare_callback; 
    
    private TransactionFinishCallback finish_callback;
    
    /**
     * Final RpcCallback to the client
     */
    private RpcCallback<byte[]> client_callback;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    public LocalTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        this.profiler = (hstore_site.getHStoreConf().site.txn_profiling ? new TransactionProfile() : null);
        
        int num_sites = CatalogUtil.getNumberOfSites(hstore_site.getSite());
        this.rpc_transactionInit = new ProtoRpcController[num_sites];
        this.rpc_transactionWork = new ProtoRpcController[num_sites];
        this.rpc_transactionPrepare = new ProtoRpcController[num_sites];
        this.rpc_transactionFinish = new ProtoRpcController[num_sites];
    }

    @SuppressWarnings("unchecked")
    public LocalTransaction init(long txnId, long clientHandle, int base_partition,
                                 boolean predict_readOnly, boolean predict_canAbort) {
        assert(this.predict_touchedPartitions != null);
        super.init(txnId, clientHandle, base_partition,
                (this.predict_touchedPartitions.size() == 1), predict_readOnly, predict_canAbort, true);
        
        if (this.predict_singlePartition == false) {
            try {
                this.init_callback = HStoreObjectPools.CALLBACKS_TXN_INIT.borrowObject(); 
                this.init_callback.init(this);
                
                this.prepare_callback = HStoreObjectPools.CALLBACKS_TXN_PREPARE.borrowObject();
                this.prepare_callback.init(this);
                
                // Don't initialize this until later, because we need to know 
                // what the final status of the txn
                this.finish_callback = HStoreObjectPools.CALLBACKS_TXN_FINISH.borrowObject();
                
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return (this);
    }

    /**
     * Main initialization method for LocalTransaction
     * @param txnId
     * @param clientHandle
     * @param base_partition
     * @param predict_singlePartition
     * @param predict_readOnly
     * @param predict_canAbort
     * @param estimator_state
     * @param catalog_proc
     * @param invocation
     * @param client_callback
     * @return
     */
    public LocalTransaction init(long txnId, long clientHandle, int base_partition,
                                 Collection<Integer> predict_touchedPartitions, boolean predict_readOnly, boolean predict_canAbort,
                                 TransactionEstimator.State estimator_state,
                                 Procedure catalog_proc, StoredProcedureInvocation invocation, RpcCallback<byte[]> client_callback) {
        assert(predict_touchedPartitions != null && predict_touchedPartitions.isEmpty() == false);
        
        this.predict_touchedPartitions = predict_touchedPartitions;
        this.estimator_state = estimator_state;
        this.catalog_proc = catalog_proc;
        this.sysproc = catalog_proc.getSystemproc();
        this.invocation = invocation;
        this.client_callback = client_callback;
        
        return this.init(txnId, clientHandle, base_partition, predict_readOnly, predict_canAbort);
    }
    
    /**
     * Initialization that copies information from the mispredicted original TransactionState 
     * @param txnId
     * @param base_partition
     * @param orig
     * @return
     */
    public LocalTransaction init(long txnId, int base_partition, LocalTransaction orig,
                                 Collection<Integer> predict_touchedPartitions, boolean predict_readOnly, boolean predict_abortable) {
        this.predict_touchedPartitions = predict_touchedPartitions;
        this.orig_txn_id = orig.getTransactionId();
        this.catalog_proc = orig.catalog_proc;
        this.sysproc = orig.sysproc;
        this.invocation = orig.invocation;
        this.client_callback = orig.client_callback;
        // this.estimator_state = orig.estimator_state;
        
        // Append the profiling times
//        if (this.executor.getEnableProfiling()) {
//            this.total_time.appendTime(orig.total_time);
//            this.java_time.appendTime(orig.java_time);
//            this.coord_time.appendTime(orig.coord_time);
//            this.ee_time.appendTime(orig.ee_time);
//            this.est_time.appendTime(orig.est_time);
//        }
        
        return this.init(txnId, orig.client_handle, base_partition, predict_readOnly, predict_abortable);
    }
    
    /**
     * Testing Constructor
     * @param txnId
     * @param clientHandle
     * @param base_partition
     * @param predict_touchedPartitions
     * @param predict_readOnly
     * @param predict_canAbort
     * @return
     */
    public LocalTransaction init(long txnId, long clientHandle, int base_partition,
                                    Collection<Integer> predict_touchedPartitions, boolean predict_readOnly, boolean predict_canAbort) {
        this.predict_touchedPartitions = predict_touchedPartitions;
        return this.init(txnId, clientHandle, base_partition, predict_readOnly, predict_canAbort);
    }
    
    
    public void setExecutionState(ExecutionState state) {
        assert(this.state == null);
        this.state = state;
    }
    
    /**
     * Returns true if this LocalTransaction was actually started
     * in the ExecutionSite.
     * @return
     */
    public boolean wasExecuted() {
        return (this.state != null);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.catalog_proc != null && super.isInitialized());
    }
    
    @Override
    public void finish() {
        super.finish();

        // Return our LocalTransactionInitCallback
        if (this.init_callback != null) {
            HStoreObjectPools.CALLBACKS_TXN_INIT.returnObject(this.init_callback);
            this.init_callback = null;
        }
        // Return our TransactionPrepareCallback
        if (this.prepare_callback != null) {
            HStoreObjectPools.CALLBACKS_TXN_PREPARE.returnObject(this.prepare_callback);
            this.prepare_callback = null;
        }
        // Return our TransactionFinishCallback
        if (this.finish_callback != null) {
            HStoreObjectPools.CALLBACKS_TXN_FINISH.returnObject(this.finish_callback);
            this.finish_callback = null;
        }
        // Return our TransactionEstimator.State handle
        if (this.estimator_state != null) {
            TransactionEstimator.POOL_STATES.returnObject(this.estimator_state);
            this.estimator_state = null;
        }
        
        this.state = null;
        this.orig_txn_id = null;
        this.catalog_proc = null;
        this.sysproc = false;
        this.exec_speculative = false;
        this.predict_touchedPartitions = null;
        this.done_partitions.clear();
        this.restart_ctr = 0;
        
        if (this.profiler != null) this.profiler.finish();
    }
    
    public void setTransactionId(long txn_id) { 
        this.txn_id = txn_id;
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    @Override
    public void initRound(int partition, long undoToken) {
        assert(this.state != null);
        assert(this.state.queued_results.isEmpty()) : 
            String.format("Trying to initialize round for txn #%d but there are %d queued results",
                          this.txn_id, this.state.queued_results.size());

        super.initRound(partition, undoToken);
        
        // Reset these guys here so that we don't waste time in the last round
        if (this.base_partition == partition && this.last_undo_token[hstore_site.getLocalPartitionOffset(partition)] != null) {
            this.state.clearRound();
        }
    }
    
    public void fastInitRound(int partition, long undoToken) {
        super.initRound(partition, undoToken);
    }
    
    @Override
    public void startRound(int partition) {
        // Same site, different partition
        if (this.base_partition != partition) {
            super.startRound(partition);
            return;
        }
        
        // Same site, same partition
        assert(this.state.output_order.isEmpty());
        assert(this.state.batch_size > 0);
        if (d) LOG.debug("__FILE__:__LINE__ " +"Starting round for local txn #" + this.txn_id + " with " + this.state.batch_size + " queued Statements");
   
        if (this.predict_singlePartition == false) state.lock.lock();
        try {
            super.startRound(partition);

            // Create our output counters
            for (int stmt_index = 0; stmt_index < this.state.batch_size; stmt_index++) {
                for (DependencyInfo dinfo : this.state.dependencies[stmt_index].values()) {
                    if (this.state.internal_dependencies.contains(dinfo.dependency_id) == false) this.state.output_order.add(dinfo.dependency_id);
                } // FOR
            } // FOR
            assert(this.state.batch_size == this.state.output_order.size()) :
                "Expected " + this.getStatementCount() + " output dependencies but we queued up " + this.state.output_order.size();
            
            // Release any queued responses/results
            if (this.state.queued_results.isEmpty() == false) {
                if (t) LOG.trace("__FILE__:__LINE__ " +"Releasing " + this.state.queued_results.size() + " queued results");
                int key[] = new int[2];
                for (Entry<Integer, VoltTable> e : this.state.queued_results.entrySet()) {
                    this.state.getPartitionDependencyFromKey(e.getKey().intValue(), key);
                    this.processResultResponse(key[0], key[1], e.getKey().intValue(), e.getValue());
                } // FOR
                this.state.queued_results.clear();
            }
            
            // Now create the latch
            int count = this.state.dependency_ctr - this.state.received_ctr;
            assert(count >= 0);
            assert(this.state.dependency_latch == null) : "This should never happen!\n" + this.toString();
            this.state.dependency_latch = new CountDownLatch(count);
        } finally {
            if (this.predict_singlePartition == false) state.lock.unlock();
        } // SYNCH
    }
    
    @Override
    public void finishRound(int partition) {
        // Same site, different partition
        if (this.base_partition != partition) {
            super.finishRound(partition);
            return;
        }
        
        assert(this.state.dependency_ctr == this.state.received_ctr) : "Trying to finish round for " + this + " before it was started"; 
        assert(this.state.queued_results.isEmpty()) : "Trying to finish round for " + this + " but there are " + this.state.queued_results.size() + " queued results";

        if (this.predict_singlePartition == false) state.lock.lock();
        try {
            super.finishRound(partition);
            
            // Reset our initialization flag so that we can be ready to run more stuff the next round
            if (this.base_partition == partition && this.state.dependency_latch != null) {
                assert(this.state.dependency_latch.getCount() == 0);
                if (t) LOG.debug("__FILE__:__LINE__ " +"Setting CountDownLatch to null for txn #" + this.txn_id);
                this.state.dependency_latch = null;
            }
        } finally {
            if (this.predict_singlePartition == false) state.lock.unlock();
        } // SYNCH
    }
    
    /**
     * Quickly finish this round. Assumes that everything executed locally
     * and therefore we don't need any locks
     */
    public void fastFinishRound(int partition) {
        this.round_state[hstore_site.getLocalPartitionOffset(partition)] = RoundState.STARTED;
        super.finishRound(partition);
    }
    
    // ----------------------------------------------------------------------------
    // ERROR HANDLING
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param error
     * @param wakeThread
     */
    public void setPendingError(SerializableException error, boolean wakeThread) {
        boolean spin_latch = (this.pending_error == null);
        super.setPendingError(error);
        if (wakeThread == false) return;
        
        // Spin through this so that the waiting thread wakes up and sees that they got an error
        if (spin_latch) {
            while (this.state.dependency_latch.getCount() > 0) {
                this.state.dependency_latch.countDown();
            } // WHILE
        }        
    }
    
    @Override
    public synchronized void setPendingError(SerializableException error) {
        this.setPendingError(error, true);
    }

    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    public TransactionInitCallback getTransactionInitCallback() {
        return (this.init_callback);
    }
    public TransactionPrepareCallback getTransactionPrepareCallback() {
        return (this.prepare_callback);
    }
    public TransactionFinishCallback getTransactionFinishCallback(Hstore.Status status) {
        assert(this.finish_callback.isInitialized() == false);
        this.finish_callback.init(this, status);
        return (this.finish_callback);
    }
    public TransactionFinishCallback getTransactionFinishCallback() {
        return (this.finish_callback);
    }
    public RpcCallback<byte[]> getClientCallback() {
        return (this.client_callback);
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    public void setBatchSize(int batchSize) {
        this.state.batch_size = batchSize;
    }
    public StoredProcedureInvocation getInvocation() {
        return invocation;
    }

    /**
     * Return the original txn id that this txn was restarted for (after a mispredict)
     * @return
     */
    public Long getOriginalTransactionId() {
        return (this.orig_txn_id);
    }
    public short getRestartCounter() {
        return (this.restart_ctr);
    }
    public void setRestartCounter(int val) {
        this.restart_ctr = (short)val;
    }
    
    public Collection<Integer> getDonePartitions() {
        return (this.done_partitions);
    }
    public Histogram<Integer> getTouchedPartitions() {
        return (this.state.exec_touchedPartitions);
    }
    public boolean hasTouchedPartitions() {
        if (this.state != null) {
            return (this.state.exec_touchedPartitions != null);
        }
        return (false);
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
    }
    
    public int getDependencyCount() { 
        return (this.state.dependency_ctr);
    }
    public int getBlockedFragmentTaskMessageCount() {
        return (this.state.blocked_tasks.size());
    }
    protected Set<FragmentTaskMessage> getBlockedFragmentTaskMessages() {
        return (this.state.blocked_tasks);
    }
    public LinkedBlockingDeque<Collection<FragmentTaskMessage>> getUnblockedFragmentTaskMessageQueue() {
        return (this.state.unblocked_tasks);
    }
    
    public TransactionEstimator.State getEstimatorState() {
        return (this.estimator_state);
    }
    public void setEstimatorState(TransactionEstimator.State state) {
        this.estimator_state = state;
    }
    
    public CountDownLatch getDependencyLatch() {
        return this.state.dependency_latch;
    }
    
    /**
     * Return the number of statements that have been queued up in the last batch
     * @return
     */
    protected int getStatementCount() {
        return (this.state.batch_size);
    }
    protected Map<Integer, DependencyInfo> getStatementDependencies(int stmt_index) {
        return (this.state.dependencies[stmt_index]);
    }
    /**
     * 
     * @param stmt_index Statement Index
     * @param d_id Output Dependency Id
     * @return
     */
    protected DependencyInfo getDependencyInfo(int stmt_index, int d_id) {
        return (this.state.dependencies[stmt_index].get(d_id));
    }
    
    
    protected List<Integer> getOutputOrder() {
        return (this.state.output_order);
    }
    
    public Set<Integer> getInternalDependencyIds() {
        return (this.state.internal_dependencies);
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
        return (this.state.exec_touchedPartitions.getValueCount() <= 1);
    }
    /**
     * Returns true if the given FragmentTaskMessage is currently set as blocked for this txn
     * @param ftask
     * @return
     */
    public boolean isBlocked(FragmentTaskMessage ftask) {
        return (this.state.blocked_tasks.contains(ftask));
    }
    
    public Collection<Integer> getPredictTouchedPartitions() {
        return (this.predict_touchedPartitions);
    }
    
    // ----------------------------------------------------------------------------
    // ProtoRpcController CACHE
    // ----------------------------------------------------------------------------
    
    private ProtoRpcController getProtoRpcController(ProtoRpcController cache[], int site_id) {
//        return new ProtoRpcController();
        if (cache[site_id] == null) {
            cache[site_id] = new ProtoRpcController();
        } else {
            cache[site_id].reset();
        }
        return (cache[site_id]);
    }
    
    public ProtoRpcController getTransactionInitController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionInit, site_id);
    }
    public ProtoRpcController getTransactionWorkController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionWork, site_id);
    }
    public ProtoRpcController getTransactionPrepareController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionPrepare, site_id);
    }
    public ProtoRpcController getTransactionFinishController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionFinish, site_id);
    }
    
    // ----------------------------------------------------------------------------
    // DEPENDENCY TRACKING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param d_id
     * @return
     */
    private DependencyInfo getOrCreateDependencyInfo(int stmt_index, Integer d_id) {
        Map<Integer, DependencyInfo> stmt_dinfos = this.state.dependencies[stmt_index];
        if (stmt_dinfos == null) {
            stmt_dinfos = new ConcurrentHashMap<Integer, DependencyInfo>();
            this.state.dependencies[stmt_index] = stmt_dinfos;
        }
        DependencyInfo dinfo = stmt_dinfos.get(d_id);
        if (dinfo == null) {
            // First try to get one that we have used before in a previous round for this txn
            dinfo = this.state.reusable_dependencies.poll();
            if (dinfo != null) {
                dinfo.finish();
            // If there is nothing local, then we have to go get an object from the global pool
            } else {
                try {
                    dinfo = HStoreObjectPools.STATES_DEPENDENCYINFO.borrowObject();
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
    public VoltTable[] getResults() {
        final VoltTable results[] = new VoltTable[this.state.output_order.size()];
        if (d) LOG.debug("__FILE__:__LINE__ " +"Generating output results with " + results.length + " tables for txn #" + this.txn_id);
        for (int stmt_index = 0; stmt_index < results.length; stmt_index++) {
            Integer dependency_id = this.state.output_order.get(stmt_index);
            assert(dependency_id != null) :
                "Null output dependency id for Statement index " + stmt_index + " in txn #" + this.txn_id;
            assert(this.state.dependencies[stmt_index] != null) :
                "Missing dependency set for stmt_index #" + stmt_index + " in txn #" + this.txn_id;
            assert(this.state.dependencies[stmt_index].containsKey(dependency_id)) :
                "Missing info for DependencyId " + dependency_id + " for Statement index " + stmt_index + " in txn #" + this.txn_id;
            
            results[stmt_index] = this.state.dependencies[stmt_index].get(dependency_id).getResult();
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
        int offset = hstore_site.getLocalPartitionOffset(this.base_partition);
        assert(this.round_state[offset] == RoundState.INITIALIZED) :
            String.format("Invalid round state %s for %s at partition %d", this.round_state[offset], this, this.base_partition);
        
        // The partition that this task is being sent to for execution
        boolean blocked = false;
        final int partition = ftask.getDestinationPartitionId();
        final int num_fragments = ftask.getFragmentCount();
        this.state.exec_touchedPartitions.put(partition, num_fragments);
        
        // If this task produces output dependencies, then we need to make 
        // sure that the txn wait for it to arrive first
        if (ftask.hasOutputDependencies()) {
            int output_dependencies[] = ftask.getOutputDependencyIds();
            int stmt_indexes[] = ftask.getFragmentStmtIndexes();
            
            if (d) LOG.debug("__FILE__:__LINE__ " +String.format("Attemping to add DependencyInfo for %d fragments in %s",
                                            num_fragments, this));
            if (this.predict_singlePartition == false) state.lock.lock();
            try {
                for (int i = 0; i < num_fragments; i++) {
                    Integer dependency_id = output_dependencies[i];
                    Integer stmt_index = stmt_indexes[i];
                    
                    if (t) LOG.trace("__FILE__:__LINE__ " +"Adding new Dependency [stmt_index=" + stmt_index + ", id=" + dependency_id + ", partition=" + partition + "] for txn #" + this.txn_id);
                    this.getOrCreateDependencyInfo(stmt_index.intValue(), dependency_id).addPartition(partition);
                    this.state.dependency_ctr++;
    
                    // Store the stmt_index of when this dependency will show up
                    Integer key_idx = this.state.createPartitionDependencyKey(partition, dependency_id.intValue());
    
                    Queue<Integer> rest_stmt_ctr = this.state.results_dependency_stmt_ctr.get(key_idx);
                    if (rest_stmt_ctr == null) {
                        rest_stmt_ctr = new LinkedList<Integer>();
                        this.state.results_dependency_stmt_ctr.put(key_idx, rest_stmt_ctr);
                    }
                    rest_stmt_ctr.add(stmt_index);
                    if (t) LOG.trace("__FILE__:__LINE__ " +String.format("Set Dependency Statement Counters for <%d %d>: %s", partition, dependency_id, rest_stmt_ctr));
                } // FOR
            } finally {
                if (this.predict_singlePartition == false) state.lock.unlock();
            } // SYNCH
        }
        
        // If this task needs an input dependency, then we need to make sure it arrives at
        // the executor before it is allowed to start executing
        if (ftask.hasInputDependencies()) {
            if (t) LOG.trace("__FILE__:__LINE__ " +"Blocking fragments " + Arrays.toString(ftask.getFragmentIds()) + " waiting for " + ftask.getInputDependencyCount() + " dependencies in txn #" + this.txn_id + ": " + Arrays.toString(ftask.getAllUnorderedInputDepIds()));
            if (this.predict_singlePartition == false) state.lock.lock();
            try {
                for (int i = 0; i < num_fragments; i++) {
                    int dependency_id = ftask.getOnlyInputDepId(i);
                    int stmt_index = ftask.getFragmentStmtIndexes()[i];
                    this.getOrCreateDependencyInfo(stmt_index, dependency_id).addBlockedFragmentTaskMessage(ftask);
                    this.state.internal_dependencies.add(dependency_id);
                } // FOR
            } finally {
                if (this.predict_singlePartition == false) state.lock.unlock();
            } // SYNCH
            this.state.blocked_tasks.add(ftask);
            blocked = true;
        }
        if (d) {
            LOG.debug("__FILE__:__LINE__ " +"Queued up FragmentTaskMessage in txn #" + this.txn_id + " for partition " + partition + " and marked as" + (blocked ? "" : " not") + " blocked");
            if (t) LOG.trace("__FILE__:__LINE__ " +"FragmentTaskMessage Contents for txn #" + this.txn_id + ":\n" + ftask);
        }
        return (blocked);
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
        int key = this.state.createPartitionDependencyKey(partition, dependency_id);
        this.processResultResponse(partition, dependency_id, key, result);
    }
    
    /**
     * 
     * @param partition
     * @param dependency_id
     * @param result
     */
    private void processResultResponse(final int partition, final int dependency_id, final int key, VoltTable result) {
        int base_offset = hstore_site.getLocalPartitionOffset(this.base_partition);
        assert(result != null);
        assert(this.round_state[base_offset] == RoundState.INITIALIZED || this.round_state[base_offset] == RoundState.STARTED) :
            String.format("Invalid round state %s for %s at partition %d", this.round_state[base_offset], this, this.base_partition);
        DependencyInfo dinfo = null;
        Map<Integer, Queue<Integer>> stmt_ctr = this.state.results_dependency_stmt_ctr;
        
        if (debug.get()) LOG.debug("__FILE__:__LINE__ " +String.format("Attemping to add new result for {Partition:%d, Dependency:%d} in %s [numRows=%d]",
                                                  partition, dependency_id, this, result.getRowCount()));
        
        // If the txn is still in the INITIALIZED state, then we just want to queue up the results
        // for now. They will get released when we switch to STARTED 
        if (this.predict_singlePartition == false) state.lock.lock();
        try {
            if (this.round_state[base_offset] == RoundState.INITIALIZED) {
                assert(this.state.queued_results.containsKey(key) == false) : "Duplicate result " + key + " for txn #" + this.txn_id;
                this.state.queued_results.put(key, result);
                if (d) LOG.debug("__FILE__:__LINE__ " +"Queued result " + key + " for txn #" + this.txn_id + " until the round is started");
                return;
            }

            // Each partition+dependency_id should be unique for a Statement batch.
            // So as the results come back to us, we have to figure out which Statement it belongs to
            Queue<Integer> queue = stmt_ctr.get(key);
            if (t) {
                LOG.trace("__FILE__:__LINE__ " +"Storing new result for key " + key + " in txn #" + this.txn_id);
                LOG.trace("__FILE__:__LINE__ " +"Result stmt_ctr(key=" + key + "): " + queue);
            }
            assert(queue != null) :
                String.format("Unexpected {Partition:%d, Dependency:%d} in %s",
                              partition, dependency_id, this);
            assert(queue.isEmpty() == false) :
                String.format("No more statements for {Partition:%d, Dependency:%d} in %s [key=%d]\nresults_dependency_stmt_ctr = %s",
                              partition, dependency_id, this, key, this.state.results_dependency_stmt_ctr);
            
            int stmt_index = queue.remove().intValue();
            dinfo = this.getDependencyInfo(stmt_index, dependency_id);
            assert(dinfo != null) :
                "Unexpected DependencyId " + dependency_id + " from partition " + partition + " for txn #" + this.txn_id + " [stmt_index=" + stmt_index + "]\n" + result;

            dinfo.addResult(partition, result);
            this.state.received_ctr++;
            if (this.state.dependency_latch != null) {
                this.state.dependency_latch.countDown();
                    
                // HACK: If the latch is now zero, then push an EMPTY set into the unblocked queue
                long count = this.state.dependency_latch.getCount();
                if (count == 0) this.state.unblocked_tasks.offer(EMPTY_SET);
                if (d) LOG.debug("__FILE__:__LINE__ " +"Setting CountDownLatch to " + count + " for txn #" + this.txn_id);
            }
            
            if (d) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Blocked Tasks", this.state.blocked_tasks.size());
                m.put("DependencyInfo", dinfo);
                m.put("hasTasksReady", dinfo.hasTasksReady());
                LOG.debug("__FILE__:__LINE__ " +this + "\n" + StringUtil.formatMaps(m));
            }
            
            // Check whether we need to start running stuff now
            if (!this.state.blocked_tasks.isEmpty() && dinfo.hasTasksReady()) {
                // Always double check whether somebody beat us to the punch
                Collection<FragmentTaskMessage> to_unblock = dinfo.getAndReleaseBlockedFragmentTaskMessages();
                if (to_unblock == null) {
                    if (d) LOG.debug("__FILE__:__LINE__ " +String.format("No new FragmentTaskMessages available to unblock for txn #%d. Ignoring...", this.txn_id));
                    return;
                }
                if (d) LOG.debug("__FILE__:__LINE__ " +String.format("Got %d FragmentTaskMessages to unblock for txn #%d that were waiting for DependencyId %d",
                                               to_unblock.size(), this.txn_id, dinfo.getDependencyId()));
                this.state.blocked_tasks.removeAll(to_unblock);
                this.state.unblocked_tasks.add(to_unblock);
            }
        } finally {
            if (this.predict_singlePartition == false) state.lock.unlock();
        } // SYNCH
    }

    /**
     * Populate the given map with the the dependency results that are used for
     * internal plan execution. Note that these are not the results that should be
     * sent to the client.
     * @param ftask
     * @param results
     * @return
     */
    public Map<Integer, List<VoltTable>> removeInternalDependencies(final FragmentTaskMessage ftask, final Map<Integer, List<VoltTable>> results) {
        if (d) LOG.debug("__FILE__:__LINE__ " +String.format("Retrieving %d internal dependencies for txn #%d",
                                       this.state.internal_dependencies.size(), this.txn_id));
        
        Collection<Integer> localPartitionIds = hstore_site.getLocalPartitionIds();
        for (int i = 0, cnt = ftask.getFragmentCount(); i < cnt; i++) {
            int input_d_id = ftask.getOnlyInputDepId(i);
            if (input_d_id == HStoreConstants.NULL_DEPENDENCY_ID) continue;
            int stmt_index = ftask.getFragmentStmtIndexes()[i];

            DependencyInfo dinfo = this.getDependencyInfo(stmt_index, input_d_id);
            assert(dinfo != null);
            int num_tables = dinfo.results.size();
            assert(dinfo.getPartitions().size() == num_tables) :
                String.format("Number of results retrieved for <Stmt #%d, DependencyId #%d> is %d " +
                              "but we were expecting %d in %s txn #%d\n%s\n%s\n%s%s", 
                              stmt_index, input_d_id, num_tables, dinfo.getPartitions().size(), this.getProcedureName(), this.txn_id,
                              this.toString(), ftask.toString(),
                              StringUtil.SINGLE_LINE, this.debug()); 
            results.put(input_d_id, dinfo.getResults(localPartitionIds, true));
            if (d) LOG.debug("__FILE__:__LINE__ " +String.format("<Stmt#%d, DependencyId#%d> -> %d VoltTables",
                                           stmt_index, input_d_id, results.get(input_d_id).size()));
        } // FOR
        return (results);
    }
    
    @Override
    public String toString() {
        if (this.isInitialized()) {
            return (String.format("%s #%d/%d", this.getProcedureName(), this.txn_id, this.base_partition));
//            return (String.format("%s #%d/%d/%d", this.getProcedureName(), this.txn_id, this.base_partition, this.hashCode()));
        } else {
            return ("<Uninitialized>");
        }
    }
    
    @Override
    public String debug() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        Map<String, Object> m;
        
        // Basic Info
        m = super.getDebugMap();
        m.put("Procedure", this.getProcedureName());
        m.put("SysProc", this.sysproc);
        maps.add(m);
        
        // Predictions
        m = new ListOrderedMap<String, Object>();
        m.put("Predict Single-Partitioned", (this.predict_touchedPartitions != null ? this.isPredictSinglePartition() : "???"));
        m.put("Predict Touched Partitions", this.getPredictTouchedPartitions());
        m.put("Predict Read Only", this.isPredictReadOnly());
        m.put("Predict Abortable", this.isPredictAbortable());
        m.put("Restart Counter", this.restart_ctr);
        m.put("Estimator State", this.estimator_state);
        maps.add(m);
        
        // Actual Execution
        if (this.state != null) {
            m = new ListOrderedMap<String, Object>();
            m.put("Exec Single-Partitioned", this.isExecSinglePartition());
            m.put("Speculative Execution", this.exec_speculative);
            m.put("Exec Read Only", Arrays.toString(this.exec_readOnly));
            m.put("Touched Partitions", this.state.exec_touchedPartitions);
            m.put("Dependency Ctr", this.state.dependency_ctr);
            m.put("Internal Ctr", this.state.internal_dependencies.size());
            m.put("Received Ctr", this.state.received_ctr);
            m.put("CountdownLatch", this.state.dependency_latch);
            m.put("# of Blocked Tasks", this.state.blocked_tasks.size());
            m.put("# of Statements", this.state.batch_size);
            m.put("Expected Results", this.state.results_dependency_stmt_ctr.keySet());
            maps.add(m);
        }

        // Additional Info
        m = new ListOrderedMap<String, Object>();
        m.put("Original Txn Id", this.orig_txn_id);
        m.put("Client Callback", this.client_callback);
        m.put("Prepare Callback", this.prepare_callback);
        maps.add(m);

        // Profile Times
        if (this.profiler != null) maps.add(this.profiler.debugMap());
        
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()])));
        
        if (this.state != null) {
            sb.append(StringUtil.SINGLE_LINE);
            String stmt_debug[] = new String[this.state.batch_size];
            for (int stmt_index = 0; stmt_index < stmt_debug.length; stmt_index++) {
                Map<Integer, DependencyInfo> s_dependencies = new HashMap<Integer, DependencyInfo>(this.state.dependencies[stmt_index]); 
                Set<Integer> dependency_ids = new HashSet<Integer>(s_dependencies.keySet());
                String inner = "";
                inner += "  Statement #" + stmt_index + "\n";
                inner += "  Output Dependency Id: " + (this.state.output_order.contains(stmt_index) ? this.state.output_order.get(stmt_index) : "<NOT STARTED>") + "\n";
                
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
        }
        
        return (sb.toString());
    }
}
