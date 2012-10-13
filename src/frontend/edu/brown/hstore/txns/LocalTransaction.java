/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
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
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableNonBlocking;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionInitCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.TransactionEstimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.profilers.TransactionProfiler;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.statistics.Histogram;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class LocalTransaction extends AbstractTransaction {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();

    private static final Set<WorkFragment> EMPTY_FRAGMENT_SET = Collections.emptySet();
    
    // ----------------------------------------------------------------------------
    // TRANSACTION INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Catalog object of the Procedure that this transaction is currently executing
     */
    protected Procedure catalog_proc;
    
    /**
     * Final RpcCallback to the client
     */
    private RpcCallback<ClientResponseImpl> client_callback;
    
    /**
     * The final ClientResponse for this txn that needs to get sent
     * back to the client.
     */
    private ClientResponseImpl cresponse;
    
    /**
     * The number of times that this transaction has been restarted 
     */
    private int restart_ctr = 0;
    
    // ----------------------------------------------------------------------------
    // INTERNAL STATE
    // ----------------------------------------------------------------------------
    
    private boolean needs_restart = false; // FIXME
    private boolean deletable = false; // FIXME
    
    /**
     * Is this transaction part of a large MapReduce transaction  
     */
    private boolean mapreduce = false;
    
    /**
     * If set to true, then this will need to have an entry written
     * to the command log for its invocation
     */
    private boolean log_enabled = false;
    
    /**
     * The timestamp (from EstTime) that our transaction showed up
     * at this HStoreSite
     */
    private long initiateTime;
    
    /**
     * This is where we will store all of the special state information for distributed txns
     */
    private DistributedState dtxnState;
    
    /**
     * The table that this txn needs to merge the results for in the EE
     * before it starts executing
     */
    private Table anticache_table = null;
    
    /**
     * Special TransactionProfiler handle
     */
    public TransactionProfiler profiler;
    
    // ----------------------------------------------------------------------------
    // INITIAL PREDICTION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The set of partitions that we expected this partition to touch.
     */
    private PartitionSet predict_touchedPartitions;
  
    // ----------------------------------------------------------------------------
    // RUN TIME DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * A handle to the execution state of this transaction
     * This will only get set when the transaction starts running.
     * No two transactions are allowed to hold the same ExecutionState
     * at the same time.
     */
    private ExecutionState state;
    
    /**
     * The partitions that we told the Dtxn.Coordinator that we were done with
     */
    private final BitSet exec_donePartitions;
    
    /**
     * Whether this txn is being executed specutatively
     */
    private boolean exec_speculative = false;
    
    /** 
     * What partitions has this txn touched
     * This needs to be a Histogram so that we can figure out what partitions
     * were touched the most if end up needing to redirect it later on
     */
    private final Histogram<Integer> exec_touchedPartitions = new Histogram<Integer>();
//    private final FastIntHistogram exec_touchedPartitions;
    
    /**
     * Whether this transaction's control code was executed on
     * its base partition.
     */
    private boolean exec_controlCode = false;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * This does not fully initialize this transaction.
     * You must call init() before this can be used
     */
    public LocalTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        CatalogContext catalogContext = hstore_site.getCatalogContext(); 
        this.exec_donePartitions = new BitSet(catalogContext.numberOfPartitions);
//        this.exec_touchedPartitions = new FastIntHistogram(num_partitions);
    }

    /**
     * Main initialization method for LocalTransaction
     * @param txn_id
     * @param clientHandle
     * @param base_partition
     * @param predict_touchedPartitions
     * @param predict_readOnly
     * @param predict_abortable
     * @param catalog_proc
     * @param params
     * @param client_callback
     * @return
     */
    public LocalTransaction init(Long txn_id,
                                 long initiateTime,
                                 long clientHandle,
                                 int base_partition,
                                 PartitionSet predict_touchedPartitions,
                                 boolean predict_readOnly,
                                 boolean predict_abortable,
                                 Procedure catalog_proc,
                                 ParameterSet params,
                                 RpcCallback<ClientResponseImpl> client_callback) {
        assert(predict_touchedPartitions != null);
        assert(predict_touchedPartitions.isEmpty() == false);
        assert(catalog_proc != null) : "Unexpected null Procedure catalog handle";
        
        this.initiateTime = initiateTime;
        this.catalog_proc = catalog_proc;
        this.client_callback = client_callback;
        this.parameters = params;
        this.mapreduce = catalog_proc.getMapreduce();
        
        // Initialize the predicted execution properties for this transaction
        this.predict_touchedPartitions = predict_touchedPartitions;
        this.predict_readOnly = predict_readOnly;
        this.predict_abortable = predict_abortable;
        
        super.init(txn_id,
                   clientHandle,
                   base_partition,
                   catalog_proc.getId(),
                   catalog_proc.getSystemproc(),
                   (this.predict_touchedPartitions.size() == 1),
                   predict_readOnly,
                   predict_abortable,
                   true);
        
        
        // Grab a DistributedState that will have all the goodies that we need
        // to execute a distributed transaction
        if (this.predict_singlePartition == false) {
            try {
                this.dtxnState = hstore_site.getObjectPools().getDistributedStatePool(base_partition).borrowObject(); 
                this.dtxnState.init(this);
            } catch (Exception ex) {
                throw new RuntimeException("Unexpected error when trying to initialize " + this, ex);
            }
        }
        
        return (this);
    }
    
    /**
     * Testing Constructor
     * @param txn_id
     * @param base_partition
     * @param predict_touchedPartitions
     * @param catalog_proc
     * @return
     */
    public LocalTransaction testInit(Long txn_id,
                                      int base_partition,
                                      PartitionSet predict_touchedPartitions,
                                      Procedure catalog_proc) {
        this.predict_touchedPartitions = predict_touchedPartitions;
        this.catalog_proc = catalog_proc;
        this.initiateTime = EstTime.currentTimeMillis();
        boolean predict_singlePartition = (this.predict_touchedPartitions.size() == 1);
        if (predict_singlePartition == false) {
            this.dtxnState = new DistributedState(hstore_site).init(this);
        }
        
        return (LocalTransaction)super.init(
                          txn_id,                       // TxnId
                          Integer.MAX_VALUE,            // ClientHandle
                          base_partition,               // BasePartition
                          catalog_proc.getId(),         // ProcedureId
                          catalog_proc.getSystemproc(), // SysProc
                          predict_singlePartition,      // SinglePartition
                          catalog_proc.getReadonly(),   // ReadOnly
                          true,                         // Abortable
                          true                          // ExecLocal
        );
    }
    
    /**
     * Testing Constructor with Parameters and Callback
     * @param txn_id
     * @param base_partition
     * @param predict_touchedPartitions
     * @param catalog_proc
     * @param proc_params
     * @return
     */
    public LocalTransaction testInit(Long txn_id,
                                     int base_partition,
                                     PartitionSet predict_touchedPartitions,
                                     Procedure catalog_proc,
                                     Object... proc_params) {
        this.parameters = new ParameterSet(proc_params);
        this.client_callback = new RpcCallback<ClientResponseImpl>() {
            public void run(ClientResponseImpl parameter) {}
        };
        return this.testInit(txn_id,
                             base_partition,
                             predict_touchedPartitions,
                             catalog_proc);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.catalog_proc != null && super.isInitialized());
    }
    
    @Override
    public void finish() {
        if (d) LOG.debug(String.format("%s - Invoking finish() cleanup", this));
        
        // Return our DistributedState
        if (this.dtxnState != null) {
            hstore_site.getObjectPools()
                       .getDistributedStatePool(this.base_partition)
                       .returnObject(this.dtxnState);
            this.dtxnState = null;
        }
        
        this.resetExecutionState();
        super.finish();
        
        this.catalog_proc = null;
        this.client_callback = null;
        this.initiateTime = 0;
        this.cresponse = null;
        
        this.exec_controlCode = false;
        this.exec_speculative = false;
        this.exec_touchedPartitions.clear();
        this.predict_touchedPartitions = null;
        this.exec_donePartitions.clear();
        this.restart_ctr = 0;

        this.anticache_table = null;
        this.log_enabled = false;
        this.needs_restart = false;
        this.deletable = false;
        
        if (this.profiler != null) this.profiler.finish();
    }
    
    // ----------------------------------------------------------------------------
    // SPECIAL SETTER METHODS
    // ----------------------------------------------------------------------------
    
    public final void setClientResponse(ClientResponseImpl cresponse) {
        assert(this.cresponse == null);
        this.cresponse = cresponse;
    }
    public final ClientResponseImpl getClientResponse() {
        return (this.cresponse);
    }
    
    
    /**
     * <B>Note:</B> This should never be called by anything other than the TransactionInitializer
     * @param txn_id
     */
    public void setTransactionId(Long txn_id) { 
        this.txn_id = txn_id;
    }
    
    public void setExecutionState(ExecutionState state) {
        if (d) LOG.debug(String.format("%s - Setting ExecutionState handle [isNull=%s]",
                                       this, (this.state == null)));
        assert(state != null);
        assert(this.state == null);
        this.state = state;
        
        // Reset this so that we will call finish() on the cached DependencyInfos
        // before we try to use it again
//        for (int i = 0; i < this.state.dinfo_lastRound.length; i++) {
//            this.state.dinfo_lastRound[i] = -1;
//        } // FOR
    }
    
    public ExecutionState getExecutionState() {
        return (this.state);
    }
    public void resetExecutionState() {
        if (d) LOG.debug(String.format("%s - Resetting ExecutionState handle [isNull=%s]",
                                       this, (this.state == null)));
        if (this.state != null) {
            ReentrantLock lock = state.lock;
            lock.lock();
            try {
                this.state = null;
            } finally {
                lock.unlock();
            }
        }
        
    }
    
    /**
     * Marks that this transaction's control code was executed at its base partition 
     */
    public void markAsExecuted() {
        this.exec_controlCode = true;
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    @Override
    public void initRound(int partition, long undoToken) {
        // They are allowed to not have the ExecutionState handle if this partition is
        // executing a prefetchable query, which may be queued up before the
        // transaction's control code starts executing
        // Of course if this is the base partition, then we *definitely* need
        // to have the ExecutionState.
        if (partition == this.base_partition) {
            assert(this.state != null) :
                String.format("Trying to initalize new round for %s on partition %d but the ExecutionState is null",
                              this, partition);
            assert(this.state.queued_results.isEmpty()) : 
                String.format("Trying to initialize ROUND #%d for %s but there are %d queued results",
                               this.round_ctr[this.hstore_site.getLocalPartitionOffset(partition)],
                               this, this.state.queued_results.size());
        }

        if (d) LOG.debug(String.format("%s - Initializing ROUND #%d on partition %d [undoToken=%d]", 
                                       this, this.round_ctr[this.hstore_site.getLocalPartitionOffset(partition)],
                                       partition, undoToken));
        
        super.initRound(partition, undoToken);
        
        if (this.base_partition == partition) {
            // Reset these guys here so that we don't waste time in the last round
            if (this.getLastUndoToken(partition) != HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
                this.state.clearRound();
            }
        }
    }
    
    /**
     * Fast-path round initialization. This should be called when the next
     * batch contains only local, single-partition queries
     * @param partition
     * @param undoToken
     */
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
        int base_partition_offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.state.output_order.isEmpty());
        assert(this.state.batch_size > 0);
        if (d) LOG.debug(String.format("%s - Starting ROUND #%d on partition %d with %d queued Statements [blocked=%d]", 
                                       this, this.round_ctr[base_partition_offset],
                                       partition, this.state.batch_size, this.state.blocked_tasks.size()));
   
        if (this.predict_singlePartition == false) this.state.lock.lock();
        try {
            // Create our output counters
            for (int stmt_index = 0; stmt_index < this.state.batch_size; stmt_index++) {
                if (t) LOG.trace(String.format("%s - Examining %d dependencies at stmt_index %d",
                                               this, this.state.dependencies.size(), stmt_index));
                for (DependencyInfo dinfo : this.state.dependencies.values()) {
                    // Add this DependencyInfo our output list if it's being used in this round for this txn
                    // and if it is not an internal dependency
                    if (dinfo.inSameTxnRound(this.txn_id, this.round_ctr[base_partition_offset]) &&
                        dinfo.isInternal() == false && dinfo.getStatementIndex() == stmt_index) {
                        this.state.output_order.add(dinfo.getDependencyId());
                    }
                } // FOR
            } // FOR
            assert(this.state.batch_size == this.state.output_order.size()) :
                String.format("%s - Expected %d output dependencies but we queued up %d\n%s",
                              this, this.state.batch_size, this.state.output_order.size(),
                              StringUtil.join("\n", this.state.output_order));
            
            // Release any queued responses/results
            if (this.state.queued_results.isEmpty() == false) {
                if (t) LOG.trace("Releasing " + this.state.queued_results.size() + " queued results");
                for (Entry<Pair<Integer, Integer>, VoltTable> e : this.state.queued_results.entrySet()) {
                    this.addResult(e.getKey(), e.getValue(), true);
                } // FOR
                this.state.queued_results.clear();
            }
            
            // Now create the latch
            int count = this.state.dependency_ctr - this.state.received_ctr;
            assert(count >= 0);
            assert(this.state.dependency_latch == null) : "This should never happen!\n" + this.toString();
            this.state.dependency_latch = new CountDownLatch(count);
            
            // It's now safe to change our state to STARTED
            super.startRound(partition);
        } finally {
            if (this.predict_singlePartition == false) this.state.lock.unlock();
        } // SYNCH
    }
    
    @Override
    public void finishRound(int partition) {
        if (d) LOG.debug(String.format("%s - Finishing ROUND #%d on partition %d", 
                                       this, this.round_ctr[this.hstore_site.getLocalPartitionOffset(partition)], partition));
        
        if (this.base_partition == partition) {
            // Same site, same partition
            assert(this.state.dependency_ctr == this.state.received_ctr) :
                String.format("Trying to finish ROUND #%d on partition %d for %s before it was started",
                              this.round_ctr[this.hstore_site.getLocalPartitionOffset(partition)],
                              partition, this);
            assert(this.state.queued_results.isEmpty()) :
                String.format("Trying to finish ROUND #%d on partition %d for %s but there are %d queued results",
                              this.round_ctr[this.hstore_site.getLocalPartitionOffset(partition)],
                              partition, this, this.state.queued_results.size());
        }
        
        // This doesn't need to be synchronized because we know that only our
        // thread should be calling this
        super.finishRound(partition);
        
        // Same site, different partition
        if (this.base_partition != partition) return;
        
//        if (this.predict_singlePartition == false) {
//            for (int i = 0; i < this.state.batch_size; i++) {
//                this.state.dinfo_lastRound[i] = -1;
//            } // FOR
//        }
        
        if (this.predict_singlePartition == false) this.state.lock.lock();
        try {
            // Reset our initialization flag so that we can be ready to run more stuff the next round
            if (this.state.dependency_latch != null) {
                assert(this.state.dependency_latch.getCount() == 0);
                if (t) LOG.debug("Setting CountDownLatch to null for txn #" + this.txn_id);
                this.state.dependency_latch = null;
            }
            this.state.clearRound();
        } finally {
            if (this.predict_singlePartition == false) this.state.lock.unlock();
        } // SYNCH
    }
    
    /**
     * Quickly finish this round for a single-partition txn. This allows us
     * to change the state to FINISHED without having to go through the
     * INIT and START states first (since we know that we will not be getting results randomly
     * from WorkFragments executed on remote partitions). 
     * @param partition The partition to finish this txn on
     */
    public void fastFinishRound(int partition) {
        this.round_state[hstore_site.getLocalPartitionOffset(partition)] = RoundState.STARTED;
        super.finishRound(partition);
        if (this.base_partition == partition) {
            assert(this.state != null) : "Unexpected null ExecutionState for " + this;
            this.state.clearRound();
        }
    }
    
    // ----------------------------------------------------------------------------
    // ERROR HANDLING
    // ----------------------------------------------------------------------------
    
    /**
     * Set the pending error for this transaction. If wakeThread is true, then
     * the transaction will be released from its lock so that the transaction can be
     * aborted without needing to wait for all of the results to return. 
     * @param error
     * @param interruptThread
     */
    public void setPendingError(SerializableException error, boolean interruptThread) {
        interruptThread = (this.pending_error == null && interruptThread);
        super.setPendingError(error);
        if (interruptThread == false) return;
        
        try {
            // And then shove an empty result at them
            this.state.unblocked_tasks.addLast(EMPTY_FRAGMENT_SET);
            
            // Spin through this so that the waiting thread wakes up and sees that they got an error
            while (this.state.dependency_latch.getCount() > 0) {
                this.state.dependency_latch.countDown();
            } // WHILE
        } catch (NullPointerException ex) {
            // HACK!
        }
    }
    
    @Override
    public void setPendingError(SerializableException error) {
        this.setPendingError(error, true);
    }

    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    public TransactionInitCallback initTransactionInitCallback() {
        assert(this.dtxnState != null) :
            "Trying to access DistributedState for non distributed txn " + this + "\n" + this.debug();
        assert(this.dtxnState.init_callback.isInitialized() == false) :
            String.format("Trying initialize the %s for %s more than once",
                          this.dtxnState.init_callback.getClass().getSimpleName(), this);
        this.dtxnState.init_callback.init(this);
        return (this.dtxnState.init_callback);
    }
    public TransactionPrepareCallback getOrInitTransactionPrepareCallback() {
        assert(this.dtxnState != null) :
            "Trying to access DistributedState for non distributed txn " + this + "\n" + this.debug();
        if (this.dtxnState.prepare_callback.isInitialized() == false) {
            this.dtxnState.prepare_callback.init(this);
        }
        return (this.dtxnState.prepare_callback);
    }

    /**
     * Initialize the TransactionFinishCallback for this transaction using the
     * given status indicator. You should always use this callback and not allocate
     * one yourself!
     * @param status
     * @return
     */
    public TransactionFinishCallback initTransactionFinishCallback(Hstoreservice.Status status) {
        assert(this.dtxnState != null) :
            "Trying to access DistributedState for non distributed txn " + this;
        assert(this.dtxnState.finish_callback.isInitialized() == false) :
            String.format("Trying initialize the %s for %s more than once",
                    this.dtxnState.finish_callback.getClass().getSimpleName(), this);
        // Don't initialize this until later, because we need to know 
        // what the final status of the txn
        this.dtxnState.finish_callback.init(this, status);
        return (this.dtxnState.finish_callback);
    }
    public TransactionFinishCallback getTransactionFinishCallback() {
        assert(this.dtxnState != null) :
            "Trying to access DistributedState for non distributed txn " + this;
        assert(this.dtxnState.finish_callback.isInitialized()) :
            "Trying to use TransactionFinishCallback for " + this + " before it is intialized";
        return (this.dtxnState.finish_callback);
    }
    
    /**
     * Return the original callback that will send the final results back to the client
     * @return
     */
    public RpcCallback<ClientResponseImpl> getClientCallback() {
        return (this.client_callback);
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the underlying procedure catalog object
     * The VoltProcedure must have already been set
     * @return
     */
    public Procedure getProcedure() {
        return (this.catalog_proc);
    }
    
    public void removeProcedureParameters() {
        this.parameters = null;
    }

    /**
     * Returns true if all of the partitions that this txn is predicted
     * to access are all on the same HStoreSite as its base partition
     */
    public boolean isPredictAllLocal() {
        if (this.dtxnState != null) {
            return (this.dtxnState.is_all_local);
        }
        return (true);
    }
    
    /**
     * Returns true if the control code for this LocalTransaction was actually started
     * in the PartitionExecutor
     */
    public boolean wasExecuted() {
        return (this.exec_controlCode);
    }
    
    @Override
    public boolean needsFinish(int partition) {
        if (this.base_partition == partition) {
            return (this.exec_controlCode);
        }
        return super.needsFinish(partition);
    }
    
    /**
     * Mark this transaction as needing to be restarted. This will prevent it from
     * being deleted immediately
     */
    public final void markNeedsRestart() {
        assert(this.needs_restart == false) :
            "Trying to enable " + this + " internal needs_restart flag twice";
        this.needs_restart = true;
    }
    
    /**
     * Unmark this transaction as needing to be restarted. This can be safely 
     * invoked multiple times 
     */
    public final void unmarkNeedsRestart() {
        this.needs_restart = false;
    }
    
    /**
     * Returns true if we believe that this transaction can be deleted
     * Note that this will only return true once and only once for each transaction invocation.
     * <B>Note:</B> This is not thread safe!
     * @return
     */
    public boolean isDeletable() {
        if (this.isInitialized() == false) {
            return (false);
        }
        if (this.dtxnState != null) {
            if (this.dtxnState.init_callback.allCallbacksFinished() == false) {
                if (t) LOG.warn(String.format("%s - %s is not finished", this,
                                this.dtxnState.init_callback.getClass().getSimpleName()));
                return (false);
            }
            if (this.dtxnState.prepare_callback.allCallbacksFinished() == false) {
                if (t) LOG.warn(String.format("%s - %s is not finished", this,
                                this.dtxnState.prepare_callback.getClass().getSimpleName()));
                return (false);
            }
            if (this.dtxnState.finish_callback.allCallbacksFinished() == false) {
                if (t) LOG.warn(String.format("%s - %s is not finished", this,
                                this.dtxnState.finish_callback.getClass().getSimpleName()));
                return (false);
            }
        }
        if (this.needs_restart) {
            if (t) LOG.warn(String.format("%s - Needs restart, can't delete now", this));
            return (false);
        }
        if (this.deletable) return (false);
        this.deletable = true;
        return (true);
    }
    public final boolean checkDeletableFlag() {
        return (this.deletable);
    }

    /**
     * Returns true if this transaction is part of a MapReduce transaction 
     * @return
     */
    public boolean isMapReduce() {
        return (this.mapreduce);
    }
    
    public void markMapReduce() {
        this.mapreduce = true;
    }
    
    /**
     * Get the timestamp that this LocalTransaction handle was initiated
     */
    public long getInitiateTime() {
        return (this.initiateTime);
    }
    
    /**
     * Set the number of Statements being executed in the current batch 
     * @param batchSize
     */
    public final void setBatchSize(int batchSize) {
        this.state.batch_size = batchSize;
    }

    /**
     * Return the number of times that this transaction was restarted
     * @return
     */
    public int getRestartCounter() {
        return (this.restart_ctr);
    }
    
    /**
     * Set the number of times that this transaction has been restarted
     * @param val
     */
    public void setRestartCounter(int val) {
        this.restart_ctr = val;
    }
    
    public boolean hasDonePartitions() {
        return (this.exec_donePartitions.cardinality() > 0);
    }
    
    /**
     * Get all of the partitions that have been marked done for this txn
     * @return
     */
    public BitSet getDonePartitions() {
        return (this.exec_donePartitions);
    }
    
    
    public Histogram<Integer> getTouchedPartitions() {
        return (this.exec_touchedPartitions);
    }

    
    public TransactionProfiler getProfiler() {
        return (this.profiler);
    }
    public void setProfiler(TransactionProfiler profiler) {
        this.profiler = profiler;
    }
    
    /**
     * 
     * @param d_id Output Dependency Id
     * @return
     */
    protected DependencyInfo getDependencyInfo(int d_id) {
        return (this.state.dependencies.get(d_id));
    }

    @Override
    public boolean isExecReadOnly(int partition) {
        if (catalog_proc.getReadonly()) return (true);
        return super.isExecReadOnly(partition);
    }
    
    /**
     * Returns true if this Transaction has executed only on a single-partition
     * @return
     */
    public boolean isExecSinglePartition() {
        return (this.exec_touchedPartitions.getValueCount() <= 1);
    }
    /**
     * Returns true if the given WorkFragment is currently set as blocked for this txn
     * @param ftask
     * @return
     */
    public boolean isBlocked(WorkFragment ftask) {
        return (this.state.blocked_tasks.contains(ftask));
    }
    
    /**
     * Return the collection of the partitions that this transaction is expected
     * to need during its execution. The transaction may choose to not use all of
     * these but it is not allowed to use more.
     */
    public PartitionSet getPredictTouchedPartitions() {
        return (this.predict_touchedPartitions);
    }
    
    // ----------------------------------------------------------------------------
    // SPECULATIVE EXECUTION
    // ----------------------------------------------------------------------------
    
    /**
     * Set the flag that indicates whether this transaction was executed speculatively
     */
    public void setSpeculative(boolean speculative) {
        this.exec_speculative = speculative;
    }

    /**
     * Returns true if this transaction was executed speculatively
     */
    public boolean isSpeculative() {
        return (this.exec_speculative);
    }
    
    // ----------------------------------------------------------------------------
    // COMMAND LOGGING
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this txn as needing to have a log entry written to disk
     */
    public void markLogEnabled() {
        assert(this.log_enabled == false) :
            "Trying to mark " + this + " as needing to be logged more than once";
        this.log_enabled = true;
    }
    
    /**
     * Returns true if this txn needs to have a command log entry written for it
     * @return
     */
    public boolean isLogEnabled() {
        return (this.log_enabled);
    }
    
    // ----------------------------------------------------------------------------
    // PREFETCHABLE QUERIES
    // ----------------------------------------------------------------------------
    
    public void addPrefetchFragmentId(int fragmentId) {
        assert(this.prefetch != null);
        this.prefetch.fragmentIds.add(fragmentId);
    }
    
    public void addPrefetchResults(WorkResult result) {
        assert(this.prefetch != null);
        this.prefetch.results.add(result);
    }
    
    // ----------------------------------------------------------------------------
    // ANTI-CACHING
    // ----------------------------------------------------------------------------
    
    public boolean hasAntiCacheMergeTable() {
        return (this.anticache_table != null);
    }
    
    public Table getAntiCacheMergeTable() {
        return (this.anticache_table);
    }
    
    public void setAntiCacheMergeTable(Table catalog_tbl) {
        assert(this.anticache_table == null);
        this.anticache_table = catalog_tbl;
    }
    
    // ----------------------------------------------------------------------------
    // ProtoRpcController CACHE
    // ----------------------------------------------------------------------------
    
    public ProtoRpcController getTransactionInitController(int site_id) {
        return this.dtxnState.getProtoRpcController(this.dtxnState.rpc_transactionInit, site_id);
    }
    public ProtoRpcController getTransactionWorkController(int site_id) {
        return this.dtxnState.getProtoRpcController(this.dtxnState.rpc_transactionWork, site_id);
    }
    public ProtoRpcController getTransactionPrepareController(int site_id) {
        return this.dtxnState.getProtoRpcController(this.dtxnState.rpc_transactionPrepare, site_id);
    }
    public ProtoRpcController getTransactionFinishController(int site_id) {
        return this.dtxnState.getProtoRpcController(this.dtxnState.rpc_transactionFinish, site_id);
    }
    
    // ----------------------------------------------------------------------------
    // DEPENDENCY TRACKING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param dep_id
     * @return
     */
    private DependencyInfo getOrCreateDependencyInfo(int stmt_index, Integer dep_id) {
        Map<Integer, DependencyInfo> stmt_dinfos = this.state.dependencies;
        DependencyInfo dinfo = stmt_dinfos.get(dep_id);
        int base_partition_offset = hstore_site.getLocalPartitionOffset(this.base_partition);
        int currentRound = this.round_ctr[base_partition_offset]; 
        
        if (dinfo != null) {
            if (d) LOG.debug(String.format("%s - Reusing DependencyInfo[%d] for %s. " +
                                           "Checking whether it needs to be reset [currentRound=%d / lastRound=%d lastTxn=%s]",
                                           this, dinfo.hashCode(), debugStmtDep(stmt_index, dep_id),
                                           currentRound, dinfo.getRound(), dinfo.getTransactionId()));
            if (dinfo.inSameTxnRound(this.txn_id, currentRound) == false) {
                if (d) LOG.debug(String.format("%s - Clearing out DependencyInfo[%d].",
                                               this, dinfo.hashCode()));
                dinfo.finish();
            }
        } else {
            dinfo = new DependencyInfo();
            stmt_dinfos.put(dep_id, dinfo);
            if (d) LOG.debug(String.format("%s - Created new DependencyInfo for %s [hashCode=%d]",
                                           this, debugStmtDep(stmt_index, dep_id), dinfo.hashCode()));
        }
        if (dinfo.isInitialized() == false) {
            dinfo.init(this.txn_id, currentRound, stmt_index, dep_id.intValue());
        }
        
        return (dinfo);
    }
    
    
    /**
     * Get the final results of the last round of execution for this Transaction
     * This should only be called to get the VoltTables that you want to send into
     * the Java stored procedure code (e.g., the return value for voltExecuteSql())
     * @return
     */
    public VoltTable[] getResults() {
        final VoltTable results[] = new VoltTable[this.state.output_order.size()];
        if (d) LOG.debug(String.format("%s - Generating output results with %d tables",
                         this, results.length));
        
        HStoreConf hstore_conf = hstore_site.getHStoreConf();
        boolean nonblocking = (hstore_conf.site.specexec_nonblocking && this.sysproc == false && this.profiler != null);
        for (int stmt_index = 0; stmt_index < results.length; stmt_index++) {
            Integer dependency_id = this.state.output_order.get(stmt_index);
            assert(dependency_id != null) :
                "Null output dependency id for Statement index " + stmt_index + " in txn #" + this.txn_id;
//            assert(this.state.dependencies[stmt_index] != null) :
//                "Missing dependency set for stmt_index #" + stmt_index + " in txn #" + this.txn_id;
            assert(this.state.dependencies.containsKey(dependency_id)) :
                String.format("Missing info for %s in %s", debugStmtDep(stmt_index, dependency_id), this); 
            
            VoltTable vt = this.state.dependencies.get(dependency_id).getResult();

            // Special Non-Blocking Wrapper
            if (nonblocking) {
                VoltTableNonBlocking vtnb = new VoltTableNonBlocking(hstore_conf.site.txn_profiling ? this.profiler : null);
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
     * Queues up a WorkFragment for this txn
     * If the return value is true, then the WorkFragment is blocked waiting for dependencies
     * If the return value is false, then the WorkFragment can be executed immediately (either locally or on at a remote partition)
     * @param fragment
     */
    public boolean addWorkFragment(WorkFragment fragment) {
        assert(this.round_state[hstore_site.getLocalPartitionOffset(this.base_partition)] == RoundState.INITIALIZED) :
            String.format("Invalid round state %s for %s at partition %d", this.round_state[hstore_site.getLocalPartitionOffset(this.base_partition)], this, this.base_partition);
        
        // The partition that this task is being sent to for execution
        boolean blocked = false;
        final int partition = fragment.getPartitionId();
        final int num_fragments = fragment.getFragmentIdCount();
        
        if (d) LOG.debug(String.format("%s - Adding %s for partition %d with %d fragments",
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
                DependencyInfo dinfo = this.getOrCreateDependencyInfo(stmt_index, output_dep_id);
                dinfo.addPartition(partition);
                if (d) LOG.debug(String.format("%s - Adding new DependencyInfo %s for PlanFragment %d at Partition %d [ctr=%d]\n%s",
                                               this, debugStmtDep(stmt_index, output_dep_id),
                                               fragment.getFragmentId(i), this.state.dependency_ctr,
                                               partition, dinfo.toString()));
                this.state.dependency_ctr++;
                
                // Store the stmt_index of when this dependency will show up
                Pair<Integer, Integer> key = Pair.of(partition, output_dep_id);
                Queue<Integer> rest_stmt_ctr = this.state.results_dependency_stmt_ctr.get(key);
                if (rest_stmt_ctr == null) {
                    rest_stmt_ctr = new LinkedList<Integer>();
                    this.state.results_dependency_stmt_ctr.put(key, rest_stmt_ctr);
                }
                rest_stmt_ctr.add(stmt_index);
                if (t) LOG.trace(String.format("%s - Set Dependency Statement Counters for <%d %d>: %s",
                                               this, partition, output_dep_id, rest_stmt_ctr));
            } // IF
            
            // If this WorkFragment needs an input dependency, then we need to make sure it arrives at
            // the executor before it is allowed to start executing
            WorkFragment.InputDependency input_dep_ids = fragment.getInputDepId(i);
            if (input_dep_ids.getIdsCount() > 0) {
                for (Integer dependency_id : input_dep_ids.getIdsList()) {
                    if (dependency_id.intValue() != HStoreConstants.NULL_DEPENDENCY_ID) {
                        DependencyInfo dinfo = this.getOrCreateDependencyInfo(stmt_index, dependency_id);
                        dinfo.addBlockedWorkFragment(fragment);
                        dinfo.markInternal();
                        if (blocked == false) {
                            this.state.blocked_tasks.add(fragment);
                            blocked = true;   
                        }
                        if (d) LOG.debug(String.format("%s - Created internal input dependency %d for PlanFragment %d\n%s", 
                                                       this, dependency_id, fragment.getFragmentId(i), dinfo.toString()));
                    }
                } // FOR
            }
            
            // *********************************** DEBUG ***********************************
            if (t) {
                StringBuilder sb = new StringBuilder();
                int output_ctr = 0;
                int dep_ctr = 0;
                for (DependencyInfo dinfo : this.state.dependencies.values()) {
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
        if (d) {
            CatalogType catalog_obj = null;
            if (catalog_proc.getSystemproc()) {
                catalog_obj = catalog_proc;
            } else {
                for (int i = 0; i < num_fragments; i++) {
                    int frag_id = fragment.getFragmentId(i);
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog_proc, frag_id);
                    catalog_obj = catalog_frag.getParent();
                    if (catalog_obj != null) break;
                } // FOR
            }
            LOG.debug(String.format("%s - Queued up %s WorkFragment for partition %d and marked as %s [fragIds=%s]",
                                    this, catalog_obj, partition,
                                    (blocked ? "blocked" : "not blocked"),
                                    fragment.getFragmentIdList()));
            if (t) LOG.trace("WorkFragment Contents for txn #" + this.txn_id + ":\n" + fragment);
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
    public void addResult(int partition, int dependency_id, VoltTable result) {
        assert(result != null) :
            String.format("%s - The result for DependencyId %d from partition %d is null",
                          this, dependency_id, partition);
        this.addResult(Pair.of(partition, dependency_id), result, false);
    }

    /**
     * Store a VoltTable result that this transaction is waiting for.
     * @param key The hackish partition+dependency key
     * @param result The actual data for the result
     * @param force If false, then we will check to make sure the result isn't a duplicate
     * @param partition The partition id that generated the result
     * @param dependency_id The dependency id that this result corresponds to
     */
    private void addResult(final Pair<Integer, Integer> key, VoltTable result, final boolean force) {
        // We have to do this because this method is allowed to be called by different threads
        ReentrantLock stateLock = null;
        try {
            stateLock = this.state.lock;
        } catch (NullPointerException ex) {
            // HACK
        }
        if (stateLock == null) return;
        
        final int base_offset = hstore_site.getLocalPartitionOffset(this.base_partition);
        assert(result != null);
        assert(this.round_state[base_offset] == RoundState.INITIALIZED || this.round_state[base_offset] == RoundState.STARTED) :
            String.format("Invalid round state %s for %s at partition %d",
                          this.round_state[base_offset], this, this.base_partition);
        
        final int partition = key.getFirst().intValue();
        final int dependency_id = key.getSecond().intValue();
        
        if (d) LOG.debug(String.format("%s - Attemping to add new result for %s [numRows=%d]",
                         this, debugPartDep(partition, dependency_id), result.getRowCount()));
        
        // If the txn is still in the INITIALIZED state, then we just want to queue up the results
        // for now. They will get released when we switch to STARTED 
        // This is the only part that we need to synchonize on
        if (force == false) {
            if (this.predict_singlePartition == false) stateLock.lock();
            try {
                if (this.state == null) return;
                if (this.round_state[base_offset] == RoundState.INITIALIZED) {
                    assert(this.state.queued_results.containsKey(key) == false) : 
                        String.format("%s - Duplicate result %s",
                                      this, debugPartDep(partition, dependency_id));
                    this.state.queued_results.put(key, result);
                    if (d) LOG.debug(String.format("%s - Queued result %s until the round is started",
                                                   this, debugPartDep(partition, dependency_id)));
                    return;
                }
                if (d) {
                    LOG.debug(String.format("%s - Storing new result for key %s", this, key));
                    if (t) LOG.trace("Result stmt_ctr(key=" + key + "): " + this.state.results_dependency_stmt_ctr.get(key));
                }
            } finally {
                if (this.predict_singlePartition == false) stateLock.unlock();
            } // SYNCH
        }
            
        // Each partition+dependency_id should be unique within the Statement batch.
        // So as the results come back to us, we have to figure out which Statement it belongs to
        DependencyInfo dinfo = null;
        Queue<Integer> queue = null;
        int stmt_index;
        try {
            queue = this.state.results_dependency_stmt_ctr.get(key);
            assert(queue != null) :
                String.format("Unexpected %s in %s / %s\n%s",
                              debugPartDep(partition, dependency_id), this,
                              key, this.state.results_dependency_stmt_ctr);
            assert(queue.isEmpty() == false) :
                String.format("No more statements for %s in %s\nresults_dependency_stmt_ctr = %s",
                              debugPartDep(partition, dependency_id), this,
                              this.state.results_dependency_stmt_ctr);

            stmt_index = queue.remove().intValue();
            dinfo = this.getDependencyInfo(dependency_id);
            assert(dinfo != null) :
                String.format("Unexpected %s for %s [stmt_index=%d]\n%s",
                              debugPartDep(partition, dependency_id), this, stmt_index, result);
        } catch (NullPointerException ex) {
            // HACK: IGNORE!
        }
        if (dinfo == null) {
            // HACK: IGNORE!
            return;
        }
        dinfo.addResult(partition, result);
        
        if (this.predict_singlePartition == false) stateLock.lock();
        try {
            // If the state has disappeared, then we know that this txn is aborted
            if (this.state == null) return;
            this.state.received_ctr++;
            
            // Check whether we need to start running stuff now
            // 2011-12-31: This needs to be synchronized because they might check
            //             whether there are no more blocked tasks before we 
            //             can add to_unblock to the unblocked_tasks queue
            if (this.state.blocked_tasks.isEmpty() == false && dinfo.hasTasksReady()) {
                Collection<WorkFragment> to_unblock = dinfo.getAndReleaseBlockedWorkFragments();
                assert(to_unblock != null);
                assert(to_unblock.isEmpty() == false);
                if (d) LOG.debug(String.format("%s - Got %d WorkFragments to unblock that were waiting for DependencyId %d",
                                 this, to_unblock.size(), dinfo.getDependencyId()));
                this.state.blocked_tasks.removeAll(to_unblock);
                this.state.unblocked_tasks.addLast(to_unblock);
            }
            else if (d) {
                LOG.debug(String.format("%s - No WorkFragments to unblock after storing %s [blockedTasks=%d, hasTasksReady=%s]",
                          this, debugPartDep(partition, dependency_id),
                          this.state.blocked_tasks.size(), dinfo.hasTasksReady()));
            }
        
            if (this.state.dependency_latch != null) {    
                this.state.dependency_latch.countDown();
                    
                // HACK: If the latch is now zero, then push an EMPTY set into the unblocked queue
                // This will cause the blocked PartitionExecutor thread to wake up and realize that he's done
                if (this.state.dependency_latch.getCount() == 0) {
                    if (d) LOG.debug(String.format("%s - Pushing EMPTY_SET to PartitionExecutor because all the dependencies have arrived!",
                                     this));
                    this.state.unblocked_tasks.addLast(EMPTY_FRAGMENT_SET);
                }
                if (d) LOG.debug(String.format("%s - Setting CountDownLatch to %d",
                                               this, this.state.dependency_latch.getCount()));
            }

            this.state.still_has_tasks = this.state.blocked_tasks.isEmpty() == false ||
                                         this.state.unblocked_tasks.isEmpty() == false;
        } finally {
            if (this.predict_singlePartition == false) stateLock.unlock();
        } // SYNCH
        
        if (d) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Blocked Tasks", (this.state != null ? this.state.blocked_tasks.size() : null));
            m.put("DependencyInfo", dinfo.toString());
            m.put("hasTasksReady", dinfo.hasTasksReady());
            LOG.debug(this + " - Status Information\n" + StringUtil.formatMaps(m));
            if (t) LOG.trace(this.debug());
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
    public Map<Integer, List<VoltTable>> removeInternalDependencies(WorkFragment fragment, Map<Integer, List<VoltTable>> results) {
        if (d) LOG.debug(String.format("%s - Retrieving %d internal dependencies for %s WorkFragment:\n%s",
                         this, fragment.getInputDepIdCount(), fragment));
        
        for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
            int stmt_index = fragment.getStmtIndex(i);
            WorkFragment.InputDependency input_dep_ids = fragment.getInputDepId(i);
            
            if (t) LOG.trace(String.format("%s - Examining %d input dependencies for PlanFragment %d in %s\n%s",
                             this, fragment.getInputDepId(i).getIdsCount(), fragment.getFragmentId(i), fragment));
            for (int input_d_id : input_dep_ids.getIdsList()) {
                if (input_d_id == HStoreConstants.NULL_DEPENDENCY_ID) continue;
                
                DependencyInfo dinfo = this.getDependencyInfo(input_d_id);
                assert(dinfo != null);
                assert(dinfo.getPartitionCount() == dinfo.getResultsCount()) :
                    String.format("%s - Number of results retrieved for %s is %d " +
                                  "but we were expecting %d\n%s\n%s\n%s",
                                  this, debugStmtDep(stmt_index, input_d_id),
                                  dinfo.getResultsCount(), dinfo.getPartitionCount(),
                                  fragment.toString(),
                                  StringUtil.SINGLE_LINE, this.debug()); 
                results.put(input_d_id, dinfo.getResults());
                if (d) LOG.debug(String.format("%s - %s -> %d VoltTables",
                                 this, debugStmtDep(stmt_index, input_d_id), results.get(input_d_id).size()));
            } // FOR
        } // FOR
        return (results);
    }
    
    public List<VoltTable> getInternalDependency(int input_d_id) {
        if (d) LOG.debug(String.format("%s - Retrieving internal dependencies for Dependency %d",
                                       this, input_d_id));
        
        DependencyInfo dinfo = this.getDependencyInfo(input_d_id);
        assert(dinfo != null) :
            String.format("No DependencyInfo object for Dependency %d in %s",
                          input_d_id, this);
        assert(dinfo.isInternal()) :
            String.format("The DependencyInfo for Dependency %s in %s is not marked as internal",
                          input_d_id, this);
        assert(dinfo.getPartitionCount() == dinfo.getResultsCount()) :
                    String.format("Number of results from partitions retrieved for Dependency %s " +
                                  "is %d but we were expecting %d in %s\n%s\n%s%s", 
                                  input_d_id, dinfo.getResultsCount(), dinfo.getPartitionCount(), this,
                                  this.toString(), StringUtil.SINGLE_LINE, this.debug()); 
        return (dinfo.getResults());
    }
    
    /**
     * Figure out what partitions this transaction is done with and notify those partitions
     * that they are done
     * Returns true if the done partitions has changed for this transaction
     * @param ts
     */
    public PartitionSet calculateDonePartitions(EstimationThresholds thresholds) {
        final int ts_done_partitions_size = this.exec_donePartitions.size();
        PartitionSet new_done = null;

        EstimatorState t_state = this.getEstimatorState();
        if (t_state == null) {
            return (null);
        }
        
        if (d) LOG.debug(String.format("Checking MarkovEstimate for %s to see whether we can notify any partitions that we're done with them [round=%d]",
                         this, this.getCurrentRound(this.base_partition)));
        
        TransactionEstimate estimate = t_state.getLastEstimate();
        assert(estimate != null) : "Got back null MarkovEstimate for " + this;
        if (estimate.isValid()) {
            return (null);
        }
        
        new_done = estimate.getFinishPartitions(thresholds);
        
        if (new_done.isEmpty() == false) { 
            // Note that we can actually be done with ourself, if this txn is only going to execute queries
            // at remote partitions. But we can't actually execute anything because this partition's only 
            // execution thread is going to be blocked. So we always do this so that we're not sending a 
            // useless message
            new_done.remove(this.base_partition);
            
            // Make sure that we only tell partitions that we actually touched, otherwise they will
            // be stuck waiting for a finish request that will never come!
            Collection<Integer> ts_touched = this.exec_touchedPartitions.values();

            // Mark the txn done at this partition if the MarkovEstimate said we were done
            for (Integer p : new_done) {
                if (this.exec_donePartitions.get(p.intValue()) == false && ts_touched.contains(p)) {
                    if (t) LOG.trace(String.format("Marking partition %d as done for %s", p, this));
                    this.exec_donePartitions.set(p.intValue());
                }
            } // FOR
        }
        return (this.exec_donePartitions.cardinality() != ts_done_partitions_size ? new_done : null);
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG STUFF
    // ----------------------------------------------------------------------------

    
    @Override
    public String toStringImpl() {
        return String.format("%s #%d/%d", this.catalog_proc.getName(),
                                          this.txn_id,
                                          this.base_partition);
    }
    
    @Override
    public String debug() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        Map<String, Object> m;
        
        // Base Class Info
        maps.add(super.getDebugMap());
        
        // Predictions
        m = new LinkedHashMap<String, Object>();
        m.put("Predict Single-Partitioned", (this.predict_touchedPartitions != null ? this.isPredictSinglePartition() : "???"));
        m.put("Predict Touched Partitions", this.getPredictTouchedPartitions());
        m.put("Predict Read Only", this.isPredictReadOnly());
        m.put("Predict Abortable", this.isPredictAbortable());
        m.put("Restart Counter", this.restart_ctr);
        m.put("Deletable", this.deletable);
        m.put("Needs Restart", this.needs_restart);
        m.put("Needs CommandLog", this.log_enabled);
        m.put("Estimator State", this.predict_tState);
        maps.add(m);

        m = new LinkedHashMap<String, Object>();
        m.put("Exec Read Only", Arrays.toString(this.exec_readOnly));
        m.put("Exec Touched Partitions", this.exec_touchedPartitions.toString(30));
        
        // Actual Execution
        if (this.state != null) {
            m.put("Exec Single-Partitioned", this.isExecSinglePartition());
            m.put("Speculative Execution", this.exec_speculative);
            m.put("Dependency Ctr", this.state.dependency_ctr);
            m.put("Received Ctr", this.state.received_ctr);
            m.put("CountdownLatch", this.state.dependency_latch);
            m.put("# of Blocked Tasks", this.state.blocked_tasks.size());
            m.put("# of Statements", this.state.batch_size);
            m.put("Expected Results", this.state.results_dependency_stmt_ctr.keySet());
        }
        maps.add(m);

        // Additional Info
        m = new LinkedHashMap<String, Object>();
        m.put("Client Callback", this.client_callback);
        if (this.dtxnState != null) {
            m.put("Init Callback", this.dtxnState.init_callback);
            m.put("Prepare Callback", this.dtxnState.prepare_callback);
            m.put("Finish Callback", this.dtxnState.finish_callback);
        }
        maps.add(m);

        // Profile Times
        if (this.profiler != null) maps.add(this.profiler.debugMap());
        
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()])));
        
        if (this.state != null) {
            sb.append(StringUtil.SINGLE_LINE);
            String stmt_debug[] = new String[this.state.batch_size];
            
            VoltProcedure voltProc = state.executor.getVoltProcedure(catalog_proc.getName());
            assert(voltProc != null);
            SQLStmt stmts[] = voltProc.voltLastQueriesExecuted();
            
            // This won't work in test cases
//            assert(stmt_debug.length == stmts.length) :
//                String.format("Expected %d SQLStmts but we only got %d", stmt_debug.length, stmts.length); 
            
            for (int stmt_index = 0; stmt_index < stmt_debug.length; stmt_index++) {
                Map<Integer, DependencyInfo> s_dependencies = new HashMap<Integer, DependencyInfo>();
                for (DependencyInfo dinfo : this.state.dependencies.values()) {
                    if (dinfo.getStatementIndex() == stmt_index) s_dependencies.put(dinfo.getDependencyId(), dinfo);
                } // FOR
                
                String inner = "  Statement #" + stmt_index;
                if (stmts != null && stmt_index < stmts.length) { 
                    inner += " - " + stmts[stmt_index].getStatement().getName();
                }
                inner += "\n";
//                inner += "  Output Dependency Id: " + (this.state.output_order.contains(stmt_index) ? this.state.output_order.get(stmt_index) : "<NOT STARTED>") + "\n";
                
                inner += "  Dependency Partitions:\n";
                for (Integer dependency_id : s_dependencies.keySet()) {
                    inner += "    [" + dependency_id + "] => " + s_dependencies.get(dependency_id).getPartitions() + "\n";
                } // FOR
                
                inner += "  Dependency Results:\n";
                for (Integer dependency_id : s_dependencies.keySet()) {
                    inner += "    [" + dependency_id + "] => [";
                    String add = "";
                    for (VoltTable vt : s_dependencies.get(dependency_id).getResults()) {
                        inner += add + (vt == null ? vt : "{" + vt.getRowCount() + " tuples}");
                        add = ",";
                    }
                    inner += "]\n";
                } // FOR
                
                inner += "  Blocked WorkFragments:\n";
                boolean none = true;
                for (Integer dependency_id : s_dependencies.keySet()) {
                    DependencyInfo d = s_dependencies.get(dependency_id);
                    for (WorkFragment task : d.getBlockedWorkFragments()) {
                        if (task == null) continue;
                        inner += "    [" + dependency_id + "] => [";
                        String add = "";
                        for (int frag_id : task.getFragmentIdList()) {
                            inner += add + frag_id;
                            add = ", ";
                        } // FOR
                        inner += "] ";
                        if (d.hasTasksReady()) {
                            inner += "*READY*";
                        }
                        else if (d.hasTasksReleased()) {
                            inner += "*RELEASED*";
                        }
                        else {
                            inner += String.format("%d / %d", d.getResults().size(), d.getPartitions().size());
                        }
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

    
    protected static String debugStmtDep(int stmt_index, int dep_id) {
        return String.format("{StmtIndex:%d, DependencyId:%d}", stmt_index, dep_id);
    }
    protected static String debugPartDep(int partition, int dep_id) {
        return String.format("{Partition:%d, DependencyId:%d}", partition, dep_id);
    }
    
}
