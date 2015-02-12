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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.types.SpeculationType;
import org.voltdb.utils.EstTime;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.LocalFinishCallback;
import edu.brown.hstore.callbacks.LocalInitQueueCallback;
import edu.brown.hstore.callbacks.LocalPrepareCallback;
import edu.brown.hstore.internal.StartTxnMessage;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.TransactionProfiler;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class LocalTransaction extends AbstractTransaction {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // TRANSACTION INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
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
     * A special lock for the critical sections of the LocalTransaction
     * This is only to handle messages coming from the HStoreCoordinator or from other
     * PartitionExecutors that are executing on this txn's behalf 
     */
    private final ReentrantLock lock = new ReentrantLock();
    
    /**
     * The DependencyTracker from this txn's base partition.
     * This is just here for caching purposes. We could always just
     * get it from the HStoreSite if we really needed it.
     */
    private DependencyTracker depTracker;
    
    // ----------------------------------------------------------------------------
    // INTERNAL STATE
    // ----------------------------------------------------------------------------
    
    /**
     * Number of SQLStmts in the current batch
     */
    private int batch_size = 0;
    
    /**
     * 
     */
    private boolean needs_restart = false; // FIXME
    
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
     * Special TransactionProfiler handle
     */
    public TransactionProfiler profiler;
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------

    /**
     * This callback is used to release the transaction once we get
     * the acknowledgments back from all of the partitions that we're going to access.
     */
    protected final LocalInitQueueCallback init_callback;

    // ----------------------------------------------------------------------------
    // INTERNAL MESSAGE WRAPPERS
    // ----------------------------------------------------------------------------
    
    private StartTxnMessage start_msg;
    
    // ----------------------------------------------------------------------------
    // RUN TIME DATA MEMBERS
    // ----------------------------------------------------------------------------
    

    /** 
     * What partitions has this txn touched
     * This needs to be a Histogram so that we can figure out what partitions
     * were touched the most if end up needing to redirect it later on
     */
    private final FastIntHistogram exec_touchedPartitions;
    
    /**
     * Whether this transaction's control code was executed on
     * its base partition.
     */
    private boolean exec_controlCode = false;
    
    /**
     * This keeps track of the number of times that we have invoked 
     * each query in this transaction.
     */
    private final Histogram<Statement> exec_stmtCounters = new ObjectHistogram<Statement>();
	private Long old_transaction_id;
    
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
        this.init_callback = new LocalInitQueueCallback(hstore_site);
        int numPartitions = hstore_site.getCatalogContext().numberOfPartitions;
        this.exec_touchedPartitions = new FastIntHistogram(false, numPartitions);
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
        super.init(txn_id,
                   clientHandle,
                   base_partition,
                   params,
                   catalog_proc,
                   predict_touchedPartitions,
                   predict_readOnly,
                   predict_abortable,
                   true);
        
        this.initiateTime = initiateTime;
        this.client_callback = client_callback;
        this.init_callback.init(this, this.predict_touchedPartitions);
        this.mapreduce = catalog_proc.getMapreduce();
        
        if (this.predict_singlePartition == false || this.isSysProc() || hstore_site.getCatalogContext().numberOfPartitions == 1) {
            this.depTracker = hstore_site.getDependencyTracker(base_partition);
        }
        
        // Grab a DistributedState that will have all the goodies that we need
        // to execute a distributed transaction
        if (this.predict_singlePartition == false) {
            try {
//                if (hstore_site.getHStoreConf().site.pool_txn_enable) {
//                    this.dtxnState = hstore_site.getObjectPools()
//                                                .getDistributedStatePool(base_partition)
//                                                .borrowObject();
//                } else {
                    this.dtxnState = new DistributedState(hstore_site);
//                }
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
     * @param params - Procedure Input Parameters
     * @param predict_touchedPartitions
     * @param catalog_proc
     * @return
     */
    public LocalTransaction testInit(Long txn_id,
                                     int base_partition,
                                     ParameterSet params,
                                     PartitionSet predict_touchedPartitions,
                                     Procedure catalog_proc) {
        this.initiateTime = EstTime.currentTimeMillis();
        
        super.init(txn_id,                       // TxnId
                   Integer.MAX_VALUE,            // ClientHandle
                   base_partition,               // BasePartition
                   params,                       // Procedure Parameters
                   catalog_proc,                 // Procedure
                   predict_touchedPartitions,    // Partitions
                   catalog_proc.getReadonly(),   // ReadOnly
                   true,                         // Abortable
                   true                          // ExecLocal
        );
        if (this.predict_singlePartition == false) {
            this.dtxnState = new DistributedState(hstore_site).init(this);
        }
        if (this.predict_singlePartition == false || this.isSysProc() || hstore_site.getCatalogContext().numberOfPartitions == 1) {
            this.depTracker = hstore_site.getDependencyTracker(base_partition);
        }
        
        return (this);
        
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
                                     Object...proc_params) {
        this.client_callback = new RpcCallback<ClientResponseImpl>() {
            public void run(ClientResponseImpl parameter) {}
        };
        return this.testInit(txn_id,
                             base_partition,
                             new ParameterSet(proc_params),
                             predict_touchedPartitions, catalog_proc);
    }
    
    @Override
    public void finish() {
        if (debug.val)
            LOG.debug(String.format("%s - Invoking finish() cleanup", this));
        
        // Return our DistributedState
//        if (this.dtxnState != null && hstore_site.getHStoreConf().site.pool_txn_enable) {
//            hstore_site.getObjectPools()
//                       .getDistributedStatePool(this.base_partition)
//                       .returnObject(this.dtxnState);
//            this.dtxnState = null;
//        }
        
        super.finish();
        
        this.client_callback = null;
        this.init_callback.finish();
        this.initiateTime = 0;
        this.cresponse = null;
        
        this.exec_controlCode = false;
        this.exec_specExecType = SpeculationType.NULL;
        this.exec_touchedPartitions.clear();
        this.exec_stmtCounters.clear();
        this.predict_touchedPartitions = null;
        this.restart_ctr = 0;

        this.anticache_table = null;
        this.log_enabled = false;
        this.needs_restart = false;
        
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
    public final void resetClientResponse() {
        this.cresponse = null;
    }
    
    public final ReentrantLock getTransactionLock() {
        return (this.lock);
    }
    public final boolean hasDependencyTracker() {
        return (this.depTracker != null);
    }
    
    public void removeProcedureParameters() {
        this.parameters = null;
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    /**
     * Initialize the first round of a new batch. This should only be
     * invoked at the txn's base partition.
     * @param partition
     * @param undoToken
     * @param batchSize
     */
    public void initFirstRound(long undoToken, int batchSize) {
        if (debug.val)
            LOG.debug(String.format("%s - Initializing ROUND #%d on partition %d [undoToken=%d]", 
                      this, this.round_ctr[this.base_partition], this.base_partition, undoToken));
        
        this.batch_size = batchSize;
        if (this.depTracker != null) {
            this.depTracker.initRound(this);
        }
        super.initRound(this.base_partition, undoToken);
    }
    
    @Override
    public void initRound(int partition, long undoToken) {
        assert(partition != this.base_partition) :
            String.format("Trying to invoke %s for %s at its base partition. Use initFirstRound()",
                          ClassUtil.getCurrentMethodName(), this);
        if (debug.val)
            LOG.debug(String.format("%s - Initializing ROUND #%d on partition %d [undoToken=%d]", 
                      this, this.round_ctr[partition], partition, undoToken));
        
        super.initRound(partition, undoToken);
    }
    
    @Override
    public void startRound(int partition) {
        // SAME SITE, DIFFERENT PARTITION
        if (this.base_partition != partition) {
            super.startRound(partition);
            return;
        }
        
        // SAME SITE, SAME PARTITION
        assert(this.batch_size > 0);
        if (debug.val)
            LOG.debug(String.format("%s - Starting ROUND #%d on partition %d with %d queued Statements", 
                      this, this.round_ctr[partition],
                      partition, this.batch_size));
   
        if (this.predict_singlePartition == false) this.lock.lock();
        try {
            // HACK: If there is only one partition, then we need to always
            // update the DependencyTracker. It's a bit complicated to explain why...
            if (this.depTracker != null) {
                this.depTracker.startRound(this);
            } else {
                LOG.warn(String.format("%s - Skipping DependencyTracker.startRound()\n%s", this, this.debug()));
            }
            super.startRound(partition);
        } catch (AssertionError ex) {
            LOG.fatal("Unexpected error for " + this, ex);
            throw ex;
        } finally {
            if (this.predict_singlePartition == false) this.lock.unlock();
        } // SYNCH
    }
    
    @Override
    public void finishRound(int partition) {
        if (debug.val)
            LOG.debug(String.format("%s - Finishing ROUND #%d on partition %d", 
                      this, this.round_ctr[partition], partition));
        
        // SAME SITE, DIFFERENT PARTITION
        if (this.base_partition != partition) {
            // This doesn't need to be synchronized because we know that only our
            // thread should be calling this
            super.finishRound(partition);
            return;
        }
        
        // SAME SITE, SAME PARTITION
        if (this.predict_singlePartition == false) this.lock.lock();
        try {
            // HACK: If there is only one partition, then we need to always
            // update the DependencyTracker. It's a bit complicated to explain why...
            if (this.depTracker != null) {
                this.depTracker.finishRound(this);
            } else {
                LOG.warn(String.format("%s - Skipping DependencyTracker.finishRound()", this));
            }
            super.finishRound(partition);
        } finally {
            if (this.predict_singlePartition == false) this.lock.unlock();
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
        this.round_state[partition] = RoundState.STARTED;
        super.finishRound(partition);
        if (this.base_partition == partition) {
            if (this.depTracker != null) this.depTracker.finishRound(this);
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
        if (this.depTracker != null) this.depTracker.unblock(this);
    }
    
    @Override
    public void setPendingError(SerializableException error) {
        this.setPendingError(error, true);
    }

    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    @Override
    public LocalInitQueueCallback getInitCallback() {
        return (this.init_callback);
    }
    @SuppressWarnings("unchecked")
    @Override
    public LocalPrepareCallback getPrepareCallback() {
        assert(this.dtxnState != null) :
            "Trying to access DistributedState for non distributed txn " + this + "\n" + this.debug();
        return (this.dtxnState.prepare_callback);
    }
    @SuppressWarnings("unchecked")
    @Override
    public LocalFinishCallback getFinishCallback() {
        assert(this.dtxnState != null) :
            "Trying to access DistributedState for non distributed txn " + this;
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
    // INTERNAL MESSAGES
    // ----------------------------------------------------------------------------
    
    public final StartTxnMessage getStartTxnMessage() {
        if (this.start_msg == null) {
            this.start_msg = new StartTxnMessage(this);
        }
        return (this.start_msg);
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------

    /**
     * Mark that we have invoked this txn's control code.
     */
    public final void markControlCodeExecuted() {
        assert(this.exec_controlCode == false);
        this.exec_controlCode = true;
    }
    
    /**
     * Returns true if the control code for this LocalTransaction was actually started
     * in the PartitionExecutor
     */
    public final boolean isMarkedControlCodeExecuted() {
        return (this.exec_controlCode);
    }
    
    /**
     * Reset this txn's control code as not executed.
     */  
    public final void resetControlCodeExecuted() {
        this.exec_controlCode = false;
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
    
    @Override
    public boolean isDeletable() {
        if (this.init_callback.allCallbacksFinished() == false) {
            if (trace.val)
                LOG.warn(String.format("%s - %s is not finished", this,
                         this.init_callback.getClass().getSimpleName()));
            return (false);
        }
        if (this.dtxnState != null) {
            if (this.dtxnState.prepare_callback.allCallbacksFinished() == false) {
                if (trace.val)
                    LOG.warn(String.format("%s - %s is not finished", this,
                             this.dtxnState.prepare_callback.getClass().getSimpleName()));
                return (false);
            }
            if (this.dtxnState.finish_callback.allCallbacksFinished() == false) {
                if (trace.val)
                    LOG.warn(String.format("%s - %s is not finished", this,
                             this.dtxnState.finish_callback.getClass().getSimpleName()));
                return (false);
            }
        }
        if (this.needs_restart) {
            if (trace.val)
                LOG.warn(String.format("%s - Needs restart, can't delete now", this));
            return (false);
        }
        return (super.isDeletable());
    }


    /**
     * Get the timestamp that this LocalTransaction handle was initiated
     */
    public long getInitiateTime() {
        return (this.initiateTime);
    }
    
    public final int getCurrentBatchSize() {
        return (this.batch_size);
    }

    
    public FastIntHistogram getTouchedPartitions() {
        return (this.exec_touchedPartitions);
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
     * Returns true if this Transaction has executed only on a single-partition
     * @return
     */
    public boolean isExecSinglePartition() {
        return (this.exec_touchedPartitions.getValueCount() <= 1);
    }
    
    /**
     * Update and return the number of times that we have executed the given
     * Statement in this transaction's lifetime.
     * @param stmt
     * @return
     */
    public int updateStatementCounter(Statement stmt) {
        return ((int)this.exec_stmtCounters.put(stmt));
    }

    // ----------------------------------------------------------------------------
    // DISTRIBUTED TXN EXECUTION
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if this transaction has partitions that has notified that
     * it is done with them. Note that even though the txn has marked a partition
     * as done doesn't mean that the partition has been notified yet.  
     * @return
     */
    public boolean hasDonePartitions() {
        return (this.dtxnState.exec_donePartitions.isEmpty() == false);
    }
    
    /**
     * Get all of the partitions that have been marked done for this txn.
     * If a partition is in this set, then the DBMS has sent a message to
     * its PartitionExecutor notifying that the txn is finished with them.
     * @return
     */
    public PartitionSet getDonePartitions() {
        if (this.dtxnState != null) {
            return (this.dtxnState.exec_donePartitions);
        }
        return (null);
    }
    
    /**
     * Check whether the calling thread should initiate the finish phase of 2PC.
     * This method will return true if this is the first time that somebody has
     * asked us whether we can finish things up. Multiple invocations of this
     * method will always return false.
     * @return
     */
    public boolean shouldInvokeFinish() {
        assert(this.dtxnState != null);
        return (this.dtxnState.notified_finish.compareAndSet(false, true));
    }
    
    public ProtoRpcController getTransactionInitController(int site_id) {
        return this.dtxnState.getTransactionInitController(site_id);
    }
    public ProtoRpcController getTransactionWorkController(int site_id) {
        return this.dtxnState.getTransactionWorkController(site_id);
    }
    public ProtoRpcController getTransactionPrepareController(int site_id) {
        return this.dtxnState.getTransactionPrepareController(site_id);
    }
    public ProtoRpcController getTransactionFinishController(int site_id) {
        return this.dtxnState.getTransactionFinishController(site_id);
    }
    
    // ----------------------------------------------------------------------------
    // SPECULATIVE EXECUTION
    // ----------------------------------------------------------------------------
    
    /**
     * Set the flag that indicates whether this transaction was executed speculatively
     */
    public void setSpeculative(SpeculationType type) {
        assert(type != SpeculationType.NULL);
        assert(this.exec_specExecType == SpeculationType.NULL) :
            String.format("Trying to mark %s as speculative twice [new=%s / previous=%s]",
                          this, type, this.exec_specExecType); 
        if (debug.val)
            LOG.debug(String.format("%s - Set %s = %s", 
                      this, type.getClass().getSimpleName(), type));
        this.exec_specExecType = type;
    }

    /**
     * Returns true if this transaction was executed speculatively
     */
    @Override
    public boolean isSpeculative() {
        return (this.exec_specExecType != SpeculationType.NULL);
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
    // MAP REDUCE
    // ----------------------------------------------------------------------------
    
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
    
    // ----------------------------------------------------------------------------
    // DEBUG STUFF
    // ----------------------------------------------------------------------------
    
    public TransactionProfiler getProfiler() {
        return (this.profiler);
    }
    public void setProfiler(TransactionProfiler profiler) {
        this.profiler = profiler;
    }
    
    @Override
    public String toStringImpl() {
        return String.format("%s #%d/%d", this.getProcedure().getName(),
                                          this.txn_id,
                                          this.base_partition);
    }
    
    @Override
    public String debug() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        
        // Base Class Info
        for (Map<String, Object> m : super.getDebugMaps()) {
            maps.add(m);
        } // FOR
        
        Map<String, Object> m;
        
        // Run Time Stuff
        m = new LinkedHashMap<String, Object>();
        m.put("Status", (this.status != null ? this.status : "null"));
        m.put("Speculative Type", this.getSpeculationType());
        m.put("Exec Java", this.exec_controlCode);
        m.put("Exec Read Only", Arrays.toString(this.exec_readOnly));
        m.put("Exec Touched Partitions", this.exec_touchedPartitions.toString(30));
        m.put("Exec Single-Partitioned", this.isExecSinglePartition());
        m.put("Exec Statement Counters", this.exec_stmtCounters.toString(30));
        m.put("Restart Counter", this.restart_ctr);
        m.put("Needs Restart", this.needs_restart);
        m.put("Needs CommandLog", this.log_enabled);
        m.put("Speculative Execution", this.exec_specExecType);
        m.put("Estimator State", this.getEstimatorState());
        maps.add(m);
        
        // Dependency Information
        DependencyTracker.Debug depTrackerDebug = null;
        if (this.depTracker != null) {
            depTrackerDebug = this.depTracker.getDebugContext();
            m = depTrackerDebug.debugMap(this);
            if (m != null) maps.add(m);
        }

        // Additional Info
        m = new LinkedHashMap<String, Object>();
        m.put("Client Callback", this.client_callback);
        m.put(this.init_callback.getClass().getSimpleName(), this.init_callback);
        if (this.dtxnState != null) {
            m.put(this.dtxnState.prepare_callback.getClass().getSimpleName(), this.dtxnState.prepare_callback);
            m.put(this.dtxnState.finish_callback.getClass().getSimpleName(), this.dtxnState.finish_callback);
        }
        maps.add(m);

        // Profile Times
        if (this.profiler != null) maps.add(this.profiler.debugMap());
        
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()])));
        
        if (this.depTracker != null && depTrackerDebug.hasTransactionState(this)) {
            sb.append(StringUtil.SINGLE_LINE);
            String stmt_debug[] = new String[this.batch_size];
            
            // FIXME
            // VoltProcedure voltProc = state.executor.getVoltProcedure(this.getProcedure().getName());
            // assert(voltProc != null);
            SQLStmt stmts[] = null; // voltProc.voltLastQueriesExecuted();
            
            // This won't work in test cases
//            assert(stmt_debug.length == stmts.length) :
//                String.format("Expected %d SQLStmts but we only got %d", stmt_debug.length, stmts.length); 
            
            for (int stmt_index = 0; stmt_index < stmt_debug.length; stmt_index++) {
                Map<Integer, DependencyInfo> s_dependencies = new HashMap<Integer, DependencyInfo>();
                for (DependencyInfo dinfo : depTrackerDebug.getAllDependencies(this)) {
                    if (dinfo.getStatementCounter() == stmt_index) {
                        s_dependencies.put(dinfo.getDependencyId(), dinfo);
                    }
                } // FOR
                
                String inner = "  Statement #" + stmt_index;
                if (stmts != null && stmt_index < stmts.length) { 
                    inner += " - " + stmts[stmt_index].getStatement().getName();
                }
                inner += "\n";
//                inner += "  Output Dependency Id: " + (this.state.output_order.contains(stmt_index) ? this.state.output_order.get(stmt_index) : "<NOT STARTED>") + "\n";
                
                inner += "  Dependency Partitions:\n";
                for (Integer dependency_id : s_dependencies.keySet()) {
                    inner += "    [" + dependency_id + "] => " + s_dependencies.get(dependency_id).getExpectedPartitions() + "\n";
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
                    for (WorkFragment.Builder task : d.getBlockedWorkFragments()) {
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
                            inner += String.format("%d / %d", d.getResults().size(), d.getExpectedPartitions().size());
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

	public void setOldTransactionId(Long transactionId) {
		this.old_transaction_id = transactionId;
	}
	
	public Long getOldTransactionId(){
		return this.old_transaction_id;
	}
    
}
