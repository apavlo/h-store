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
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.NotImplementedException;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.callbacks.TransactionPrepareWrapperCallback;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.internal.FinishTxnMessage;
import edu.brown.hstore.internal.InitializeTxnMessage;
import edu.brown.hstore.internal.PrepareTxnMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

/**
 * @author pavlo
 */
public abstract class AbstractTransaction implements Poolable, Loggable, Comparable<AbstractTransaction> {
    private static final Logger LOG = Logger.getLogger(AbstractTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    private static boolean d = debug.get();
    
    /**
     * Internal state for the transaction
     */
    protected enum RoundState {
        INITIALIZED,
        STARTED,
        FINISHED;
    }
    
    protected final HStoreSite hstore_site;
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Catalog object of the Procedure that this transaction is currently executing
     */
    private Procedure catalog_proc;
    
    protected Long txn_id = null;
    protected long client_handle;
    protected int base_partition;
    protected Status status;
    protected SerializableException pending_error;

    /**
     * StoredProcedureInvocation Input Parameters
     * These are the parameters that are sent from the client
     */
    protected ParameterSet parameters;
    
    /**
     * Internal flag that is set to true once to tell whomever 
     * that this transaction handle can be deleted.
     */
    private AtomicBoolean deletable = new AtomicBoolean(false);
    
    // ----------------------------------------------------------------------------
    // Attached Data Structures
    // ----------------------------------------------------------------------------

    /**
     * Attached inputs for the current execution round.
     * This cannot be in the ExecutionState because we may have attached inputs for 
     * transactions that aren't running yet.
     */
    private final Map<Integer, List<VoltTable>> attached_inputs = new HashMap<Integer, List<VoltTable>>();
    
    /**
     * Attached ParameterSets for the current execution round
     * This is so that we don't have to marshal them over to different partitions on the same HStoreSite  
     */
    private ParameterSet attached_parameterSets[];
    
    /**
     * Internal state information for txns that request prefetch queries
     * This is only needed for distributed transactions
     */
    protected PrefetchState prefetch;
    
    // ----------------------------------------------------------------------------
    // INTERNAL MESSAGE WRAPPERS
    // ----------------------------------------------------------------------------
    
    private final InitializeTxnMessage init_task;
    
    private final PrepareTxnMessage prepare_task;
    
    private final FinishTxnMessage finish_task;
    
    private final WorkFragmentMessage work_task[];
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    protected final TransactionInitQueueCallback initQueue_callback;
    protected final TransactionPrepareWrapperCallback prepareWrapper_callback;
    
    // ----------------------------------------------------------------------------
    // GLOBAL PREDICTIONS FLAGS
    // ----------------------------------------------------------------------------
    
    /**
     * Whether this transaction will only touch one partition
     */
    protected boolean predict_singlePartition = false;
    
    /**
     * Whether this txn can abort
     */
    protected boolean predict_abortable = true;
    
    /**
     * Whether we predict that this txn will be read-only
     */
    protected boolean predict_readOnly = false;
    
    /**
     * EstimationState Handle
     */
    private EstimatorState predict_tState;
    
    /**
     * The set of partitions that we expected this partition to touch.
     */
    protected PartitionSet predict_touchedPartitions;
    
    // ----------------------------------------------------------------------------
    // PER PARTITION EXECUTION FLAGS
    // ----------------------------------------------------------------------------
    
    // TODO(pavlo): Document what these arrays are and how the offsets are calculated
    
    private final boolean prepared[];
    private final boolean finished[];
    protected final RoundState round_state[];
    protected final int round_ctr[];

    /**
     * The first undo token used at each local partition
     * When we abort a txn we will need to give the EE this value
     */
    private final long exec_firstUndoToken[];
    /**
     * The last undo token used at each local partition
     * When we commit a txn we will need to give the EE this value 
     */
    private final long exec_lastUndoToken[];
    /**
     * This is set to true if the transaction did some work without an undo 
     * buffer at a local partition. This is used to just check that we aren't 
     * trying to rollback changes without having used the undo log.
     */
    private final boolean exec_noUndoBuffer[];
    
    /**
     * Whether this transaction has been read-only so far on a local partition
     */
    protected final boolean exec_readOnly[];
    
    /**
     * Whether this Transaction has queued work that may need to be removed
     * from this partition
     */
    protected final boolean exec_queueWork[];
    
    /**
     * Whether this Transaction has submitted work to the EE that may need to be 
     * rolled back on a local partition
     */
    protected final boolean exec_eeWork[];
    
    /**
     * BitSets for whether this txn has read from a particular table on each
     * local partition.
     * PartitionId -> TableId 
     */
    protected final BitSet readTables[];
    
    /**
     * BitSets for whether this txn has executed a modifying query against a particular table
     * on each partition. Note that it does not actually record whether any rows were changed,
     * but just we executed a query that targeted that table.
     * PartitionId -> TableId
     */
    protected final BitSet writeTables[];
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param executor
     */
    public AbstractTransaction(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        
        int numLocalPartitions = hstore_site.getLocalPartitionIds().size();
        this.prepared = new boolean[numLocalPartitions];
        this.finished = new boolean[numLocalPartitions];
        this.round_state = new RoundState[numLocalPartitions];
        this.round_ctr = new int[numLocalPartitions];
        this.exec_readOnly = new boolean[numLocalPartitions];
        this.exec_queueWork = new boolean[numLocalPartitions];
        this.exec_eeWork = new boolean[numLocalPartitions];
        
        this.exec_firstUndoToken = new long[numLocalPartitions];
        this.exec_lastUndoToken = new long[numLocalPartitions];
        this.exec_noUndoBuffer = new boolean[numLocalPartitions];
        
        this.init_task = new InitializeTxnMessage(this);
        this.prepare_task = new PrepareTxnMessage(this);
        this.finish_task = new FinishTxnMessage(this, Status.OK);
        this.work_task = new WorkFragmentMessage[numLocalPartitions];
        
        this.initQueue_callback = new TransactionInitQueueCallback(hstore_site);
        this.prepareWrapper_callback = new TransactionPrepareWrapperCallback(hstore_site);
        
        this.readTables = new BitSet[numLocalPartitions];
        this.writeTables = new BitSet[numLocalPartitions];
        int num_tables = hstore_site.getCatalogContext().database.getTables().size();
        for (int i = 0; i < numLocalPartitions; i++) {
            this.readTables[i] = new BitSet(num_tables);
            this.writeTables[i] = new BitSet(num_tables);
        } // FOR
        
        Arrays.fill(this.exec_firstUndoToken, HStoreConstants.NULL_UNDO_LOGGING_TOKEN);
        Arrays.fill(this.exec_lastUndoToken, HStoreConstants.NULL_UNDO_LOGGING_TOKEN);
        Arrays.fill(this.exec_readOnly, true);
        Arrays.fill(this.exec_queueWork, false);
        Arrays.fill(this.exec_eeWork, false);
    }

    /**
     * Initialize this TransactionState for a new Transaction invocation
     * @param txn_id
     * @param client_handle
     * @param base_partition
     * @param parameters TODO
     * @param sysproc
     * @param predict_singlePartition
     * @param predict_readOnly
     * @param predict_abortable
     * @param exec_local
     * @return
     */
    protected final AbstractTransaction init(Long txn_id,
                                             long client_handle,
                                             int base_partition,
                                             ParameterSet parameters,
                                             Procedure catalog_proc,
                                             PartitionSet predict_touchedPartitions,
                                             boolean predict_readOnly,
                                             boolean predict_abortable,
                                             boolean exec_local) {
        assert(predict_touchedPartitions != null);
        assert(predict_touchedPartitions.isEmpty() == false);
        assert(catalog_proc != null) : "Unexpected null Procedure catalog handle";
        
        this.txn_id = txn_id;
        this.client_handle = client_handle;
        this.base_partition = base_partition;
        this.parameters = parameters;
        this.catalog_proc = catalog_proc;
        
        // Initialize the predicted execution properties for this transaction
        this.predict_touchedPartitions = predict_touchedPartitions;
        this.predict_singlePartition = (this.predict_touchedPartitions.size() == 1);
        this.predict_readOnly = predict_readOnly;
        this.predict_abortable = predict_abortable;
        
        return (this);
    }

    @Override
    public final boolean isInitialized() {
        return (this.txn_id != null && this.catalog_proc != null);
    }
    
    /**
     * Should be called once the TransactionState is finished and is
     * being returned to the pool
     */
    @Override
    public void finish() {
        this.predict_singlePartition = false;
        this.predict_abortable = true;
        this.predict_readOnly = false;
        this.predict_tState = null;
        
        this.initQueue_callback.finish();
        this.prepareWrapper_callback.finish();
        
        this.pending_error = null;
        this.status = null;
        this.parameters = null;
        this.attached_inputs.clear();
        this.attached_parameterSets = null;

        // If this transaction handle was keeping track of pre-fetched queries,
        // then go ahead and reset those state variables.
        if (this.prefetch != null) {
            hstore_site.getObjectPools()
                       .getPrefetchStatePool(this.base_partition)
                       .returnObject(this.prefetch);
            this.prefetch = null;
        }
        
        for (int i = 0; i < this.exec_readOnly.length; i++) {
            this.prepared[i] = false;
            this.finished[i] = false;
            this.round_state[i] = null;
            this.round_ctr[i] = 0;
            this.exec_readOnly[i] = true;
            this.exec_queueWork[i] = false;
            this.exec_eeWork[i] = false;
            this.exec_firstUndoToken[i] = HStoreConstants.NULL_UNDO_LOGGING_TOKEN;
            this.exec_lastUndoToken[i] = HStoreConstants.NULL_UNDO_LOGGING_TOKEN;
            this.exec_noUndoBuffer[i] = false;
            
            this.readTables[i].clear();
            this.writeTables[i].clear();
        } // FOR

        if (d) LOG.debug(String.format("Finished txn #%d and cleaned up internal state [hashCode=%d, finished=%s]",
                                       this.txn_id, this.hashCode(), Arrays.toString(this.finished)));
        
        this.deletable.lazySet(false);
        this.catalog_proc = null;
        this.base_partition = HStoreConstants.NULL_PARTITION_ID;
        this.txn_id = null;
    }
    
    @Override
    public void updateLogging() {
        d = debug.get();
//        t = trace.get();
    }

    // ----------------------------------------------------------------------------
    // DATA STORAGE
    // ----------------------------------------------------------------------------
    
    /**
     * Store data from mapOutput tables to reduceInput table
     * reduceInput table should merge all incoming data from the mapOutput tables.
     */
    public Status storeData(int partition, VoltTable vt) {
        throw new NotImplementedException("Not able to store data for non-MapReduce transactions");
    }
    
    // ----------------------------------------------------------------------------
    // ROUND METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Must be called once before one can add new WorkFragments for this txn
     * This will always clear out any pending errors 
     * @param undoToken
     */
    public void initRound(int partition, long undoToken) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.round_state[offset] == null || this.round_state[offset] == RoundState.FINISHED) : 
            String.format("Invalid state %s for ROUND #%s on partition %d for %s [hashCode=%d]",
                          this.round_state[offset], this.round_ctr[offset], partition, this, this.hashCode());
        
        // If we get to this point, then we know that nobody cares about any 
        // errors from the previous round, therefore we can just clear it out
        this.pending_error = null;
        
        if (this.exec_lastUndoToken[offset] == HStoreConstants.NULL_UNDO_LOGGING_TOKEN || 
            undoToken != HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            // LAST UNDO TOKEN
            this.exec_lastUndoToken[offset] = undoToken;
            
            // FIRST UNDO TOKEN
            if (this.exec_firstUndoToken[offset] == HStoreConstants.NULL_UNDO_LOGGING_TOKEN ||
                this.exec_firstUndoToken[offset] == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
                this.exec_firstUndoToken[offset] = undoToken;
            }
        }
        // NO UNDO LOGGING
        if (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            this.exec_noUndoBuffer[offset] = true;
        }
        this.round_state[offset] = RoundState.INITIALIZED;
        
        if (d) LOG.debug(String.format("%s - Initializing ROUND %d at partition %d [undoToken=%d / first=%d / last=%d]",
                         this, this.round_ctr[offset], partition,
                         undoToken, this.exec_firstUndoToken[offset],  this.exec_lastUndoToken[offset]));
    }
    
    /**
     * Called once all of the WorkFragments have been submitted for this txn
     * @return
     */
    public void startRound(int partition) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.round_state[offset] == RoundState.INITIALIZED) :
            String.format("Invalid state %s for ROUND #%s on partition %d for %s [hashCode=%d]",
                          this.round_state[offset], this.round_ctr[offset], partition, this, this.hashCode());
        
        this.round_state[offset] = RoundState.STARTED;
        if (d) LOG.debug(String.format("%s - Starting batch ROUND #%d on partition %d",
                         this, this.round_ctr[offset], partition));
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound(int partition) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.round_state[offset] == RoundState.STARTED) :
            String.format("Invalid batch round state %s for %s at partition %d",
                          this.round_state[offset], this, partition);
        
        if (d) LOG.debug(String.format("%s - Finishing batch ROUND #%d on partition %d",
                         this, this.round_ctr[offset], partition));
        this.round_state[offset] = RoundState.FINISHED;
        this.round_ctr[offset]++;
    }
    
    // ----------------------------------------------------------------------------
    // PREDICTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the collection of the partitions that this transaction is expected
     * to need during its execution. The transaction may choose to not use all of
     * these but it is not allowed to use more.
     */
    public PartitionSet getPredictTouchedPartitions() {
        return (this.predict_touchedPartitions);
    }
    
    /**
     * Returns true if this Transaction was originally predicted as being able to abort
     */
    public final boolean isPredictAbortable() {
        return (this.predict_abortable);
    }
    /**
     * Returns true if this transaction was originally predicted as read only
     */
    public final boolean isPredictReadOnly() {
        return (this.predict_readOnly);
    }
    /**
     * Returns true if this Transaction was originally predicted to be single-partitioned
     */
    public boolean isPredictSinglePartition() {
        return (this.predict_singlePartition);
    }
    
    public EstimatorState getEstimatorState() {
        return (this.predict_tState);
    }
    public void setEstimatorState(EstimatorState state) {
        this.predict_tState = state;
    }
    
    /**
     * Get the last TransactionEstimate produced for this transaction.
     * If there is no estimate, then the return result is null.
     * @return
     */
    public Estimate getLastEstimate() {
        if (this.predict_tState != null) {
            return (this.predict_tState.getLastEstimate());
        }
        return (null);
    }
    
    /**
     * Returns true if this transaction was executed speculatively
     */
    public boolean isSpeculative() {
        return (false);
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION FLAG METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this transaction as have performed some modification on this partition
     */
    public void markExecNotReadOnly(int partition) {
        this.exec_readOnly[hstore_site.getLocalPartitionOffset(partition)] = false;
    }

    /**
     * Returns true if this transaction has not executed any modifying work at this partition
     */
    public final boolean isExecReadOnly(int partition) {
        if (this.catalog_proc.getReadonly()) return (true);
        return (this.exec_readOnly[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
    /**
     * Returns true if this transaction executed without undo buffers at some point
     */
    public boolean isExecNoUndoBuffer(int partition) {
        return (this.exec_noUndoBuffer[hstore_site.getLocalPartitionOffset(partition)]);
    }
    public void markExecNoUndoBuffer(int partition) {
        this.exec_noUndoBuffer[hstore_site.getLocalPartitionOffset(partition)] = true;
    }
    /**
     * Returns true if this transaction's control code running at this partition 
     */
    public boolean isExecLocal(int partition) {
        return (this.base_partition == partition);
    }
    
    /**
     * Returns true if this transaction has done something at this partition and therefore
     * the PartitionExecutor needs to be told that they are finished
     * This could be either executing a query or executing the transaction's control code
     */
    public final boolean needsFinish(int partition) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        
        boolean ret = (this.exec_readOnly[offset] == false &&
                        (this.round_state[offset] != null || 
                         this.exec_eeWork[offset] ||
                         this.exec_queueWork[offset])
        );
        
//        if (this.isSpeculative()) {
//            LOG.info(String.format(
//                "%s - Speculative Execution Partition %d => %s\n" +
//                "  Round State:   %s\n" +
//                "  Exec ReadOnly: %s\n" +
//                "  Exec Queue:    %s\n" +
//                "  Exec EE:       %s\n",
//                this, partition, ret,
//                this.round_state[offset],
//                this.exec_readOnly[offset],
//                this.exec_queueWork[offset],
//                this.exec_eeWork[offset]));
//        }
        
        // This transaction needs to be "finished" down in the EE if:
        //  (1) It's not read-only
        //  (2) It has executed at least one batch round
        //  (3) It has invoked work directly down in the EE
        //  (4) It added work that may be waiting in this partition's queue
        return (ret);
    }
    
    /**
     * Returns true if we believe that this transaction can be deleted
     * <B>Note:</B> This will only return true once and only once for each 
     * transaction invocation. So don't call this unless you are able to really
     * delete the transaction now. If you just want to know whether the txn has
     * been marked as deletable before, then use checkDeletableFlag()
     * @return
     */
    public boolean isDeletable() {
        if (this.isInitialized() == false) {
            return (false);
        }
        if (this.initQueue_callback.allCallbacksFinished() == false) {
            if (d) LOG.warn(String.format("%s - %s is not finished", this,
                            this.initQueue_callback.getClass().getSimpleName()));
            return (false);
        }
        if (this.prepareWrapper_callback.allCallbacksFinished() == false) {
            if (d) LOG.warn(String.format("%s - %s is not finished", this,
                            this.prepareWrapper_callback.getClass().getSimpleName()));
            return (false);
        }
        return (this.deletable.compareAndSet(false, true));
    }
    
    /**
     * Returns true if this txn has already been marked for deletion
     * This will not change the internal state of the txn.
     */
    public final boolean checkDeletableFlag() {
        return (this.deletable.get());
    }

    
    // ----------------------------------------------------------------------------
    // GENERAL METHODS
    // ----------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractTransaction) {
            AbstractTransaction other = (AbstractTransaction)obj;
            if (other.txn_id == null) {
                if (this.txn_id == null) return (this.hashCode() != other.hashCode());
                return (false);
            }
            else if (this.txn_id == null) {
                if (other.txn_id == null) return (this.hashCode() != other.hashCode());
                return (false);
            }
            return (this.txn_id.equals(other.txn_id) && this.base_partition == other.base_partition);
        }
        return (false);
    }
    
    @Override
    public int compareTo(AbstractTransaction o) {
        if (this.txn_id == null) return (1);
        else if (o.txn_id == null) return (-1);
        return this.txn_id.compareTo(o.txn_id);
    }
    
    /**
     * Get this state's transaction id
     */
    public final Long getTransactionId() {
        return this.txn_id;
    }
    /**
     * Get this state's client_handle
     */
    public final long getClientHandle() {
        return this.client_handle;
    }
    /**
     * Get the base PartitionId where this txn's Java code is executing on
     */
    public final int getBasePartition() {
        return this.base_partition;
    }
    /**
     * Return the underlying procedure catalog object
     * @return
     */
    public Procedure getProcedure() {
        return (this.catalog_proc);
    }
    /**
     * Return the ParameterSet that contains the procedure input
     * parameters for this transaction. These are the original parameters
     * that were sent from the client for this txn.
     * This can be null for distributed transactions on remote partitions
     */
    public ParameterSet getProcedureParameters() {
        return (this.parameters);
    }
    /**
     * Returns true if this transaction is for a system procedure
     */
    public final boolean isSysProc() {
        assert(this.catalog_proc != null) :
            "Unexpected null Procedure handle for " + this;
        return this.catalog_proc.getSystemproc();
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return this handle's TransactionInitQueueCallback
     */
    public final TransactionInitQueueCallback initTransactionInitQueueCallback(RpcCallback<TransactionInitResponse> callback) {
        assert(this.isInitialized());
        assert(this.initQueue_callback.isInitialized() == false);
        this.initQueue_callback.init(this, this.predict_touchedPartitions, callback);
        return (this.initQueue_callback);
    }
    
    /**
     * Return this handle's TransactionInitQueueCallback
     */
    public final TransactionInitQueueCallback getTransactionInitQueueCallback() {
        return (this.initQueue_callback);
    }
    
    
    public final TransactionPrepareWrapperCallback getPrepareWrapperCallback() {
        return (this.prepareWrapper_callback);
    }
    
    // ----------------------------------------------------------------------------
    // ERROR METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if this transaction has a pending error
     */
    public boolean hasPendingError() {
        return (this.pending_error != null);
    }
    /**
     * Return the pending error for this transaction
     * Does not clear it.
     * @return
     */
    public SerializableException getPendingError() {
        return (this.pending_error);
    }
    public String getPendingErrorMessage() {
        return (this.pending_error != null ? this.pending_error.getMessage() : null);
    }
    
    /**
     * 
     * @param error
     */
    public synchronized void setPendingError(SerializableException error) {
        assert(error != null) : "Trying to set a null error for txn #" + this.txn_id;
        if (this.pending_error == null) {
            if (d) LOG.warn(String.format("%s - Got %s error for txn: %s",
                            this, error.getClass().getSimpleName(), error.getMessage()));
            this.pending_error = error;
        }
    }
    
    public InitializeTxnMessage getInitializeTxnMessage() {
        return (this.init_task);
    }
    public PrepareTxnMessage getPrepareTxnMessage() {
        return (this.prepare_task);
    }
    
    public FinishTxnMessage getFinishTxnMessage(Status status) {
        this.finish_task.setStatus(status);
        return (this.finish_task);
    }
    
    public WorkFragmentMessage getWorkFragmentMessage(WorkFragment fragment) {
        int offset = hstore_site.getLocalPartitionOffset(fragment.getPartitionId());
        if (this.work_task[offset] == null) {
            this.work_task[offset] = new WorkFragmentMessage(this, fragment);
        } else {
            this.work_task[offset].setFragment(fragment);
        }
        return (this.work_task[offset]);
    }
    
    /**
     * Set the current Status for this transaction
     * This is not thread-safe.
     * @param status
     */
    public final void setStatus(Status status) {
        this.status = status;
    }
    
    public final Status getStatus() {
        return (this.status);
    }
    
    /**
     * Returns true if this transaction has been aborted.
     */
    public final boolean isAborted() {
        return (this.status != null && this.status != Status.OK);
    }
    
    // ----------------------------------------------------------------------------
    // Keep track of whether this txn executed stuff at this partition's EE
    // ----------------------------------------------------------------------------
    
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public void markQueuedWork(int partition) {
        if (d) LOG.debug(String.format("%s - Marking as having queued work on partition %d [exec_queueWork=%s]",
                         this, partition, Arrays.toString(this.exec_queueWork)));
        this.exec_queueWork[hstore_site.getLocalPartitionOffset(partition)] = true;
    }
    
    /**
     * Returns true if this txn has queued work at the given partition
     * @return
     */
    public boolean hasQueuedWork(int partition) {
        return (this.exec_queueWork[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public void markExecutedWork(int partition) {
        if (d) LOG.debug(String.format("%s - Marking as having submitted to the EE on partition %d [exec_eeWork=%s]",
                         this, partition, Arrays.toString(this.exec_eeWork)));
        this.exec_eeWork[hstore_site.getLocalPartitionOffset(partition)] = true;
    }
    
    public void unmarkExecutedWork(int partition) {
        this.exec_eeWork[hstore_site.getLocalPartitionOffset(partition)] = false;
    }
    /**
     * Returns true if this txn has submitted work to the EE that needs to be rolled back
     * @return
     */
    public boolean hasExecutedWork(int partition) {
        return (this.exec_eeWork[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
    // ----------------------------------------------------------------------------
    // READ/WRITE TABLE TRACKING
    // ----------------------------------------------------------------------------
    
    public void markTableAsRead(int partition, Table catalog_tbl) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        this.readTables[offset].set(catalog_tbl.getRelativeIndex());
    }
    public void markTableIdsAsRead(int partition, int...tableIds) {
        if (tableIds != null) {
            int offset = hstore_site.getLocalPartitionOffset(partition);
            for (int id : tableIds) {
                this.readTables[offset].set(id);
            } // FOR
        }
    }
    public boolean isTableRead(int partition, Table catalog_tbl) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        return (this.readTables[offset].get(catalog_tbl.getRelativeIndex()));
    }
    
    public void markTableAsWritten(int partition, Table catalog_tbl) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        this.writeTables[offset].set(catalog_tbl.getRelativeIndex());
    }
    public void markTableIdsAsWritten(int partition, int...tableIds) {
        if (tableIds != null) {
            int offset = hstore_site.getLocalPartitionOffset(partition);
            for (int id : tableIds) {
                this.writeTables[offset].set(id);
            } // FOR
        }
    }
    public boolean isTableWritten(int partition, Table catalog_tbl) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        return (this.writeTables[offset].get(catalog_tbl.getRelativeIndex()));
    }
    
    public boolean isTableReadOrWritten(int partition, Table catalog_tbl) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        int tableId = catalog_tbl.getRelativeIndex();
        return (this.readTables[offset].get(tableId) || this.writeTables[offset].get(tableId));
    }
    
    /**
     * Clear read/write table sets
     * <B>NOTE:</B> This should only be used for testing
     */
    public final void clearReadWriteSets() {
        for (int i = 0; i < this.readTables.length; i++) {
            this.readTables[i].clear();
            this.writeTables[i].clear();
        } // FOR
    }
    
    
    // ----------------------------------------------------------------------------
    // Whether the ExecutionSite is finished with the transaction
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this txn as prepared and return the original value
     * This is a thread-safe operation
     * @param partition - The partition to mark this txn as "prepared"
     */
    public boolean markPrepared(int partition) {
        if (d) LOG.debug(String.format("%s - Marking as prepared on partition %d %s [hashCode=%d, offset=%d]",
                                       this, partition, Arrays.toString(this.prepared),
                                       this.hashCode(), hstore_site.getLocalPartitionOffset(partition)));
        boolean orig = false;
        int offset = hstore_site.getLocalPartitionOffset(partition);
        synchronized (this.prepared) {
            orig = this.prepared[offset];
            this.prepared[offset] = true;
        } // SYNCH
        return (orig == false);
    }
    /**
     * Is this TransactionState marked as prepared
     * @return
     */
    public boolean isMarkedPrepared(int partition) {
        return (this.prepared[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
    /**
     * Mark this txn as finished (and thus ready for clean-up)
     */
    public void markFinished(int partition) {
        if (d) LOG.debug(String.format("%s - Marking as finished on partition %d " +
        		                       "[finished=%s / hashCode=%d / offset=%d]",
                                       this, partition, Arrays.toString(this.finished),
                                       this.hashCode(), hstore_site.getLocalPartitionOffset(partition)));
        this.finished[hstore_site.getLocalPartitionOffset(partition)] = true;
    }
    /**
     * Is this TransactionState marked as finished
     * @return
     */
    public boolean isMarkedFinished(int partition) {
        return (this.finished[hstore_site.getLocalPartitionOffset(partition)]);
    }

    /**
     * Get the current Round that this TransactionState is in
     * Used only for testing  
     */
    protected RoundState getCurrentRoundState(int partition) {
        return (this.round_state[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
    /**
     * Get the first undo token used for this transaction
     * When we ABORT a txn we will need to give the EE this value
     */
    public long getFirstUndoToken(int partition) {
        return this.exec_firstUndoToken[hstore_site.getLocalPartitionOffset(partition)];
    }
    /**
     * Get the last undo token used for this transaction
     * When we COMMIT a txn we will need to give the EE this value
     */
    public long getLastUndoToken(int partition) {
        return this.exec_lastUndoToken[hstore_site.getLocalPartitionOffset(partition)];
    }
    
    // ----------------------------------------------------------------------------
    // ATTACHED DATA FOR NORMAL WORK FRAGMENTS
    // ----------------------------------------------------------------------------
    
    
    public void attachParameterSets(ParameterSet parameterSets[]) {
        this.attached_parameterSets = parameterSets;
    }
    
    public ParameterSet[] getAttachedParameterSets() {
        assert(this.attached_parameterSets != null) :
            String.format("The attached ParameterSets for %s is null?", this);
        return (this.attached_parameterSets);
    }
    
    public void attachInputDependency(int input_dep_id, VoltTable vt) {
        List<VoltTable> l = this.attached_inputs.get(input_dep_id);
        if (l == null) {
            l = new ArrayList<VoltTable>();
            this.attached_inputs.put(input_dep_id, l);
        }
        l.add(vt);
    }
    
    public Map<Integer, List<VoltTable>> getAttachedInputDependencies() {
        return (this.attached_inputs);
    }
    
    // ----------------------------------------------------------------------------
    // PREFETCH QUERIES
    // ----------------------------------------------------------------------------
    
    /**
     * Grab a PrefetchState object for this LocalTransaction
     * This must be called before you can send prefetch requests on behalf of this txn
     */
    public final void initializePrefetch() {
        if (this.prefetch == null) {
            try {
                this.prefetch = hstore_site.getObjectPools()
                                           .getPrefetchStatePool(this.base_partition)
                                           .borrowObject();
                this.prefetch.init(this);
            } catch (Throwable ex) {
                String message = String.format("Unexpected error when trying to initialize %s for %s",
                                               PrefetchState.class.getSimpleName(), this);
                throw new ServerFaultException(message, ex, this.txn_id);
            }
        }
    }
    
    /**
     * Returns true if this transaction has prefetched queries 
     */
    public final boolean hasPrefetchQueries() {
        // return (this.prefetch.fragments != null && this.prefetch.fragments.isEmpty() == false);
        return (this.prefetch != null);
    }
    
    /**
     * Attach prefetchable WorkFragments for this transaction
     * This should be invoked on the remote side of the initialization request.
     * That is, it is not the transaction's base partition that is storing this information,
     * it's coming from over the network
     * @param fragments
     * @param rawParameters
     */
    public void attachPrefetchQueries(List<WorkFragment> fragments, List<ByteString> rawParameters) {
        assert(this.prefetch.fragments == null) :
            "Trying to attach Prefetch WorkFragments more than once!";
        
        // Simply copy the references so we don't allocate more objects
        this.prefetch.fragments = fragments;
        this.prefetch.paramsRaw = rawParameters;
    }
    
    public void attachPrefetchParameters(ParameterSet params[]) {
        assert(this.prefetch.params == null) :
            "Trying to attach Prefetch ParameterSets more than once!";
        this.prefetch.params = params;
    }
    
    public final List<WorkFragment> getPrefetchFragments() {
        return (this.prefetch.fragments);
    }
    
    public final List<ByteString> getPrefetchRawParameterSets() {
        return (this.prefetch.paramsRaw);
    }

    public final ParameterSet[] getPrefetchParameterSets() {
        assert(this.prefetch.params != null);
        return (this.prefetch.params);
    }
    
    public final void markExecPrefetchQuery(int partition) {
        assert(this.prefetch != null);
        int offset = hstore_site.getLocalPartitionOffset(partition);
        this.prefetch.partitions.set(offset);
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Get the current round counter at the given partition.
     * Note that a round is different than a batch. A "batch" contains multiple queries
     * that the txn wants to execute, of which their PlanFragments are broken
     * up into separate execution "rounds" in the PartitionExecutor.
     */
    protected int getCurrentRound(int partition) {
        return (this.round_ctr[hstore_site.getLocalPartitionOffset(partition)]);
    }

    @Override
    public final String toString() {
        String str = null;
        if (this.isInitialized()) {
            str = this.toStringImpl();
        } else {
            str = String.format("<Uninitialized-%s>", this.getClass().getSimpleName());
        }
        // Include hashCode for debugging
        str += "/" + this.hashCode();
        return (str);
    }
    
    public abstract String toStringImpl();
    public abstract String debug();
    
    @SuppressWarnings("unchecked")
    protected Map<String, Object>[] getDebugMaps() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        Map<String, Object> m;
        
        m = new LinkedHashMap<String, Object>();
        m.put("Transaction Id", this.txn_id);
        m.put("Procedure", this.catalog_proc);
        m.put("Base Partition", this.base_partition);
        m.put("Hash Code", this.hashCode());
        m.put("Deletable", this.deletable.get());
        m.put("Current Round State", Arrays.toString(this.round_state));
        m.put("Read-Only", Arrays.toString(this.exec_readOnly));
        m.put("First UndoToken", Arrays.toString(this.exec_firstUndoToken));
        m.put("Last UndoToken", Arrays.toString(this.exec_lastUndoToken));
        m.put("No Undo Buffer", Arrays.toString(this.exec_noUndoBuffer));
        m.put("# of Rounds", Arrays.toString(this.round_ctr));
        m.put("Executed Work", Arrays.toString(this.exec_eeWork));
        if (this.pending_error != null)
            m.put("Pending Error", this.pending_error);
        maps.add(m);
        
        // Predictions
        m = new LinkedHashMap<String, Object>();
        m.put("Predict Single-Partitioned", (this.predict_touchedPartitions != null ? this.isPredictSinglePartition() : "???"));
        m.put("Predict Touched Partitions", this.getPredictTouchedPartitions());
        m.put("Predict Read Only", this.isPredictReadOnly());
        m.put("Predict Abortable", this.isPredictAbortable());
        maps.add(m);
        
        return ((Map<String, Object>[])maps.toArray(new Map[0]));
    }
    
    public static String formatTxnName(Procedure catalog_proc, Long txn_id) {
        if (catalog_proc != null) {
            return (catalog_proc.getName() + " #" + txn_id);
        }
        return ("#" + txn_id);
    }
    

}