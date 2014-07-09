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
import org.voltdb.types.SpeculationType;
import org.voltdb.utils.NotImplementedException;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.UnevictDataResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.PartitionCountingCallback;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.internal.FinishTxnMessage;
import edu.brown.hstore.internal.SetDistributedTxnMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * Base Transaction State
 * @author pavlo
 */
public abstract class AbstractTransaction implements Poolable, Comparable<AbstractTransaction> {
    private static final Logger LOG = Logger.getLogger(AbstractTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    /**
     * Internal state for the transaction
     */
    public enum RoundState {
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
    private boolean sysproc;
    private boolean readonly;
    private boolean allow_early_prepare = true;
    
    protected Long txn_id = null;
    protected Long last_txn_id = null; // FOR DEBUGGING
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
    // ATTACHED DATA STRUCTURES
    // ----------------------------------------------------------------------------

    /**
     * Attached inputs for the current execution round.
     * This cannot be in the ExecutionState because we may have attached inputs for 
     * transactions that aren't running yet.
     */
    private Map<Integer, List<VoltTable>> attached_inputs;
    
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
    
    private SetDistributedTxnMessage setdtxn_task;
    
    private FinishTxnMessage finish_task;
    
    private WorkFragmentMessage work_task[];
    
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
    
    private final boolean released[];
    private final boolean prepared[];
    private final boolean finished[];
    
    protected final RoundState round_state[];
    protected final int round_ctr[];
    /**
     * The number of times that this transaction has been restarted 
     */
    protected int restart_ctr = 0;
    

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
    protected final boolean readTables[][];
    
    /**
     * BitSets for whether this txn has executed a modifying query against a particular table
     * on each partition. Note that it does not actually record whether any rows were changed,
     * but just we executed a query that targeted that table.
     * PartitionId -> TableId
     */
    protected final boolean writeTables[][];
    /**
     * The table that this txn needs to merge the results for in the EE
     * before it starts executing
     */
    protected Table anticache_table = null;
    /**
     * Whether this txn was speculatively executed
     */
    protected SpeculationType exec_specExecType = SpeculationType.NULL;
    
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param executor
     */
    public AbstractTransaction(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        int numPartitions = hstore_site.getCatalogContext().numberOfPartitions;
        
        this.released = new boolean[numPartitions];
        this.prepared = new boolean[numPartitions];
        this.finished = new boolean[numPartitions];
        this.round_state = new RoundState[numPartitions];
        this.round_ctr = new int[numPartitions];
        this.exec_readOnly = new boolean[numPartitions];
        this.exec_queueWork = new boolean[numPartitions];
        this.exec_eeWork = new boolean[numPartitions];
        
        this.exec_firstUndoToken = new long[numPartitions];
        this.exec_lastUndoToken = new long[numPartitions];
        this.exec_noUndoBuffer = new boolean[numPartitions];
        
        this.readTables = new boolean[numPartitions][];
        this.writeTables = new boolean[numPartitions][];
        
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
        this.last_txn_id = txn_id;
        this.client_handle = client_handle;
        this.base_partition = base_partition;
        this.parameters = parameters;
        
        this.catalog_proc = catalog_proc;
        this.sysproc = this.catalog_proc.getSystemproc();
        this.readonly = this.catalog_proc.getReadonly();
        
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
     * <B>Note:</B> This should never be called by anything other than the TransactionInitializer
     * @param txn_id
     */
    public void setTransactionId(Long txn_id) { 
        this.txn_id = txn_id;
    }
    /**
     * Returns the speculation type (i.e., stall point) that this txn was executed at.
     * Will be null if this transaction was not speculatively executed
     * @return
     */
    public SpeculationType getSpeculationType() {
        return (this.exec_specExecType);
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
        
        this.allow_early_prepare = true;
        this.pending_error = null;
        this.status = null;
        this.parameters = null;
        if (this.attached_inputs != null) this.attached_inputs.clear();
        this.attached_parameterSets = null;

        for (int partition : this.hstore_site.getLocalPartitionIds().values()) {
            this.released[partition] = false;
            this.prepared[partition] = false;
            this.finished[partition] = false;
            this.round_state[partition] = null;
            this.round_ctr[partition] = 0;
            this.exec_readOnly[partition] = true;
            this.exec_queueWork[partition] = false;
            this.exec_eeWork[partition] = false;
            this.exec_firstUndoToken[partition] = HStoreConstants.NULL_UNDO_LOGGING_TOKEN;
            this.exec_lastUndoToken[partition] = HStoreConstants.NULL_UNDO_LOGGING_TOKEN;
            this.exec_noUndoBuffer[partition] = false;
            
            if (this.readTables[partition] != null) Arrays.fill(this.readTables[partition], false);
            if (this.writeTables[partition] != null) Arrays.fill(this.writeTables[partition], false);
        } // FOR

        if (debug.val)
            LOG.debug(String.format("Finished txn #%d and cleaned up internal state [hashCode=%d, finished=%s]",
                      this.txn_id, this.hashCode(), Arrays.toString(this.finished)));
        
        this.deletable.lazySet(false);
        this.catalog_proc = null;
        this.sysproc = false;
        this.readonly = false;
        this.base_partition = HStoreConstants.NULL_PARTITION_ID;
        this.txn_id = null;
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
        assert(this.round_state[partition] == null || this.round_state[partition] == RoundState.FINISHED) : 
            String.format("Invalid state %s for ROUND #%s on partition %d for %s [hashCode=%d]",
                          this.round_state[partition], this.round_ctr[partition],
                          partition, this, this.hashCode());
        
        // If we get to this point, then we know that nobody cares about any 
        // errors from the previous round, therefore we can just clear it out
        this.pending_error = null;
        
        if (this.exec_lastUndoToken[partition] == HStoreConstants.NULL_UNDO_LOGGING_TOKEN || 
            undoToken != HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            // LAST UNDO TOKEN
            this.exec_lastUndoToken[partition] = undoToken;
            
            // FIRST UNDO TOKEN
            if (this.exec_firstUndoToken[partition] == HStoreConstants.NULL_UNDO_LOGGING_TOKEN ||
                this.exec_firstUndoToken[partition] == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
                this.exec_firstUndoToken[partition] = undoToken;
            }
        }
        // NO UNDO LOGGING
        if (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            this.exec_noUndoBuffer[partition] = true;
        }
        this.round_state[partition] = RoundState.INITIALIZED;
        
        if (debug.val)
            LOG.debug(String.format("%s - Initializing ROUND %d at partition %d [undoToken=%d / first=%d / last=%d]",
                      this, this.round_ctr[partition], partition,
                      undoToken, this.exec_firstUndoToken[partition], 
                      this.exec_lastUndoToken[partition]));
    }
    
    /**
     * Called once all of the WorkFragments have been submitted for this txn
     * @return
     */
    public void startRound(int partition) {
        assert(this.round_state[partition] == RoundState.INITIALIZED) :
            String.format("Invalid state %s for ROUND #%s on partition %d for %s [hashCode=%d]",
                          this.round_state[partition], this.round_ctr[partition],
                          partition, this, this.hashCode());
        
        this.round_state[partition] = RoundState.STARTED;
        if (debug.val)
            LOG.debug(String.format("%s - Starting batch ROUND #%d on partition %d",
                      this, this.round_ctr[partition], partition));
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound(int partition) {
        assert(this.round_state[partition] == RoundState.STARTED) :
            String.format("Invalid batch round state %s for %s at partition %d",
                          this.round_state[partition], this, partition);
        
        if (debug.val)
            LOG.debug(String.format("%s - Finishing batch ROUND #%d on partition %d",
                      this, this.round_ctr[partition], partition));
        this.round_state[partition] = RoundState.FINISHED;
        this.round_ctr[partition]++;
    }
    
    // ----------------------------------------------------------------------------
    // PREDICTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the collection of the partitions that this transaction is expected
     * to need during its execution. The transaction may choose to not use all of
     * these but it is not allowed to use more.
     */
    public final PartitionSet getPredictTouchedPartitions() {
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
    public final boolean isPredictSinglePartition() {
        return (this.predict_singlePartition);
    }
    /**
     * Returns the EstimatorState for this txn
     * @return
     */
    public EstimatorState getEstimatorState() {
        return (this.predict_tState);
    }
    /**
     * Set the EstimatorState for this txn
     * @param state
     */
    public final void setEstimatorState(EstimatorState state) {
        assert(this.predict_tState == null) :
            String.format("Trying to set the %s for %s twice",
                          EstimatorState.class.getSimpleName(), this);
        if (debug.val)
            LOG.debug(String.format("%s - Setting %s for txn",
                      this, state.getClass().getSimpleName()));
        this.predict_tState = state;
    }
    /**
     * Get the last TransactionEstimate produced for this transaction.
     * If there is no estimate, then the return result is null.
     * @return
     */
    public final Estimate getLastEstimate() {
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
    public final void markExecNotReadOnly(int partition) {
        assert(this.sysproc == true || this.readonly == false);
        this.exec_readOnly[partition] = false;
    }

    /**
     * Returns true if this transaction has not executed any modifying work at this partition
     */
    public final boolean isExecReadOnly(int partition) {
        if (this.readonly) return (true);
        return (this.exec_readOnly[partition]);
    }
    
    /**
     * Returns true if this transaction executed without undo buffers at some point
     */
    public final boolean isExecNoUndoBuffer(int partition) {
        return (this.exec_noUndoBuffer[partition]);
    }
    /**
     * Mark that the transaction executed queries without using undo logging
     * at the given partition.
     * @param partition
     */
    public final void markExecNoUndoBuffer(int partition) {
        this.exec_noUndoBuffer[partition] = true;
    }
    /**
     * Returns true if this transaction's control code running at this partition 
     */
    public final boolean isExecLocal(int partition) {
        return (this.base_partition == partition);
    }
    
    /**
     * Returns true if this transaction has done something at this partition and therefore
     * the PartitionExecutor needs to be told that they are finished.
     * This could be either executing a query or executing the transaction's control code
     */
    public final boolean needsFinish(int partition) {
        boolean ret = (this.exec_readOnly[partition] == false &&
                        (this.round_state[partition] != null || 
                         this.exec_eeWork[partition] ||
                         this.exec_queueWork[partition])
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
//        if (this.isInitialized() == false) {
//            return (false);
//        }
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
    public final boolean equals(Object obj) {
        if (obj instanceof AbstractTransaction) {
            AbstractTransaction other = (AbstractTransaction)obj;
            if (this.txn_id == null) {
                return (this.hashCode() != other.hashCode());
            }
            return (this.txn_id.equals(other.txn_id));
        }
        return (false);
    }
    
    @Override
    public final int compareTo(AbstractTransaction o) {
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
    public final Procedure getProcedure() {
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
        return (this.sysproc);
    }
    
    public final boolean allowEarlyPrepare() {
        return (this.allow_early_prepare);
    }
    
    public final void setAllowEarlyPrepare(boolean enable) {
        this.allow_early_prepare = enable;
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return this handle's InitCallback
     */
    public abstract <T extends PartitionCountingCallback<? extends AbstractTransaction>> T getInitCallback();
    
    /**
     * Return this handle's PrepareCallback
     */
    public abstract <T extends PartitionCountingCallback<? extends AbstractTransaction>> T getPrepareCallback();
    
    /**
     * Return this handle's FinishCallback
     */
    public abstract <T extends PartitionCountingCallback<? extends AbstractTransaction>> T getFinishCallback();

    // ----------------------------------------------------------------------------
    // ERROR METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if this transaction has a pending error
     */
    public final boolean hasPendingError() {
        return (this.pending_error != null);
    }
    /**
     * Return the pending error for this transaction.
     * This does not clear it.
     */
    public final SerializableException getPendingError() {
        return (this.pending_error);
    }
    /**
     * Return the message for the pending error of this transaction.
     */
    public final String getPendingErrorMessage() {
        return (this.pending_error != null ? this.pending_error.getMessage() : null);
    }
    
    /**
     * Set the Exception that is causing this txn to abort. It will need
     * to be processed later on.
     * This is a thread-safe operation. Only the first error will be stored.
     * @param error
     */
    public synchronized void setPendingError(SerializableException error) {
        assert(error != null) : "Trying to set a null error for txn #" + this.txn_id;
        if (this.pending_error == null) {
            if (debug.val)
                LOG.warn(String.format("%s - Got %s error for txn: %s",
                         this, error.getClass().getSimpleName(), error.getMessage()));
            this.pending_error = error;
        }
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL MESSAGE WRAPPERS
    // ----------------------------------------------------------------------------
    
    public final SetDistributedTxnMessage getSetDistributedTxnMessage() {
        if (this.setdtxn_task == null) {
            this.setdtxn_task = new SetDistributedTxnMessage(this);
        }
        return (this.setdtxn_task);
    }
    public final FinishTxnMessage getFinishTxnMessage(Status status) {
        if (this.finish_task == null) {
            synchronized (this) {
                if (this.finish_task == null) {
                    this.finish_task = new FinishTxnMessage(this, status);            
                }
            } // SYNCH
        }
        this.finish_task.setStatus(status);
        return (this.finish_task);
    }
    public final WorkFragmentMessage getWorkFragmentMessage(WorkFragment fragment) {
        if (this.work_task == null) {
            this.work_task = new WorkFragmentMessage[hstore_site.getCatalogContext().numberOfPartitions];
        }
        int partition = fragment.getPartitionId();
        if (this.work_task[partition] == null) {
            this.work_task[partition] = new WorkFragmentMessage(this, fragment);
        } else {
            this.work_task[partition].setFragment(fragment);
        }
        return (this.work_task[partition]);
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
    // TRANSACTION STATE BOOKKEEPING
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this txn as being released from the lock queue at the given partition.
     * This means that this txn was released to that partition and will
     * need to be removed with a FinishTxnMessage
     * @param partition - The partition to mark this txn as "released"
     */
    public final void markReleased(int partition) {
        if (debug.val)
            LOG.debug(String.format("%s - Marking as released on partition %d %s [hashCode=%d]",
                      this, partition, Arrays.toString(this.released), this.hashCode()));
//        assert(this.released[partition] == false) :
//            String.format("Trying to mark %s as released to partition %d twice", this, partition);
        this.released[partition] = true;
    }
    /**
     * Is this TransactionState marked as released at the given partition
     * @return
     */
    public final boolean isMarkedReleased(int partition) {
        return (this.released[partition]);
    }
    
    /**
     * Mark this txn as prepared and return the original value
     * This is a thread-safe operation
     * @param partition - The partition to mark this txn as "prepared"
     */
    public final boolean markPrepared(int partition) {
        if (debug.val)
            LOG.debug(String.format("%s - Marking as prepared on partition %d %s [hashCode=%d, offset=%d]",
                      this, partition, Arrays.toString(this.prepared),
                      this.hashCode(), partition));
        boolean orig = false;
        synchronized (this.prepared) {
            orig = this.prepared[partition];
            this.prepared[partition] = true;
        } // SYNCH
        return (orig == false);
    }
    /**
     * Is this TransactionState marked as prepared at the given partition
     * @return
     */
    public final boolean isMarkedPrepared(int partition) {
        return (this.prepared[partition]);
    }
    
    /**
     * Mark this txn as finished (and thus ready for clean-up)
     */
    public final void markFinished(int partition) {
        if (debug.val)
            LOG.debug(String.format("%s - Marking as finished on partition %d " +
                      "[finished=%s / hashCode=%d / offset=%d]",
                      this, partition, Arrays.toString(this.finished),
                      this.hashCode(), partition));
        this.finished[partition] = true;
    }
    /**
     * Is this TransactionState marked as finished
     * @return
     */
    public final boolean isMarkedFinished(int partition) {
        return (this.finished[partition]);
    }
    
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public final void markQueuedWork(int partition) {
        if (debug.val) LOG.debug(String.format("%s - Marking as having queued work on partition %d [exec_queueWork=%s]",
                                 this, partition, Arrays.toString(this.exec_queueWork)));
        this.exec_queueWork[partition] = true;
    }
    
    /**
     * Returns true if this txn has queued work at the given partition
     * @return
     */
    public final boolean hasQueuedWork(int partition) {
        return (this.exec_queueWork[partition]);
    }
    
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public final void markExecutedWork(int partition) {
        if (debug.val) LOG.debug(String.format("%s - Marking as having submitted to the EE on partition %d [exec_eeWork=%s]",
                                 this, partition, Arrays.toString(this.exec_eeWork)));
        this.exec_eeWork[partition] = true;
    }
    
    /**
     * Returns true if this txn has submitted work to the EE that needs to be rolled back
     * @return
     */
    public final boolean hasExecutedWork(int partition) {
        return (this.exec_eeWork[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // READ/WRITE TABLE TRACKING
    // ----------------------------------------------------------------------------
    
    private final int[] getMarkedTableIds(boolean bitmap[]) {
        int cnt = 0;
        if (bitmap != null) {
            for (int i = 0; i < bitmap.length; i++) {
                if (bitmap[i]) cnt++;
            } // FOR
        }
        int ret[] = new int[cnt];
        if (bitmap != null) {
            cnt = 0;
            for (int i = 0; i < bitmap.length; i++) {
                if (bitmap[i]) {
                    ret[cnt++] = i;
                }
            } // FOR
        }
        return (ret);
    }
    
    /**
     * Return an array of the tableIds that are marked as read by the txn at 
     * the given partition id.
     * @param partition
     * @return
     */
    public final int[] getTableIdsMarkedRead(int partition) {
        return (this.getMarkedTableIds(this.readTables[partition]));
    }
    /**
     * Mark that this txn read from the Table at the given partition 
     * @param partition
     * @param catalog_tbl
     */
    public final void markTableRead(int partition, Table catalog_tbl) {
        if (this.readTables[partition] == null) {
            this.readTables[partition] = new boolean[hstore_site.getCatalogContext().numberOfTables + 1];
        }
        this.readTables[partition][catalog_tbl.getRelativeIndex()] = true;
    }
    /**
     * Mark that this txn read from the tableIds at the given partition
     * @param partition
     * @param tableIds
     */
    public final void markTableIdsRead(int partition, int...tableIds) {
        if (this.readTables[partition] == null) {
            this.readTables[partition] = new boolean[hstore_site.getCatalogContext().numberOfTables + 1];
        }
        for (int id : tableIds) {
            this.readTables[partition][id] = true;
        } // FOR
    }
    /**
     * Returns true if this txn has executed a query that read from the Table
     * at the given partition.
     * @param partition
     * @param catalog_tbl
     * @return
     */
    public final boolean isTableRead(int partition, Table catalog_tbl) {
        if (this.readTables[partition] != null) {
            return (this.readTables[partition][catalog_tbl.getRelativeIndex()]);
        }
        return (false);
    }
    
    /**
     * Return an array of the tableIds that are marked as written by the txn at 
     * the given partition id.
     * @param partition
     * @return
     */
    public final int[] getTableIdsMarkedWritten(int partition) {
        return (this.getMarkedTableIds(this.writeTables[partition]));
    }
    /**
     * Mark that this txn has executed a modifying query for the Table at the given partition.
     * <B>Note:</B> This is just tracking that we executed a query.
     * It does not necessarily mean that the query actually changed anything. 
     * @param partition
     * @param catalog_tbl
     */
    public final void markTableWritten(int partition, Table catalog_tbl) {
        if (this.writeTables[partition] == null) {
            this.writeTables[partition] = new boolean[hstore_site.getCatalogContext().numberOfTables + 1];
        }
        this.writeTables[partition][catalog_tbl.getRelativeIndex()] = true;
    }
    /**
     * Mark that this txn has executed a modifying query for the tableIds at the given partition.
     * <B>Note:</B> This is just tracking that we executed a query.
     * It does not necessarily mean that the query actually changed anything.
     * @param partition
     * @param tableIds
     */
    public final void markTableIdsWritten(int partition, int...tableIds) {
        if (this.writeTables[partition] == null) {
            this.writeTables[partition] = new boolean[hstore_site.getCatalogContext().numberOfTables + 1];
        }
        for (int id : tableIds) {
            this.writeTables[partition][id] = true;
        } // FOR
    }
    /**
     * Returns true if this txn has executed a non-readonly query that read from 
     * the Table at the given partition.
     * @param partition
     * @param catalog_tbl
     * @return
     */
    public final boolean isTableWritten(int partition, Table catalog_tbl) {
        if (this.writeTables[partition] != null) {
            return (this.writeTables[partition][catalog_tbl.getRelativeIndex()]);
        }
        return (false);
    }
    
    /**
     * Returns true if this txn executed at query that either accessed or modified
     * the Table at the given partition.
     * @param partition
     * @param catalog_tbl
     * @return
     */
    public final boolean isTableReadOrWritten(int partition, Table catalog_tbl) {
        int tableId = catalog_tbl.getRelativeIndex();
        if (this.readTables[partition] != null && this.readTables[partition][tableId]) {
            return (true);
        }
        if (this.writeTables[partition] != null && this.writeTables[partition][tableId]) {
            return (true);
        }
        return (false);
    }
    
    // ----------------------------------------------------------------------------
    // GLOBAL STATE TRACKING
    // ----------------------------------------------------------------------------
    

    /**
     * Get the current Round that this TransactionState is in
     * Used only for testing  
     */
    protected RoundState getCurrentRoundState(int partition) {
        return (this.round_state[partition]);
    }
    
    /**
     * Get the first undo token used for this transaction
     * When we ABORT a txn we will need to give the EE this value
     */
    public long getFirstUndoToken(int partition) {
        return this.exec_firstUndoToken[partition];
    }
    /**
     * Get the last undo token used for this transaction
     * When we COMMIT a txn we will need to give the EE this value
     */
    public long getLastUndoToken(int partition) {
        return this.exec_lastUndoToken[partition];
    }
    
    // ----------------------------------------------------------------------------
    // ATTACHED DATA FOR NORMAL WORK FRAGMENTS
    // ----------------------------------------------------------------------------
    
    
    public final void attachParameterSets(ParameterSet parameterSets[]) {
        this.attached_parameterSets = parameterSets;
    }
    
    public final ParameterSet[] getAttachedParameterSets() {
        assert(this.attached_parameterSets != null) :
            String.format("The attached ParameterSets for %s is null?", this);
        return (this.attached_parameterSets);
    }
    
    public final void attachInputDependency(int input_dep_id, VoltTable vt) {
        if (this.attached_inputs == null) {
            this.attached_inputs = new HashMap<Integer, List<VoltTable>>();
        }
        List<VoltTable> l = this.attached_inputs.get(input_dep_id);
        if (l == null) {
            l = new ArrayList<VoltTable>();
            this.attached_inputs.put(input_dep_id, l);
        }
        l.add(vt);
    }
    
    public final Map<Integer, List<VoltTable>> getAttachedInputDependencies() {
        if (this.attached_inputs == null) {
            this.attached_inputs = new HashMap<Integer, List<VoltTable>>();
        }
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
            this.prefetch = new PrefetchState(this.hstore_site);
            this.prefetch.init(this);
        }
    }
    
    public final PrefetchState getPrefetchState() {
        return (this.prefetch);
    }
    
    /**
     * Returns true if this transaction has prefetched queries 
     */
    public final boolean hasPrefetchQueries() {
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
    
    public boolean hasPrefetchParameters() {
        return (this.prefetch != null && this.prefetch.params != null);
    }
    
    public void attachPrefetchParameters(ParameterSet params[]) {
        assert(this.prefetch.params == null) :
            String.format("Trying to attach Prefetch %s more than once for %s",
                          ParameterSet.class.getSimpleName(), this);
        this.prefetch.params = params;
    }
    
    public final boolean hasPrefetchFragments() {
        return (this.prefetch != null && this.prefetch.fragments != null);
    }
    
    public final List<WorkFragment> getPrefetchFragments() {
        return (this.prefetch.fragments);
    }
    
    /**
     * 
     * @return
     */
    public final List<ByteString> getPrefetchRawParameterSets() {
        return (this.prefetch.paramsRaw);
    }

    /**
     * Retrieve the ParameterSets for the prefetch queries.
     * There should be one ParameterSet array per site, which means that this
     * is shared across all partitions.
     * @return
     */
    public final ParameterSet[] getPrefetchParameterSets() {
        assert(this.prefetch.params != null);
        return (this.prefetch.params);
    }
    
    /**
     * Mark this txn as having executed a prefetched query at the given partition
     * @param partition
     */
    public final void markExecPrefetchQuery(int partition) {
        assert(this.prefetch != null);
        this.prefetch.partitions.add(partition);
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
        return (this.round_ctr[partition]);
    }
    
    @Override
    public final String toString() {
        String str = null;
        if (this.isInitialized()) {
            str = this.toStringImpl();
        } else {
            str = String.format("<Uninitialized-%s>", this.getClass().getSimpleName());
            if (this.last_txn_id != null) str += String.format(" {LAST:%d}", this.last_txn_id);
        }
        // Include hashCode for debugging
        // str += "/" + this.hashCode();
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
        m.put("Pending Error", this.pending_error);
        m.put("Allow Early Prepare", this.allow_early_prepare);
        maps.add(m);
        
        // Predictions
        m = new LinkedHashMap<String, Object>();
        m.put("Predict Single-Partitioned", this.predict_singlePartition);
        m.put("Predict Touched Partitions", this.getPredictTouchedPartitions());
        m.put("Predict Read Only", this.isPredictReadOnly());
        m.put("Predict Abortable", this.isPredictAbortable());
        maps.add(m);
        
        // Global State
        m = new LinkedHashMap<String, Object>();
        m.put("Marked Released", PartitionSet.toString(this.released));
        m.put("Marked Prepared", PartitionSet.toString(this.prepared));
        m.put("Marked Finished", PartitionSet.toString(this.finished));
        m.put("Marked Deletable", this.checkDeletableFlag());
        maps.add(m);
        
        // Prefetch State
        if (this.prefetch != null) {
            m = new LinkedHashMap<String, Object>();
            m.put("Prefetch Partitions", this.prefetch.partitions);
            m.put("Prefetch Fragments", StringUtil.join("\n", this.prefetch.fragments));
            m.put("Prefetch Parameters", StringUtil.join("\n", this.prefetch.params));
            m.put("Prefetch Raw Parameters", StringUtil.join("\n", this.prefetch.paramsRaw));
            maps.add(m);
        }

        // Partition Execution State
        m = new LinkedHashMap<String, Object>();
        m.put("Current Round State", Arrays.toString(this.round_state));
        m.put("Exec Read-Only", PartitionSet.toString(this.exec_readOnly));
        m.put("First UndoToken", Arrays.toString(this.exec_firstUndoToken));
        m.put("Last UndoToken", Arrays.toString(this.exec_lastUndoToken));
        m.put("No Undo Buffer", PartitionSet.toString(this.exec_noUndoBuffer));
        m.put("# of Rounds", Arrays.toString(this.round_ctr));
        m.put("Executed Work", PartitionSet.toString(this.exec_eeWork));
        maps.add(m);
        
        return ((Map<String, Object>[])maps.toArray(new Map[0]));
    }
    
    public class Debug implements DebugContext {
        /**
         * Clear read/write table sets
         * <B>NOTE:</B> This should only be used for testing
         */
        public final void clearReadWriteSets() {
            for (int i = 0; i < readTables.length; i++) {
                if (readTables[i] != null) Arrays.fill(readTables[i], false);
                if (writeTables[i] != null) Arrays.fill(writeTables[i], false);
            } // FOR
        }
    }

    private Debug cachedDebugContext;
	private RpcCallback<UnevictDataResponse> unevict_callback;
	private long new_transaction_id;
    public Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new Debug();
        }
        return this.cachedDebugContext;
    }

	public void setUnevictCallback(RpcCallback<UnevictDataResponse> done) {
		this.unevict_callback = done;
	}
	public RpcCallback<UnevictDataResponse> getUnevictCallback() {
		return this.unevict_callback;
	}

	public void setNewTransactionId(long newTransactionId) {
		this.new_transaction_id = newTransactionId;
	}
	
	public long getNewTransactionId(){
		return this.new_transaction_id;
	}

}