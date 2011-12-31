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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FinishTaskMessage;

import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.Poolable;
import edu.mit.hstore.HStoreConstants;
import edu.mit.hstore.HStoreSite;

/**
 * @author pavlo
 */
public abstract class AbstractTransaction implements Poolable {
    private static final Logger LOG = Logger.getLogger(AbstractTransaction.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Internal state for the transaction
     */
    protected enum RoundState {
        INITIALIZED,
        STARTED,
        FINISHED;
    }
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected final HStoreSite hstore_site;
    
    protected long txn_id = -1;
    protected long client_handle;
    protected int base_partition;
    protected final Set<Integer> touched_partitions = new HashSet<Integer>();
    protected boolean rejected;
    protected SerializableException pending_error;
    private final FinishTaskMessage finish_task;
    
    // ----------------------------------------------------------------------------
    // PREDICTIONS FLAGS
    // ----------------------------------------------------------------------------
    
    protected boolean predict_singlePartition = false;
    
    /** Whether this txn can abort */
    protected boolean predict_abortable = true;
    
    /** Whether we predict that this txn will be read-only */
    protected boolean predict_readOnly = false;
    
    // ----------------------------------------------------------------------------
    // PER PARTITION EXECUTION FLAGS
    // ----------------------------------------------------------------------------
    
    private final boolean finished[];
    protected final Long last_undo_token[];
    protected final RoundState round_state[];
    protected final int round_ctr[];
    
    /** Whether this transaction has been read-only so far */
    protected final boolean exec_readOnly[];

    /** Whether this Transaction has submitted work to the EE that may need to be rolled back */
    protected final boolean exec_eeWork[];
    
    /** This is set to true if the transaction did some work without an undo buffer **/
    private final boolean exec_noUndoBuffer[];
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param executor
     */
    public AbstractTransaction(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        
        int cnt = hstore_site.getLocalPartitionIds().size();
        this.finished = new boolean[cnt];
        this.last_undo_token = new Long[cnt];
        this.round_state = new RoundState[cnt];
        this.round_ctr = new int[cnt];
        this.exec_readOnly = new boolean[cnt];
        this.exec_eeWork = new boolean[cnt];
        this.exec_noUndoBuffer = new boolean[cnt];
        
        this.finish_task = new FinishTaskMessage(this, Hstore.Status.OK);
    }

    /**
     * Initialize this TransactionState for a new Transaction invocation
     * @param txn_id
     * @param client_handle
     * @param predict_readOnly TODO
     * @param predict_abortable TODO
     * @param exec_local
     * @param dtxn_txn_id
     */
    protected final AbstractTransaction init(long txn_id, long client_handle, int base_partition,
                                             boolean predict_singlePartition, boolean predict_readOnly, boolean predict_abortable, boolean exec_local) {
        this.txn_id = txn_id;
        this.client_handle = client_handle;
        this.base_partition = base_partition;
        this.rejected = false;
        this.predict_singlePartition = predict_singlePartition;
        this.predict_readOnly = predict_readOnly;
        this.predict_abortable = predict_abortable;
        
        return (this);
    }

    @Override
    public boolean isInitialized() {
        return (this.txn_id != -1);
    }
    
    /**
     * Should be called once the TransactionState is finished and is
     * being returned to the pool
     */
    @Override
    public void finish() {
        this.predict_readOnly = false;
        this.predict_abortable = true;
        this.pending_error = null;
        this.touched_partitions.clear();
        
        for (int i = 0; i < this.exec_readOnly.length; i++) {
            this.finished[i] = false;
            this.round_state[i] = null;
            this.round_ctr[i] = 0;
            this.last_undo_token[i] = null;
            this.exec_readOnly[i] = true;
            this.exec_eeWork[i] = false;
            this.exec_noUndoBuffer[i] = false;
        } // FOR

        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " +String.format("Finished txn #%d and cleaned up internal state [hashCode=%d, finished=%s]",
                                    txn_id, this.hashCode(), Arrays.toString(this.finished)));
        this.txn_id = -1;
    }

    // ----------------------------------------------------------------------------
    // IMPLEMENTING CLASS METHODS
    // ----------------------------------------------------------------------------

    /**
     * Initialize this TransactionState
     * @param txnId
     * @param clientHandle
     * @param source_partition
     * @param predict_singlePartitioned TODO
     * @param predict_readOnly TODO
     * @param predict_abortable TODO
     */
    public abstract <T extends AbstractTransaction> T init(long txnId, long clientHandle, int source_partition, boolean predict_readOnly, boolean predict_abortable);
    
    // ----------------------------------------------------------------------------
    // DATA STORAGE
    // ----------------------------------------------------------------------------
    
    /*
     * Store data from mapOutput tables to reduceInput table
     * reduceInput table should merge all incoming data from the mapOutput tables.
     */
    public Hstore.Status storeData(int partition, VoltTable vt) {
        assert(false) : "Unimplemented!";
        
        return (Hstore.Status.OK);
    }
    
    // ----------------------------------------------------------------------------
    // ROUND METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Must be called once before one can add new FragmentTaskMessages for this txn 
     * @param undoToken
     */
    public void initRound(int partition, long undoToken) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.round_state[offset] == null || this.round_state[offset] == RoundState.FINISHED) : 
            String.format("Invalid batch round state %s for %s at partition %d [hashCode=%d]", this.round_state[offset], this, partition, this.hashCode());
        
        if (this.last_undo_token[offset] == null || undoToken != HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            this.last_undo_token[offset] = undoToken;
        }
        if (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            this.exec_noUndoBuffer[this.base_partition] = true;
        }
        this.round_state[offset] = RoundState.INITIALIZED;
//        this.pending_error = null;
        
        if (debug.get()) LOG.debug("__FILE__:__LINE__ " +String.format("Initializing new round information for %s at partition %d [undoToken=%d]", this, partition, undoToken));
    }
    
    /**
     * Called once all of the FragmentTaskMessages have been submitted for this txn
     * @return
     */
    public void startRound(int partition) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.round_state[offset] == RoundState.INITIALIZED) :
            String.format("Invalid batch round state %s for %s at partition %d", this.round_state[offset], this, partition);
        
        this.round_state[offset] = RoundState.STARTED;
        if (debug.get()) LOG.debug("__FILE__:__LINE__ " +String.format("Starting batch round #%d for %s at partition", this.round_ctr[offset], this, partition));
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound(int partition) {
        int offset = hstore_site.getLocalPartitionOffset(partition);
        assert(this.round_state[offset] == RoundState.STARTED) :
            String.format("Invalid batch round state %s for %s at partition %d", this.round_state[offset], this, partition);
        
        if (debug.get()) LOG.debug("__FILE__:__LINE__ " +String.format("Finishing batch round #%d for %s at partition", this.round_ctr[offset], this, partition));
        this.round_state[offset] = RoundState.FINISHED;
        this.round_ctr[offset]++;
    }
    
    // ----------------------------------------------------------------------------
    // PREDICTIONS
    // ----------------------------------------------------------------------------
    
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
    public boolean isExecReadOnly(int partition) {
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
    
    // ----------------------------------------------------------------------------
    // GENERAL METHODS
    // ----------------------------------------------------------------------------

    
    /**
     * Get this state's transaction id
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    /**
     * Get this state's client_handle
     */
    public long getClientHandle() {
        return this.client_handle;
    }
    /**
     * Get the base PartitionId where this txn's Java code is executing on
     */
    public int getBasePartition() {
        return base_partition;
    }
    
    public boolean isRejected() {
        return (this.rejected);
    }
    
    public void markAsRejected() {
        this.rejected = true;
    }
    
    /**
     * Returns true if this transaction has done something at this partition
     */
    public boolean hasStarted(int partition) {
        return (this.last_undo_token[hstore_site.getLocalPartitionOffset(partition)] != null);
    }
    
    /**
     * Get the current batch/round counter
     */
    public int getCurrentRound(int partition) {
        return (this.round_ctr[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
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
            if (debug.get()) LOG.debug("__FILE__:__LINE__ " +"Got error for txn #" + this.txn_id + " - " + error.getMessage());
            this.pending_error = error;
        }
    }
    
    public FinishTaskMessage getFinishTaskMessage(Hstore.Status status) {
        this.finish_task.setTxnId(this.getTransactionId());
        this.finish_task.setStatus(status);
        return (this.finish_task);
    }
    
    // ----------------------------------------------------------------------------
    // Keep track of whether this txn executed stuff at this partition's EE
    // ----------------------------------------------------------------------------
    
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public void setSubmittedEE(int partition) {
        if (debug.get()) LOG.debug("__FILE__:__LINE__ " +String.format("Marking %s as having submitted to the EE on partition %d %s",
                                                 this, partition, Arrays.toString(this.exec_eeWork)));
        this.exec_eeWork[hstore_site.getLocalPartitionOffset(partition)] = true;
    }
    
    public void unsetSubmittedEE(int partition) {
        this.exec_eeWork[hstore_site.getLocalPartitionOffset(partition)] = false;
    }
    /**
     * Returns true if this txn has submitted work to the EE that needs to be rolled back
     * @return
     */
    public boolean hasSubmittedEE(int partition) {
        return (this.exec_eeWork[hstore_site.getLocalPartitionOffset(partition)]);
    }
    
    // ----------------------------------------------------------------------------
    // Whether the ExecutionSite is finished with the transaction
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this txn as finished (and thus ready for clean-up)
     */
    public void setFinishedEE(int partition) {
        if (debug.get()) 
            LOG.debug("__FILE__:__LINE__ " +String.format("Marking %s as finished on partition %d %s [hashCode=%d, offset=%d]",
                                   this, partition, Arrays.toString(this.finished),
                                   this.hashCode(), hstore_site.getLocalPartitionOffset(partition)));
        this.finished[hstore_site.getLocalPartitionOffset(partition)] = true;
    }
    /**
     * Is this TransactionState marked as finished
     * @return
     */
    public boolean isFinishedEE(int partition) {
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
     * Get the last undo token used for this transaction
     */
    public Long getLastUndoToken(int partition) {
        return this.last_undo_token[hstore_site.getLocalPartitionOffset(partition)];
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractTransaction) {
            AbstractTransaction other = (AbstractTransaction)obj;
            if ((other.txn_id == -1) && (this.txn_id == -1)) return (this.hashCode() != other.hashCode());
            return (this.txn_id == other.txn_id && this.base_partition == other.base_partition);
        }
        return (false);
    }

    @Override
    public String toString() {
        if (this.isInitialized()) {
            return "Txn #" + this.txn_id;
        } else {
            return ("<Uninitialized>");
        }
    }
    
    public abstract String debug();
    
    protected Map<String, Object> getDebugMap() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Transaction #", this.txn_id);
        m.put("Current Round State", Arrays.toString(this.round_state));
        m.put("Read-Only", Arrays.toString(this.exec_readOnly));
        m.put("Last UndoToken", Arrays.toString(this.last_undo_token));
        m.put("# of Rounds", Arrays.toString(this.round_ctr));
        if (this.pending_error != null)
            m.put("Pending Error", this.pending_error);
        return (m);
    }
    
    public static String formatTxnName(Procedure catalog_proc, long txn_id) {
        if (catalog_proc != null) {
            return (catalog_proc.getName() + " #" + txn_id);
        }
        return ("#" + txn_id);
    }
}