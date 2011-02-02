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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentTaskMessage;

import com.google.protobuf.RpcCallback;

import edu.brown.utils.Poolable;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.mit.dtxn.Dtxn;

/**
 * @author pavlo
 */
public abstract class TransactionState implements Poolable {
    protected static final Logger LOG = Logger.getLogger(TransactionState.class);
    private final static AtomicBoolean debug = new AtomicBoolean(LOG.isDebugEnabled());
    private final static AtomicBoolean trace = new AtomicBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected enum RoundState {
        NULL,
        INITIALIZED,
        STARTED,
        FINISHED;
    }

    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /** The ExecutionSite that this TransactionState is tied to **/
    protected final ExecutionSite executor;
    
    public final Map<Integer, List<VoltTable>> ee_dependencies = new HashMap<Integer, List<VoltTable>>();
    
    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected long txn_id;
    protected long client_handle;
    protected int source_partition;
    protected final Set<Integer> touched_partitions = new HashSet<Integer>();
    protected boolean exec_local;
    protected boolean single_partitioned;
    protected Long last_undo_token;
    protected RoundState round_state;
    protected int round_ctr = 0;
    protected RuntimeException pending_error;
    protected Long finished_timestamp;

    /**
     * Whether this Transaction has submitted work to the EE that needs to be rolled back
     */
    protected boolean submitted_to_ee = false;

    /**
     * Callbacks for specific FragmentTaskMessages
     * We have to keep these separate because the txn may request to execute a bunch of tasks that also go to this partition
     */
    protected final HashMap<FragmentTaskMessage, RpcCallback<Dtxn.FragmentResponse>> fragment_callbacks = new HashMap<FragmentTaskMessage, RpcCallback<Dtxn.FragmentResponse>>(); 

    /**
     * PartitionDependencyKey
     */
//    protected class PartitionDependencyKey extends Pair<Integer, Integer> {
//        public PartitionDependencyKey(Integer partition_id, Integer dependency_id) {
//            super(partition_id, dependency_id);
//        }
//    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param executor
     */
    public TransactionState(ExecutionSite executor) {
        this.executor = executor;
    }

    /**
     * Initialize this TransactionState for a new Transaction invocation
     * @param txn_id
     * @param dtxn_txn_id
     * @param client_handle
     * @param exec_local
     */
    protected final TransactionState init(long txn_id, long client_handle, int source_partition, boolean exec_local) {
        this.txn_id = txn_id;
        this.client_handle = client_handle;
        this.source_partition = source_partition;
        this.exec_local = exec_local;
        this.round_state = RoundState.NULL;
        return (this);
    }

    /**
     * Should be called once the TransactionState is finished and is
     * being returned to the pool
     */
    @Override
    public void finish() {
        this.touched_partitions.clear();
        this.fragment_callbacks.clear();
    }

    // ----------------------------------------------------------------------------
    // IMPLEMENTING CLASS METHODS
    // ----------------------------------------------------------------------------

    /**
     * Initialize this TransactionState
     * @param txnId
     * @param clientHandle
     * @param source_partition
     */
    public abstract <T extends TransactionState> T init(long txnId, long clientHandle, int source_partition);
    
    public abstract VoltTable[] getResults();
    
    public abstract void addResponse(int partition, int dependency_id);
    
    public abstract void addResult(int partition, int dependency_id, VoltTable result);

    public abstract void addFinishedBatchPlan(BatchPlanner.BatchPlan plan);
    
    // ----------------------------------------------------------------------------
    // ROUND METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Must be called once before one can add new FragmentTaskMessages for this txn 
     * @param undoToken
     */
    public void initRound(long undoToken) {
        assert(this.round_state == RoundState.NULL || this.round_state == RoundState.FINISHED) : 
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        this.last_undo_token = undoToken;
        this.round_state = RoundState.INITIALIZED;
        this.pending_error = null;
        
        if (debug.get()) LOG.debug(String.format("Initializing new round information for %s local txn #%d [undoToken=%d]",
                                                 (this.exec_local ? "" : "non-"), this.txn_id, undoToken));
    }
    
    /**
     * Called once all of the FragmentTaskMessages have been submitted for this txn
     * @return
     */
    public void startRound() {
        assert(this.round_state == RoundState.INITIALIZED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        this.round_state = RoundState.STARTED;
        
        if (debug.get()) LOG.debug("Starting round for local txn #" + this.txn_id);
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound() {
        assert(this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        this.round_state = RoundState.FINISHED;
        this.round_ctr++;
    }
    
    // ----------------------------------------------------------------------------
    // GENERAL METHODS
    // ----------------------------------------------------------------------------

    /**
     * 
     */
    public boolean hasPendingError() {
        return (this.pending_error != null);
    }
    /**
     * 
     * @return
     */
    public RuntimeException getPendingError() {
        return (this.pending_error);
    }
    /**
     * 
     * @param error
     */
    public synchronized void setPendingError(RuntimeException error) {
        assert(error != null) : "Trying to set a null error for txn #" + this.txn_id;
        if (this.pending_error == null) {
            if (debug.get()) LOG.debug("Got error for txn #" + this.txn_id + ". Aborting...");
            this.pending_error = error;
        }
    }
    /**
     * Should be called whenever the txn submits work to the EE 
     */
    public void setSubmittedEE() {
        this.submitted_to_ee = true;
    }
    /**
     * Returns true if this txn has submitted work to the EE that needs to be rolled back
     * @return
     */
    public boolean hasSubmittedEE() {
        return (this.submitted_to_ee);
    }
    /**
     * Mark this txn as finished (and thus ready for clean-up)
     */
    public void markAsFinished() {
        if (this.finished_timestamp == null) {
            this.finished_timestamp = System.currentTimeMillis();
        }
    }
    /**
     * Is this TransactionState marked as finished
     * @return
     */
    public boolean isMarkedFinished() {
        return (this.finished_timestamp != null);
    }
    /**
     * 
     * @return
     */
    public long getFinishedTimestamp() {
        assert(this.finished_timestamp != null);
        return (this.finished_timestamp);
    }
    /**
     * Get this state's transaction id
     * @return
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    /**
     * Get the current Round that this TransactionState is in
     * Used only for testing  
     * @return
     */
    protected RoundState getCurrentRoundState() {
        return (this.round_state);
    }
    /**
     * 
     * @return
     */
    public Long getLastUndoToken() {
        return this.last_undo_token;
    }
    
    /**
     * @return the client_handle
     */
    public long getClientHandle() {
        return this.client_handle;
    }
    
    /**
     * Is this transaction's control code running at this partition? 
     * @return the exec_local
     */
    public boolean isExecLocal() {
        return this.exec_local;
    }

    /**
     * Return the previously stored callback for a FragmentTaskMessage
     * @param ftask
     * @return
     */
    public RpcCallback<Dtxn.FragmentResponse> getFragmentTaskCallback(FragmentTaskMessage ftask) {
        return (this.fragment_callbacks.get(ftask));
    }
    
    /**
     * Store a callback specifically for one FragmentTaskMessage 
     * @param ftask
     * @param callback
     */
    public void setFragmentTaskCallback(FragmentTaskMessage ftask, RpcCallback<Dtxn.FragmentResponse> callback) {
        assert(callback != null) : "Null callback for txn #" + this.txn_id;
        if (trace.get()) LOG.trace("Storing FragmentTask callback for txn #" + this.txn_id);
        this.fragment_callbacks.put(ftask, callback);
    }

    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Current Round State", this.round_state);
        m.put("FragmentTask Callbacks", this.fragment_callbacks.size());
        m.put("Executing Locally", this.exec_local);
        m.put("Local Partition", this.executor.getPartitionId());
        m.put("Last UndoToken", this.last_undo_token);
        m.put("# of Rounds", this.round_ctr);
        return (String.format("Transaction State #%d\n%s", this.txn_id, StringUtil.formatMaps(m)));
    }
}