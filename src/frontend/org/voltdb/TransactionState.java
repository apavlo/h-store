/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
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
package org.voltdb;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;
import edu.mit.dtxn.Dtxn;

/**
 * @author pavlo
 */
public class TransactionState {
    protected static final Logger LOG = Logger.getLogger(TransactionState.class);
    
    enum RoundState {
        NULL,
        INITIALIZED,
        STARTED,
        FINISHED;
    }
    
    private final Object lock = new Object();
    
    /**
     * This is the real txn id that we want to use for all correspondence
     */
    private final long txn_id;

    /**
     * This is the txn id assigned to us by the coordinator. We need to use it 
     * whenever we need to send new requests to the coordinator.
     */
    private final Long dtxn_txn_id;
    
    private final ExecutionSite executor;
    private final long client_handle;
    private final int origin_partition;
    private final Set<Integer> touched_partitions = new HashSet<Integer>();
    private final boolean exec_local;
    private CountDownLatch dependency_latch;
    private Long start_undo_token;
    private Long last_undo_token;
    private RoundState round_state = RoundState.NULL;
    private int round_ctr = 0;

    /**
     * Callback to the coordinator for txns that are running on this partition
     */
    private RpcCallback<Dtxn.FragmentResponse> coordinator_callback;
    
    /**
     * Callbacks for specific FragmentTaskMessages
     * We have to keep these separate because the txn may request to execute a bunch of tasks that also go to this partition
     */
    private final HashMap<FragmentTaskMessage, RpcCallback<Dtxn.FragmentResponse>> fragment_callbacks = new HashMap<FragmentTaskMessage, RpcCallback<Dtxn.FragmentResponse>>(); 
    
    /**
     * SQLStmt Index -> DependencyId -> DependencyInfo
     */
    private final Map<Integer, ConcurrentHashMap<Integer, DependencyInfo>> dependencies = new TreeMap<Integer, ConcurrentHashMap<Integer, DependencyInfo>>();
    
    /**
     * Final result output dependencies. Each position in the list represents a single Statement
     */
    private final List<Integer> output_order = new ArrayList<Integer>();
    
    /**
     * As information come back to us, we need to keep track of what SQLStmt we are storing 
     * the data for. Note that we have to maintain two separate lists for results and responses
     * <Partition, DependencyId> -> Next SQLStmt Index
     */
    private final ConcurrentHashMap<Pair<Integer, Integer>, Queue<Integer>> results_dependency_stmt_ctr = new ConcurrentHashMap<Pair<Integer,Integer>, Queue<Integer>>();
    private final ConcurrentHashMap<Pair<Integer, Integer>, Queue<Integer>> responses_dependency_stmt_ctr = new ConcurrentHashMap<Pair<Integer,Integer>, Queue<Integer>>();
    
    /**
     * Blocked FragmentTaskMessages
     */
    private final Set<FragmentTaskMessage> blocked_tasks = new HashSet<FragmentTaskMessage>();
    
    /**
     * These are the DependencyIds that we don't bother returning to the ExecutionSite
     */
    private final Set<Integer> internal_dependencies = new HashSet<Integer>();
    
    /**
     * The total # of dependencies this Transaction is waiting for
     */
    private int dependency_ctr = 0;
    /**
     * The total # of dependencies received thus far
     */
    private int received_ctr = 0;
    
    /**
     * DependencyInfo
     */
    protected class DependencyInfo {
        private final int stmt_index;
        private final int dependency_id;
        private final List<Integer> partitions = new ArrayList<Integer>();
        private final Map<Integer, VoltTable> results = new HashMap<Integer, VoltTable>();
        private final List<Integer> responses = new ArrayList<Integer>();
        
        /**
         * We assume a 1-to-n mapping from DependencyInfos to blocked FragmentTaskMessages
         */
        private final Set<FragmentTaskMessage> blocked_tasks = new HashSet<FragmentTaskMessage>();
        private boolean blocked_tasks_released = false;
        private boolean blocked_all_local = true;
        
        /**
         * Constructor
         * @param stmt_index
         * @param dependency_id
         */
        public DependencyInfo(int stmt_index, int dependency_id) {
            this.stmt_index = stmt_index;
            this.dependency_id = dependency_id;
        }
        
        public int getStatementIndex() {
            return (this.stmt_index);
        }
        public int getDependencyId() {
            return (this.dependency_id);
        }
        public List<Integer> getPartitions() {
            return (this.partitions);
        }
        public void addBlockedFragmentTaskMessage(FragmentTaskMessage ftask) {
            this.blocked_tasks.add(ftask);
            this.blocked_all_local = this.blocked_all_local && (ftask.getDestinationPartitionId() == TransactionState.this.origin_partition);
        }
        
        
        public Set<FragmentTaskMessage> getBlockedFragmentTaskMessages() {
            return (Collections.unmodifiableSet(this.blocked_tasks));
        }
        
        /**
         * Gets the blocked tasks for this DependencyInfo and marks them as "released"
         * @return
         */
        public synchronized Set<FragmentTaskMessage> getAndReleaseBlockedFragmentTaskMessages() {
            assert(this.blocked_tasks_released == false) : "Trying to unblock tasks more than once for txn #" + txn_id;
            this.blocked_tasks_released = true;
            return (this.getBlockedFragmentTaskMessages());
        }

        /**
         * Return true if this Dependency is considered an internal dependency
         * @return
         */
        public boolean isInternal() {
            return (TransactionState.this.internal_dependencies.contains(this.dependency_id));
        }
        
        /**
         * 
         * @param partition
         */
        public void addPartition(int partition) {
            this.partitions.add(partition);
        }
        
        public boolean addResponse(int partition) {
            if (LOG.isTraceEnabled()) LOG.trace("Storing RESPONSE for DependencyId #" + this.dependency_id + " from Partition #" + partition + " in txn #" + TransactionState.this.txn_id);
            assert(this.responses.contains(partition) == false);
            this.responses.add(partition);
            return (this.results.containsKey(partition));
        }
        
        public boolean addResult(int partition, VoltTable result) {
            if (LOG.isTraceEnabled()) LOG.trace("Storing RESULT for DependencyId #" + this.dependency_id + " from Partition #" + partition + " in txn #" + TransactionState.this.txn_id + " with " + result.getRowCount() + " tuples");
            assert(this.results.containsKey(partition) == false);
            this.results.put(partition, result);
            return (this.responses.contains(partition)); 
        }
        
        protected List<VoltTable> getResults() {
            return (Collections.unmodifiableList(new ArrayList<VoltTable>(this.results.values())));
        }
        
        public VoltTable getResult() {
            assert(this.results.isEmpty() == false) : "There are no result available for " + this;
            assert(this.results.size() == 1) : "There are " + this.results.size() + " results for " + this + "\n-------\n" + this.getResults();
            return (CollectionUtil.getFirst(this.results.values()));
        }
        
        /**
         * Returns true if the task blocked by this Dependency is now ready to run 
         * @return
         */
        public boolean hasTasksReady() {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Block Tasks Not Empty? " + !this.blocked_tasks.isEmpty());
                LOG.trace("# of Results:   " + this.results.size());
                LOG.trace("# of Responses: " + this.responses.size());
                LOG.trace("# of <Responses/Results> Needed = " + this.partitions.size());
            }
            boolean ready = (this.blocked_tasks.isEmpty() == false) &&
                            (this.blocked_tasks_released == false) &&
                            (this.results.size() == this.partitions.size()) &&
                            (this.responses.size() == this.partitions.size());
            return (ready);
        }
        
        public boolean hasTasksBlocked() {
            return (!this.blocked_tasks.isEmpty());
        }
        
        public boolean hasTasksReleased() {
            return (this.blocked_tasks_released);
        }
        
        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("DependencyInfo[#").append(this.dependency_id).append("]\n")
             .append("  Partitions: ").append(this.partitions).append("\n")
             .append("  Responses:  ").append(this.responses.size()).append("\n")
             .append("  Results:    ").append(this.results.size()).append("\n")
             .append("  Blocked:    ").append(this.blocked_tasks).append("\n")
             .append("  Status:     ").append(this.blocked_tasks_released ? "RELEASED" : "BLOCKED").append("\n");
            return b.toString();
        }
    } // END CLASS

    /**
     * Constructor
     * @param dtxn_txn_id TODO
     * @param client_handle
     * @param callback
     */
    public TransactionState(ExecutionSite executor, long txn_id, Long dtxn_txn_id, int origin_partition, long client_handle, boolean exec_local) {
        this.executor = executor;
        this.origin_partition = origin_partition;
        this.txn_id = txn_id;
        this.dtxn_txn_id = dtxn_txn_id;
        this.client_handle = client_handle;
        this.exec_local = exec_local;
        
        assert(this.exec_local == false || (this.exec_local == true && this.dtxn_txn_id != null)) :
            "Missing Dtxn.Coordinator Txn Id for local Txn #" + this.txn_id; 
    }
    
    /**
     * Must be called once before one can add new FragmentTaskMessages for this txn 
     * @param undoToken
     */
    public void initRound(long undoToken) {
        assert(this.round_state == RoundState.NULL || this.round_state == RoundState.FINISHED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        synchronized (this.lock) {
            if (this.start_undo_token == null) {
                this.start_undo_token = undoToken;
            // Reset these guys here so that we don't waste time in the last round
            } else {
                this.internal_dependencies.clear();
                this.output_order.clear();
                this.blocked_tasks.clear();
                this.dependencies.clear();
                this.results_dependency_stmt_ctr.clear();
                this.received_ctr = 0;
                this.dependency_ctr = 0;
            }
            this.last_undo_token = undoToken;
            this.round_state = RoundState.INITIALIZED;
        }
        LOG.debug("Initializing new round information for txn #" + this.txn_id + " [undoToken=" + undoToken + "]");
    }
    
    /**
     * 
     * @return
     */
    public CountDownLatch startRound() {
        assert(this.round_state == RoundState.INITIALIZED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;

        if (this.exec_local) {
            assert(this.output_order.isEmpty());
            for (int stmt_index : this.dependencies.keySet()) {
                for (DependencyInfo dinfo : this.dependencies.get(stmt_index).values()) {
                    if (!dinfo.isInternal()) this.output_order.add(dinfo.getDependencyId());
                } // FOR
            } // FOR
            assert(this.getStatementCount() == this.output_order.size()) :
                "Expected " + this.getStatementCount() + " output dependencies but we queued up " + this.output_order.size();
            
            synchronized (this.lock) {
                int count = this.dependency_ctr - this.received_ctr;
                assert(count >= 0);
                assert(this.dependency_latch == null);
                this.dependency_latch = new CountDownLatch(count);
                this.round_state = RoundState.STARTED;
            }
        } else {
            // If the stored procedure is not executing locally then we need at least
            // one FragmentTaskMessage callback
            assert(this.fragment_callbacks.isEmpty() == false) :
                "No FragmentTaskMessage callbacks available for txn #" + this.txn_id;
            synchronized (this.lock) {
                this.round_state = RoundState.STARTED;
            }
        }
        return (this.dependency_latch);
    }
    
    /**
     * When a round is over, this must be called so that we can clean up the various
     * dependency tracking information that we have
     */
    public void finishRound() {
        assert(this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        assert(this.dependency_ctr == this.received_ctr) : "Trying to finish round for txn #" + this.txn_id + " before it was started"; 

        LOG.debug("Finishing round for txn #" + this.txn_id);
        synchronized (this.lock) {
            // Reset our initialization flag so that we can be ready to run more stuff the next round
            if (this.dependency_latch != null) {
                assert(this.dependency_latch.getCount() == 0);
                this.dependency_latch = null;
            }
            this.round_state = RoundState.FINISHED;
            this.round_ctr++;
        }
        // This needs to be here because we need to store these before initRound() gets called! 
//        this.fragment_callbacks.clear();
    }
    
    protected RoundState getCurrentRoundState() {
        return (this.round_state);
    }
    
    /**
     * 
     * @param stmt_index Statement Index
     * @param d_id Output Dependency Id
     * @return
     */
    protected DependencyInfo getDependencyInfo(int stmt_index, int d_id) {
        return (this.dependencies.get(stmt_index).get(d_id));
    }
    
    /**
     * 
     * @param d_id
     * @return
     */
    protected DependencyInfo getOrCreateDependencyInfo(int stmt_index, int d_id) {
        ConcurrentHashMap<Integer, DependencyInfo> stmt_dinfos = this.dependencies.get(stmt_index);
        if (stmt_dinfos == null) {
            stmt_dinfos = new ConcurrentHashMap<Integer, DependencyInfo>();
            this.dependencies.put(stmt_index, stmt_dinfos);
        }
        DependencyInfo d = stmt_dinfos.get(d_id);
        if (d == null) {
            d = new DependencyInfo(stmt_index, d_id);
            stmt_dinfos.put(d_id, d);
        }
        return (d);
    }
    
    /**
     * Get the final results of the last round of execution for this Transaction
     * @return
     */
    public VoltTable[] getResults() {
        final VoltTable results[] = new VoltTable[this.output_order.size()];
        LOG.debug("Generating output results with " + results.length + " tables for txn #" + this.txn_id);
        for (int stmt_index = 0, cnt = this.output_order.size(); stmt_index < cnt; stmt_index++) {
            Integer dependency_id = this.output_order.get(stmt_index);
            assert(dependency_id != null) :
                "Null output dependency id for Statement index " + stmt_index + " in txn #" + this.txn_id;
            assert(this.dependencies.containsKey(stmt_index)) :
                "Missing dependency set for stmt_index #" + stmt_index + " in txn #" + this.txn_id;
            assert(this.dependencies.get(stmt_index).containsKey(dependency_id)) :
                "Missing info for DependencyId " + dependency_id + " for Statement index " + stmt_index + " in txn #" + this.txn_id;
            results[stmt_index] = this.dependencies.get(stmt_index).get(dependency_id).getResult();
            assert(results[stmt_index] != null) :
                "Null output result for Statement index " + stmt_index + " in txn #" + this.txn_id;
        } // FOR
        return (results);
    }
    
    /**
     * Get this transaction's id
     * @return
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    
    /**
     * Get this transaction's Dtxn.Coordinator id
     * @return
     */
    public Long getDtxnTransactionId() {
        return this.dtxn_txn_id;
    }
    
    /**
     * 
     * @return
     */
    public Long getStartUndoToken() {
        return this.start_undo_token;
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
     * Returns true if this Transaction only touched a single-partition
     * @return
     */
    public boolean isSinglePartition() {
        return (this.touched_partitions.size() == 1);
    }

    /**
     * Retrieves and removes the coordinator callback
     * @return the coordinator_callback
     */
    public RpcCallback<Dtxn.FragmentResponse> getCoordinatorCallback() {
        RpcCallback<Dtxn.FragmentResponse> ret = this.coordinator_callback;
        this.coordinator_callback = null;
        return (ret);
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
        if (LOG.isTraceEnabled()) LOG.trace("Storing FragmentTask callback for txn #" + this.txn_id);
        this.fragment_callbacks.put(ftask, callback);
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
    
    /**
     * Return the number of statements that have been queued up in the last batch
     * @return
     */
    protected int getStatementCount() {
        return (this.dependencies.size());
    }
    protected Map<Integer, DependencyInfo> getStatementDependencies(int stmt_index) {
        return (this.dependencies.get(stmt_index));
    }
    protected List<Integer> getOutputOrder() {
        return (this.output_order);
    }
    
    public Set<Integer> getInternalDependencyIds() {
        return (this.internal_dependencies);
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
     * 
     * @param ftask
     */
    public synchronized boolean addFragmentTaskMessage(FragmentTaskMessage ftask) {
        assert(this.round_state == RoundState.INITIALIZED) : "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        // The partition that this task is being sent to for execution
        boolean blocked = false;
        int partition = ftask.getDestinationPartitionId();
        this.touched_partitions.add(partition);
        int num_fragments = ftask.getFragmentCount();
        
        // If this task produces output dependencies, then we need to make 
        // sure that the txn wait for it to arrive first
        if (ftask.hasOutputDependencies()) {
            for (int i = 0; i < num_fragments; i++) {
                int dependency_id = ftask.getOutputDependencyIds()[i];
                int stmt_index = ftask.getFragmentStmtIndexes()[i];
                
                if (trace) LOG.trace("Adding new Dependency [stmt_index=" + stmt_index + ",id=" + dependency_id + ",partition=" + partition + "] for txn #" + this.txn_id);
                this.getOrCreateDependencyInfo(stmt_index, dependency_id).addPartition(partition);
                this.dependency_ctr++;

                // Store the stmt_index of when this dependency will show up
                Pair<Integer, Integer> result_key = Pair.of(partition, dependency_id);
                if (!this.results_dependency_stmt_ctr.containsKey(result_key)) {
                    this.results_dependency_stmt_ctr.put(result_key, new LinkedList<Integer>());
                    this.responses_dependency_stmt_ctr.put(result_key, new LinkedList<Integer>());
                }
                this.results_dependency_stmt_ctr.get(result_key).add(stmt_index);
                this.responses_dependency_stmt_ctr.get(result_key).add(stmt_index);
            } // FOR
        }
        
        // If this task needs an input dependency, then we need to make sure it arrives at
        // the executor before it is allowed to start executing
        if (ftask.hasInputDependencies()) {
            if (trace) LOG.trace("Blocking fragments " + Arrays.toString(ftask.getFragmentIds()) + " waiting for " + ftask.getInputDependencyCount() + " dependencies in txn #" + this.txn_id + ": " + Arrays.toString(ftask.getAllUnorderedInputDepIds()));
            for (int i = 0; i < num_fragments; i++) {
                int dependency_id = ftask.getOnlyInputDepId(i);
                int stmt_index = ftask.getFragmentStmtIndexes()[i];
                this.getOrCreateDependencyInfo(stmt_index, dependency_id).addBlockedFragmentTaskMessage(ftask);
                this.internal_dependencies.add(dependency_id);
            } // FOR
            this.blocked_tasks.add(ftask);
            blocked = true;
        }
        if (debug) LOG.debug("Queued up FragmentTaskMessage in txn #" + this.txn_id + " for partition " + partition + " and marked as" + (blocked ? "" : " not") + " blocked");
        if (trace) LOG.trace("FragmentTaskMessage Contents for txn #" + this.txn_id + ":\n" + ftask);
        return (blocked);
    }
    
    /**
     * 
     * @param partition
     * @param dependency_id
     */
    public void addResponse(int partition, int dependency_id) {
        assert(this.round_state == RoundState.INITIALIZED || this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
//        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();

        // Each partition+dependency_id should be unique for a Statement batch.
        // So as the results come back to us, we have to figure out which Statement it belongs to
        Pair<Integer, Integer> result_key = Pair.of(partition, dependency_id);
        if (trace) LOG.trace("responses_dependency_stmt_ctr(result_key=" + result_key + "): " + this.responses_dependency_stmt_ctr.get(result_key));  

        assert(this.responses_dependency_stmt_ctr.containsKey(result_key)) :
            "Unexpected partition/dependency result pair " + result_key + " in txn #" + this.txn_id;
        assert(!this.responses_dependency_stmt_ctr.get(result_key).isEmpty()) :
            "No more statements for partition/dependency responses pair " + result_key + " in txn #" + this.txn_id;
        
        Integer stmt_index = this.responses_dependency_stmt_ctr.get(result_key).remove();
        assert(stmt_index != null) :
            "Null stmt_index for result_pair " + result_key + " in txn #" + this.txn_id;
        
        DependencyInfo d = this.getDependencyInfo(stmt_index, dependency_id);
        assert(d != null) :
            "Unexpected DependencyId " + dependency_id + " from partition " + partition + " for txn #" + this.txn_id + " " + this.dependencies.keySet() + " [stmt_index=" + stmt_index + "]";
        synchronized (this.lock) {
            if (d.addResponse(partition)) {
                this.received_ctr++;
                if (this.dependency_latch != null) {
                    this.dependency_latch.countDown();
                    if (trace) LOG.trace("Setting CountDownLatch to " + this.dependency_latch.getCount() + " for txn #" + this.txn_id);
                }    
            }
        } // SYNC
        // Check whether we need to start running stuff now
        if (!this.blocked_tasks.isEmpty() && d.hasTasksReady()) {
            this.executeBlockedTasks(d);
        }
    }
    
    /**
     * 
     * @param partition
     * @param dependency_id
     * @param result
     */
    public void addResult(int partition, int dependency_id, VoltTable result) {
        assert(this.round_state == RoundState.INITIALIZED || this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;

//        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();

        assert(result != null) :
            "The result for DependencyId " + dependency_id + " is null in txn #" + this.txn_id;

        // Each partition+dependency_id should be unique for a Statement batch.
        // So as the results come back to us, we have to figure out which Statement it belongs to
        Pair<Integer, Integer> result_key = Pair.of(partition, dependency_id);
        if (trace) LOG.trace("results_dependency_stmt_ctr(result_key=" + result_key + "): " + this.results_dependency_stmt_ctr.get(result_key));  
        assert(this.results_dependency_stmt_ctr.containsKey(result_key)) :
            "Unexpected partition/dependency result pair " + result_key + " in txn #" + this.txn_id;
        assert(!this.results_dependency_stmt_ctr.get(result_key).isEmpty()) :
            "No more statements for partition/dependency result pair " + result_key + " in txn #" + this.txn_id + "\n" + this;
        Integer stmt_index = this.results_dependency_stmt_ctr.get(result_key).remove();
        assert(stmt_index != null) :
            "Null stmt_index for result_pair " + result_key + " in txn #" + this.txn_id;
        
        DependencyInfo d = this.getDependencyInfo(stmt_index, dependency_id);
        assert(d != null) :
            "Unexpected DependencyId " + dependency_id + " from partition " + partition + " for txn #" + this.txn_id + " " + this.dependencies.keySet() + " [stmt_index=" + stmt_index + "]\n" + result;
        synchronized (this.lock) {
            if (d.addResult(partition, result)) {
                this.received_ctr++;
                if (this.dependency_latch != null) {
                    this.dependency_latch.countDown();
                    if (trace) LOG.trace("Setting CountDownLatch to " + this.dependency_latch.getCount() + " for txn #" + this.txn_id);
                }    
            }
        } // SYNC
        // Check whether we need to start running stuff now
        if (!this.blocked_tasks.isEmpty() && d.hasTasksReady()) {
            this.executeBlockedTasks(d);
        }
    }
    

    /**
     * Note the arrival of a new result that this txn needs
     * @param dependency_id
     * @param result
     */
    private synchronized void executeBlockedTasks(DependencyInfo d) {
//        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();
        
        // Always double check whether somebody beat us to the punch
        if (this.blocked_tasks.isEmpty()) return;
        
        List<FragmentTaskMessage> to_execute = new ArrayList<FragmentTaskMessage>();
        Set<FragmentTaskMessage> tasks = d.getAndReleaseBlockedFragmentTaskMessages();
        for (FragmentTaskMessage unblocked : tasks) {
            assert(unblocked != null);
            assert(this.blocked_tasks.contains(unblocked));
            this.blocked_tasks.remove(unblocked);
            if (trace) LOG.trace("Unblocking FragmentTaskMessage that was waiting for DependencyId #" + d.getDependencyId() + " in txn #" + this.txn_id);
            
            // If this task needs to execute locally, then we'll just queue up with the Executor
            if (d.blocked_all_local) {
                if (trace) LOG.trace("Sending unblocked task to local ExecutionSite for txn #" + this.txn_id);
                this.executor.doWork(unblocked);
            // Otherwise we will push out to the coordinator to handle for us
            // But we have to make sure that we attach the dependencies that they need 
            // so that the other partition can get to them
            } else {
                if (trace) LOG.trace("Sending unblocked task to partition #" + unblocked.getDestinationPartitionId() + " with attached results for txn #" + this.txn_id);
                to_execute.add(unblocked);
            }
        } // FOR
        if (!to_execute.isEmpty()) {
            this.executor.requestWork(this, to_execute);
        }
    }
    
    /**
     * Retrieve the dependency results that are used for internal plan execution
     * These are not the results that should be sent to the client
     * @return
     */
    public synchronized HashMap<Integer, List<VoltTable>> removeInternalDependencies(FragmentTaskMessage ftask) {
        if (LOG.isTraceEnabled()) LOG.trace("Retrieving " + this.internal_dependencies.size() + " internal dependencies for txn #" + this.txn_id);
        HashMap<Integer, List<VoltTable>> results = new HashMap<Integer, List<VoltTable>>();
        
        for (int i = 0, cnt = ftask.getFragmentCount(); i < cnt; i++) {
            int input_d_id = ftask.getOnlyInputDepId(i);
            if (input_d_id == ExecutionSite.NULL_DEPENDENCY_ID) continue;
            int stmt_index = ftask.getFragmentStmtIndexes()[i];

            DependencyInfo d = this.getDependencyInfo(stmt_index, input_d_id);
            assert(d != null);
            int num_tables = d.results.size();
            assert(d.getPartitions().size() == num_tables) :
                "Number of results retrieved for DependencyId is " + num_tables + " but we were expecting " + d.getPartitions().size();
            results.put(input_d_id, d.getResults());
        } // FOR
        return (results);
    }
    
    @Override
    public synchronized String toString() {
        String line = StringUtil.SINGLE_LINE;
        String proc_name = null;
        if (this.executor != null && this.executor.getRunningVoltProcedure(txn_id) != null) {
            proc_name = this.executor.getRunningVoltProcedure(txn_id).procedure_name;
        }
        
        String ret = line + "Transaction State #" + txn_id + " [" + proc_name + "]\n";

        ListOrderedMap<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Current Round State", this.round_state);
        m.put("Dtxn.Coordinator Id", this.dtxn_txn_id);
        m.put("Dtxn.Coordinator Callback", this.coordinator_callback);
        m.put("FragmentTask Callbacks", this.fragment_callbacks.size());
        m.put("Executing Locally", this.exec_local);
        m.put("Local Partition", this.executor.getPartitionId());
        m.put("Dependency Ctr", this.dependency_ctr);
        m.put("Start UndoToken", this.start_undo_token);
        m.put("Last UndoToken", this.last_undo_token);
        m.put("# of Rounds", this.round_ctr);
        
        if (this.exec_local) {
            m.put("Internal Ctr", this.internal_dependencies.size());
            m.put("Received Ctr", this.received_ctr);
            m.put("CountdownLatch", this.dependency_latch);
            m.put("# of Blocked Tasks", this.blocked_tasks.size());
            m.put("# of Statements", this.dependencies.size());
            m.put("Expected Results", this.results_dependency_stmt_ctr.keySet());
            m.put("Expected Responses", this.responses_dependency_stmt_ctr.keySet());
        }
        
        final String f = "  %-30s%s\n";
        for (Entry<String, Object> e : m.entrySet()) {
            ret += String.format(f, e.getKey()+":", e.getValue());
        } // FOR
        

        if (this.exec_local) {
            for (int stmt_index : this.dependencies.keySet()) {
                Map<Integer, DependencyInfo> s_dependencies = this.dependencies.get(stmt_index); 
                Set<Integer> dependency_ids = s_dependencies.keySet();
    
                ret += line;
                ret += "  Statement #" + stmt_index + "\n";
                ret += "  Output Dependency Id: " + (this.output_order.contains(stmt_index) ? this.output_order.get(stmt_index) : "<NOT STARTED>") + "\n";
                
                ret += "  Dependency Partitions:\n";
                for (Integer dependency_id : dependency_ids) {
                    ret += "    [" + dependency_id + "] => " + s_dependencies.get(dependency_id).partitions + "\n";
                } // FOR
                
                ret += "  Dependency Results:\n";
                for (Integer dependency_id : dependency_ids) {
                    ret += "    [" + dependency_id + "] => [";
                    String add = "";
                    for (VoltTable vt : s_dependencies.get(dependency_id).getResults()) {
                        ret += add + (vt == null ? vt : vt.getClass());
                        add = ",";
                    }
                    ret += "]\n";
                } // FOR
        
                ret += "  Blocked FragmentTaskMessages:\n";
                boolean none = true;
                for (Integer dependency_id : dependency_ids) {
                    DependencyInfo d = s_dependencies.get(dependency_id);
                    for (FragmentTaskMessage task : d.getBlockedFragmentTaskMessages()) {
                        if (task == null) continue;
                        ret += "    [" + dependency_id + "] => [";
                        String add = "";
                        for (long id : task.getFragmentIds()) {
                            ret += add + id;
                            add = ", ";
                        } // FOR
                        ret += "]";
                        if (d.hasTasksReady()) ret += " READY!"; 
                        ret += "\n";
                        none = false;
                    }
                } // FOR
                if (none) ret += "    <none>\n";
            } // (dependencies)
        } // (exec_local)
        
        return (ret);
    }
}