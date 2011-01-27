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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.mit.dtxn.Dtxn;

/**
 * 
 * @author pavlo
 */
public class LocalTransactionState extends TransactionState {
    protected static final Logger LOG = Logger.getLogger(LocalTransactionState.class);
    private final static AtomicBoolean debug = new AtomicBoolean(LOG.isDebugEnabled());
    private final static AtomicBoolean trace = new AtomicBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * 
     */
    private final Object lock = new Object();

    
    /**
     * LocalTransactionState Factory
     */
    public static class Factory extends BasePoolableObjectFactory {
        private final ExecutionSite executor;
        
        public Factory(ExecutionSite executor) {
            this.executor = executor;
        }
        @Override
        public Object makeObject() throws Exception {
            return new LocalTransactionState(this.executor);
        }
        @Override
        public void passivateObject(Object obj) throws Exception {
            super.passivateObject(obj);
            LocalTransactionState ts = (LocalTransactionState)obj;
            ts.clearRound();
        }
    };
    
    // ----------------------------------------------------------------------------
    // TRANSACTION INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Callback to the coordinator for txns that are running on this partition
     */
    private RpcCallback<Dtxn.FragmentResponse> coordinator_callback;

    // ----------------------------------------------------------------------------
    // ROUND DATA MEMBERS
    // ----------------------------------------------------------------------------

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
     * <Partition, DependencyId> -> Next SQLStmt Index
     */
    private final Map<PartitionDependencyKey, Queue<Integer>> results_dependency_stmt_ctr = new ConcurrentHashMap<PartitionDependencyKey, Queue<Integer>>();
    private final Map<PartitionDependencyKey, Queue<Integer>> responses_dependency_stmt_ctr = new ConcurrentHashMap<PartitionDependencyKey, Queue<Integer>>();

    /**
     * Sometimes we will get responses/results back while we are still queuing up the rest of the tasks and
     * haven't started the next round. So we need a temporary space where we can put these guys until 
     * we start the round. Otherwise calculating the proper latch count is tricky
     */
    private final Set<PartitionDependencyKey> queued_responses = new ListOrderedSet<PartitionDependencyKey>();
    private final Map<PartitionDependencyKey, VoltTable> queued_results = new ListOrderedMap<PartitionDependencyKey, VoltTable>();
    
    /**
     * Blocked FragmentTaskMessages
     */
    private final Set<FragmentTaskMessage> blocked_tasks = new HashSet<FragmentTaskMessage>();
    
    /**
     * These are the DependencyIds that we don't bother returning to the ExecutionSite
     */
    private final Set<Integer> internal_dependencies = new HashSet<Integer>();

    /**
     * Number of SQLStmts in the current batch
     */
    private Integer batch_size = null;
    
    /**
     * The total # of dependencies this Transaction is waiting for in the current round
     */
    private int dependency_ctr = 0;
    
    /**
     * The total # of dependencies received thus far in the current round
     */
    private int received_ctr = 0;
    
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
    
    @Override
    @SuppressWarnings("unchecked")
    public LocalTransactionState init(long txnId, long clientHandle, int source_partition) {
        return ((LocalTransactionState)super.init(txnId, clientHandle, source_partition, true));
    }
    
    @Override
    public void finished() {
        super.finished();

        this.coordinator_callback = null;
        this.clearRound();
    }
    
    private void clearRound() {
        this.output_order.clear();
        this.results_dependency_stmt_ctr.clear();
        this.responses_dependency_stmt_ctr.clear();
        this.queued_responses.clear();
        this.queued_results.clear();
        this.blocked_tasks.clear();
        this.internal_dependencies.clear();
        
        if (this.batch_size != null) {
            for (int i = 0; i < this.batch_size; i++) {
                this.dependencies[i].clear();
            } // FOR
        }
        
        this.batch_size = null;
        this.dependency_ctr = 0;
        this.received_ctr = 0;
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
        assert(this.queued_responses.isEmpty()) : "Trying to initialize round for txn #" + this.txn_id + " but there are " + this.queued_responses.size() + " queued responses";
        assert(this.queued_results.isEmpty()) : "Trying to initialize round for txn #" + this.txn_id + " but there are " + this.queued_results.size() + " queued results";
        
        synchronized (this.lock) {
            super.initRound(undoToken);
            
            // Reset these guys here so that we don't waste time in the last round
            if (this.last_undo_token != null) this.clearRound();
        } // SYNCHRONIZED
    }
    
    @Override
    public void startRound() {
        assert(this.output_order.isEmpty());
        assert(this.batch_size != null);
        if (debug.get()) LOG.debug("Starting round for local txn #" + this.txn_id + " with " + this.batch_size + " queued Statements");
        
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
            if (trace.get()) LOG.trace("Releasing " + this.queued_responses.size() + " queued responses");
            for (Pair<Integer, Integer> p : this.queued_responses) {
                this.addResponse(p.getFirst(), p.getSecond());
            } // FOR
            if (trace.get()) LOG.trace("Releasing " + this.queued_results.size() + " queued results");
            for (Entry<PartitionDependencyKey, VoltTable> e : this.queued_results.entrySet()) {
                this.addResult(e.getKey().getFirst(), e.getKey().getSecond(), e.getValue());
            } // FOR
            this.queued_responses.clear();
            this.queued_results.clear();
            
            // Now create the latch
            int count = this.dependency_ctr - this.received_ctr;
            assert(count >= 0);
            assert(this.dependency_latch == null);
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
        
        if (debug.get()) LOG.debug("Finishing " + (this.exec_local ? "" : "non-") + "local round for txn #" + this.txn_id);
        synchronized (this.lock) {
            super.finishRound();
            
            // Reset our initialization flag so that we can be ready to run more stuff the next round
            if (this.dependency_latch != null) {
                assert(this.dependency_latch.getCount() == 0);
                if (trace.get()) LOG.debug("Setting CountDownLatch to null for txn #" + this.txn_id);
                this.dependency_latch = null;
            }
        } // SYNCHRONIZED
        // This needs to be here because we need to store these before initRound() gets called! 
//        this.fragment_callbacks.clear();
    }
    
    public void setBatchSize(int batchSize) {
        this.batch_size = batchSize;
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
    
    /**
     * Returns true if this Transaction was originally predicted to be single-partitioned
     * @return
     */
    public boolean isPredictSinglePartition() {
        return (this.single_partitioned);
    }
    
    /**
     * Returns true if this Transaction has executed only on a single-partition
     * @return
     */
    public boolean isExecSinglePartition() {
        return (this.touched_partitions.size() == 1);
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
     * 
     * @param d_id
     * @return
     */
    private DependencyInfo getOrCreateDependencyInfo(int stmt_index, int d_id) {
        Map<Integer, DependencyInfo> stmt_dinfos = this.dependencies[stmt_index];
        if (stmt_dinfos == null) {
            stmt_dinfos = new ConcurrentHashMap<Integer, DependencyInfo>();
            this.dependencies[stmt_index] = stmt_dinfos;
        }
        DependencyInfo d = stmt_dinfos.get(d_id);
        if (d == null) {
            try {
                d = (DependencyInfo)DependencyInfo.POOL.borrowObject();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            d.init(this, stmt_index, d_id);
            stmt_dinfos.put(d_id, d);
        }
        return (d);
    }
    
    /**
     * Get the final results of the last round of execution for this Transaction
     * @return
     */
    @Override
    public VoltTable[] getResults() {
        final VoltTable results[] = new VoltTable[this.output_order.size()];
        LOG.debug("Generating output results with " + results.length + " tables for txn #" + this.txn_id);
        for (int stmt_index = 0, cnt = this.output_order.size(); stmt_index < cnt; stmt_index++) {
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
     * Returns true if the FragmentTaskMessage can be executed immediately (either locally or on at a remote partition)
     * @param ftask
     */
    public boolean addFragmentTaskMessage(FragmentTaskMessage ftask) {
        assert(this.round_state == RoundState.INITIALIZED) : "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        
        // The partition that this task is being sent to for execution
        boolean blocked = false;
        int partition = ftask.getDestinationPartitionId();
        this.touched_partitions.add(partition);
        int num_fragments = ftask.getFragmentCount();
        
        // If this task produces output dependencies, then we need to make 
        // sure that the txn wait for it to arrive first
        synchronized (this.lock) {
            if (ftask.hasOutputDependencies()) {
                for (int i = 0; i < num_fragments; i++) {
                    int dependency_id = ftask.getOutputDependencyIds()[i];
                    int stmt_index = ftask.getFragmentStmtIndexes()[i];
                    
                    if (trace.get()) LOG.trace("Adding new Dependency [stmt_index=" + stmt_index + ", id=" + dependency_id + ", partition=" + partition + "] for txn #" + this.txn_id);
                    this.getOrCreateDependencyInfo(stmt_index, dependency_id).addPartition(partition);
                    this.dependency_ctr++;
    
                    // Store the stmt_index of when this dependency will show up
                    PartitionDependencyKey result_key = new PartitionDependencyKey(partition, dependency_id);
                    if (!this.results_dependency_stmt_ctr.containsKey(result_key)) {
                        this.results_dependency_stmt_ctr.put(result_key, new LinkedList<Integer>());
                        this.responses_dependency_stmt_ctr.put(result_key, new LinkedList<Integer>());
                    }
                    this.results_dependency_stmt_ctr.get(result_key).add(stmt_index);
                    this.responses_dependency_stmt_ctr.get(result_key).add(stmt_index);
                    if (trace.get()) LOG.trace("Set Dependency Statement Counters for " + result_key + ": " + this.results_dependency_stmt_ctr);
                    assert(this.responses_dependency_stmt_ctr.get(result_key).size() == this.results_dependency_stmt_ctr.get(result_key).size());
                } // FOR
            }
            
            // If this task needs an input dependency, then we need to make sure it arrives at
            // the executor before it is allowed to start executing
            if (ftask.hasInputDependencies()) {
                if (trace.get()) LOG.trace("Blocking fragments " + Arrays.toString(ftask.getFragmentIds()) + " waiting for " + ftask.getInputDependencyCount() + " dependencies in txn #" + this.txn_id + ": " + Arrays.toString(ftask.getAllUnorderedInputDepIds()));
                for (int i = 0; i < num_fragments; i++) {
                    int dependency_id = ftask.getOnlyInputDepId(i);
                    int stmt_index = ftask.getFragmentStmtIndexes()[i];
                    this.getOrCreateDependencyInfo(stmt_index, dependency_id).addBlockedFragmentTaskMessage(ftask);
                    this.internal_dependencies.add(dependency_id);
                } // FOR
                this.blocked_tasks.add(ftask);
                blocked = true;
            }
        } // SYNC
        if (debug.get()) LOG.debug("Queued up FragmentTaskMessage in txn #" + this.txn_id + " for partition " + partition + " and marked as" + (blocked ? "" : " not") + " blocked");
        if (trace.get()) LOG.trace("FragmentTaskMessage Contents for txn #" + this.txn_id + ":\n" + ftask);
        return (blocked);
    }
    
    /**
     * 
     * @param partition
     * @param dependency_id
     * @param result
     */
    private void processResultResponse(int partition, int dependency_id, VoltTable result) {
        final String type = (result != null ? "RESULT" : "RESPONSE");
        assert(this.round_state == RoundState.INITIALIZED || this.round_state == RoundState.STARTED) :
            "Invalid round state " + this.round_state + " for txn #" + this.txn_id;
        assert(this.exec_local) :
            "Trying to store " + type + " for txn #" + this.txn_id + " but it is not executing locally!";
//        final boolean debug = LOG.isDebugEnabled();

        final PartitionDependencyKey key = new PartitionDependencyKey(partition, dependency_id);
        DependencyInfo d = null;
        
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
                if (trace.get()) LOG.trace("Queued " + type + " " + key + " for txn #" + this.txn_id + " until the round is started");
                return;
            }
            
            Map<PartitionDependencyKey, Queue<Integer>> stmt_ctr = (result != null ? this.results_dependency_stmt_ctr : this.responses_dependency_stmt_ctr);
            if (trace.get()) LOG.trace("Storing new " + type + " for key " + key + " in txn #" + this.txn_id);
        
            // Each partition+dependency_id should be unique for a Statement batch.
            // So as the results come back to us, we have to figure out which Statement it belongs to
            if (trace.get()) LOG.trace(type + " stmt_ctr(key=" + key + "): " + stmt_ctr.get(key));
            if (stmt_ctr.containsKey(key) == false) System.err.println(stmt_ctr);
            assert(stmt_ctr.containsKey(key)) :
                "Unexpected partition/dependency " + type + " pair " + key + " in txn #" + this.txn_id;
            assert(!stmt_ctr.get(key).isEmpty()) :
                "No more statements for partition/dependency " + type + " pair " + key + " in txn #" + this.txn_id + "\n" + this;
            Integer stmt_index = stmt_ctr.get(key).remove();
            assert(stmt_index != null) :
                "Null stmt_index for " + type + " " + key + " in txn #" + this.txn_id;
            
            d = this.getDependencyInfo(stmt_index, dependency_id);
            assert(d != null) :
                "Unexpected DependencyId " + dependency_id + " from partition " + partition + " for txn #" + this.txn_id + " [stmt_index=" + stmt_index + "]\n" + result;

            final boolean complete = (result != null ? d.addResult(partition, result) : d.addResponse(partition));
            if (complete) {
                if (trace.get()) LOG.trace("Received all RESULTS + RESPONSES for [stmt#" + stmt_index + ", dep#=" + dependency_id + "] for txn #" + this.txn_id);
                this.received_ctr++;
                if (this.dependency_latch != null) {
                    this.dependency_latch.countDown();
                    if (trace.get()) LOG.trace("Setting CountDownLatch to " + this.dependency_latch.getCount() + " for txn #" + this.txn_id);
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
     */
    public void addResponse(int partition, int dependency_id) {
        this.processResultResponse(partition, dependency_id, null);
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
        this.processResultResponse(partition, dependency_id, result);
    }
    
    protected void addResultWithResponse(int partition, int dependency_id, VoltTable result) {
        this.addResult(partition, dependency_id, result);
        this.addResponse(partition, dependency_id);
    }
    

    /**
     * Note the arrival of a new result that this txn needs
     * @param dependency_id
     * @param result
     */
    private synchronized void executeBlockedTasks(DependencyInfo d) {
        
        // Always double check whether somebody beat us to the punch
        if (this.blocked_tasks.isEmpty() || d.blocked_tasks_released) return;
        
        List<FragmentTaskMessage> to_execute = new ArrayList<FragmentTaskMessage>();
        Set<FragmentTaskMessage> tasks = d.getAndReleaseBlockedFragmentTaskMessages();
        for (FragmentTaskMessage unblocked : tasks) {
            assert(unblocked != null);
            assert(this.blocked_tasks.contains(unblocked));
            this.blocked_tasks.remove(unblocked);
            if (trace.get()) LOG.trace("Unblocking FragmentTaskMessage that was waiting for DependencyId #" + d.getDependencyId() + " in txn #" + this.txn_id);
            
            // If this task needs to execute locally, then we'll just queue up with the Executor
            if (d.blocked_all_local) {
                if (trace.get()) LOG.trace("Sending unblocked task to local ExecutionSite for txn #" + this.txn_id);
                this.executor.doWork(unblocked);
            // Otherwise we will push out to the coordinator to handle for us
            // But we have to make sure that we attach the dependencies that they need 
            // so that the other partition can get to them
            } else {
                if (trace.get()) LOG.trace("Sending unblocked task to partition #" + unblocked.getDestinationPartitionId() + " with attached results for txn #" + this.txn_id);
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
        if (debug.get()) LOG.debug("Retrieving " + this.internal_dependencies.size() + " internal dependencies for txn #" + this.txn_id);
        HashMap<Integer, List<VoltTable>> results = new HashMap<Integer, List<VoltTable>>();
        
        for (int i = 0, cnt = ftask.getFragmentCount(); i < cnt; i++) {
            int input_d_id = ftask.getOnlyInputDepId(i);
            if (input_d_id == ExecutionSite.NULL_DEPENDENCY_ID) continue;
            int stmt_index = ftask.getFragmentStmtIndexes()[i];

            DependencyInfo d = this.getDependencyInfo(stmt_index, input_d_id);
            assert(d != null);
            int num_tables = d.results.size();
            assert(d.getPartitions().size() == num_tables) :
                "Number of results retrieved for <Stmt #" + stmt_index + ", DependencyId #" + input_d_id + "> is " + num_tables +
                " but we were expecting " + d.getPartitions().size() + " in txn #" + this.txn_id +
                " [" + this.executor.getRunningVoltProcedure(this.txn_id).getClass().getSimpleName() + "]\n" + 
                this.toString() + "\n" +
                ftask.toString();
            results.put(input_d_id, d.getResults());
        } // FOR
        return (results);
    }
    
    @Override
    public synchronized String toString() {
        String ret = super.toString() + StringUtil.SINGLE_LINE;

        String proc_name = null;
        if (this.executor != null && this.executor.getRunningVoltProcedure(txn_id) != null) {
            proc_name = this.executor.getRunningVoltProcedure(txn_id).getProcedureName();
        }
        
        ListOrderedMap<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Procedure", proc_name);
        m.put("Dtxn.Coordinator Callback", this.coordinator_callback);
        m.put("Dependency Ctr", this.dependency_ctr);
        m.put("Internal Ctr", this.internal_dependencies.size());
        m.put("Received Ctr", this.received_ctr);
        m.put("CountdownLatch", this.dependency_latch);
        m.put("# of Blocked Tasks", this.blocked_tasks.size());
        m.put("# of Statements", this.batch_size);
        m.put("Expected Results", this.results_dependency_stmt_ctr.keySet());
        m.put("Expected Responses", this.responses_dependency_stmt_ctr.keySet());
        ret += StringUtil.formatMaps(m);

        for (int stmt_index = 0; stmt_index < this.batch_size; stmt_index++) {
            Map<Integer, DependencyInfo> s_dependencies = this.dependencies[stmt_index]; 
            Set<Integer> dependency_ids = s_dependencies.keySet();

            ret += StringUtil.SINGLE_LINE;
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
                    ret += add + (vt == null ? vt : "{" + vt.getRowCount() + " tuples}");
                    add = ",";
                }
                ret += "]\n";
            } // FOR
            
            ret += "  Dependency Responses:\n";
            for (Integer dependency_id : dependency_ids) {
                ret += "    [" + dependency_id + "] => " + s_dependencies.get(dependency_id).getResponses() + "\n";
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
        
        return (ret);
    }
}
