package edu.mit.hstore.dtxn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentTaskMessage;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;

public class DependencyInfo {
    protected static final Logger LOG = Logger.getLogger(DependencyInfo.class);
    private final static AtomicBoolean debug = new AtomicBoolean(LOG.isDebugEnabled());
    private final static AtomicBoolean trace = new AtomicBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * DependencyInfo Object Pool
     */
    public static final ObjectPool POOL = new StackObjectPool(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            return (new DependencyInfo());
        }
    });

    
    protected LocalTransactionState ts;
    protected int stmt_index;
    protected int dependency_id;
    protected final List<Integer> partitions = new ArrayList<Integer>();
    protected final Map<Integer, VoltTable> results = new HashMap<Integer, VoltTable>();
    protected final List<Integer> responses = new ArrayList<Integer>();
    
    /**
     * We assume a 1-to-n mapping from DependencyInfos to blocked FragmentTaskMessages
     */
    protected final Set<FragmentTaskMessage> blocked_tasks = new HashSet<FragmentTaskMessage>();
    protected boolean blocked_tasks_released = false;
    protected boolean blocked_all_local = true;
    
    /**
     * Constructor
     * @param stmt_index
     * @param dependency_id
     */
    private DependencyInfo() {
        // Nothing...
    }
    
    public void init(LocalTransactionState ts, int stmt_index, int dependency_id) {
        this.ts = ts;
        this.stmt_index = stmt_index;
        this.dependency_id = dependency_id;
    }
    
    public void finished() {
        this.partitions.clear();
        this.results.clear();
        this.responses.clear();
        this.blocked_tasks.clear();
        this.blocked_tasks_released = false;
        this.blocked_all_local = true;
        
        try {
            POOL.returnObject(this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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
        this.blocked_all_local = this.blocked_all_local && (ftask.getDestinationPartitionId() == this.ts.source_partition);
    }
    
    
    public Set<FragmentTaskMessage> getBlockedFragmentTaskMessages() {
        return (Collections.unmodifiableSet(this.blocked_tasks));
    }
    
    /**
     * Gets the blocked tasks for this DependencyInfo and marks them as "released"
     * @return
     */
    public synchronized Set<FragmentTaskMessage> getAndReleaseBlockedFragmentTaskMessages() {
        assert(this.blocked_tasks_released == false) : "Trying to unblock tasks more than once for txn #" + this.ts.txn_id;
        this.blocked_tasks_released = true;
        return (this.getBlockedFragmentTaskMessages());
    }
    
    /**
     * 
     * @param partition
     */
    public void addPartition(int partition) {
        this.partitions.add(partition);
    }
    
    public boolean addResponse(int partition) {
        if (trace.get()) LOG.trace("Storing RESPONSE for DependencyId #" + this.dependency_id + " from Partition #" + partition + " in txn #" + this.ts.txn_id);
        assert(this.responses.contains(partition) == false);
        this.responses.add(partition);
        return (this.results.containsKey(partition));
    }
    
    public boolean addResult(int partition, VoltTable result) {
        if (trace.get()) LOG.trace("Storing RESULT for DependencyId #" + this.dependency_id + " from Partition #" + partition + " in txn #" + this.ts.txn_id + " with " + result.getRowCount() + " tuples");
        assert(this.results.containsKey(partition) == false);
        this.results.put(partition, result);
        return (this.responses.contains(partition)); 
    }
    
    protected List<VoltTable> getResults() {
        return (Collections.unmodifiableList(new ArrayList<VoltTable>(this.results.values())));
    }
    
    protected List<Integer> getResponses() {
        return (this.responses);
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
        if (trace.get()) {
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

}