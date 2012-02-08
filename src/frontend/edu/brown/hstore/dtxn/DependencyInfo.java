package edu.brown.hstore.dtxn;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import edu.brown.hstore.Hstore.WorkFragment;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.Poolable;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class DependencyInfo implements Poolable {
    protected static final Logger LOG = Logger.getLogger(DependencyInfo.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();
    
    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private Long txn_id;
    private int round;
    private int stmt_index = -1;
    private int dependency_id = -1;
    
    /**
     * List of PartitionIds that we expect to get responses/results back
     */
    private final List<Integer> partitions = new ArrayList<Integer>();
    
    /**
     * The list of PartitionIds that have sent results
     */
    private final List<Integer> results = new ArrayList<Integer>();

    /**
     * The list of VoltTable results that have been sent back partitions
     * We store it as a list so that we don't have to convert it for ExecutionSite
     */
    private final List<VoltTable> results_list = new ArrayList<VoltTable>();
    
    /**
     * We assume a 1-to-n mapping from DependencyInfos to blocked FragmentTaskMessages
     */
    private final Set<WorkFragment> blocked_tasks = new HashSet<WorkFragment>();
    private boolean blocked_tasks_released = false;
    
    private boolean internal = false;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    public DependencyInfo() {
        // Nothing...
    }
    
    public void init(Long txn_id, int round, int stmt_index, int dependency_id) {
        if (d) LOG.debug(String.format("#%s - Intializing DependencyInfo for %s in ROUND #%d",
                                       txn_id, LocalTransaction.debugStmtDep(stmt_index, dependency_id), round));
        this.txn_id = txn_id;
        this.round = round;
        this.stmt_index = stmt_index;
        this.dependency_id = dependency_id;
    }

    public Long getTransactionId() {
        return (this.txn_id);
    }
    
    public boolean inRound(Long txn_id, int round) {
        return (txn_id.equals(this.txn_id) && this.round == round);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.txn_id != null);
    }
    
    @Override
    public void finish() {
        this.txn_id = null;
        this.stmt_index = -1;
        this.dependency_id = -1;
        this.partitions.clear();
        this.results.clear();
        this.results_list.clear();
        this.blocked_tasks.clear();
        this.blocked_tasks_released = false;
        this.internal = false;
    }
    
    public int getStatementIndex() {
        return (this.stmt_index);
    }
    public int getDependencyId() {
        return (this.dependency_id);
    }
    protected List<Integer> getPartitions() {
        return (this.partitions);
    }
    
    public void markInternal() {
        if (d) LOG.debug(String.format("#%s - Marking DependencyInfo for %s as internal",
                                       txn_id, LocalTransaction.debugStmtDep(stmt_index, dependency_id)));
        this.internal = true;
    }
    public boolean isInternal() {
        return this.internal;
    }
    
    /**
     * Add a FragmentTaskMessage this blocked until all of the partitions return results/responses
     * for this DependencyInfo
     * @param ftask
     */
    public void addBlockedWorkFragment(WorkFragment ftask) {
        if (t) LOG.trace("Adding block FragmentTaskMessage for txn #" + this.txn_id);
        this.blocked_tasks.add(ftask);
//        this.blocked_all_local = this.blocked_all_local && (ftask.getDestinationPartitionId() == this.ts.base_partition);
    }
    
    /**
     * Return the set of FragmentTaskMessages that are blocked until all of the partitions
     * return results/responses for this DependencyInfo 
     * @return
     */
    protected Set<WorkFragment> getBlockedWorkFragments() {
        return (this.blocked_tasks);
    }
    
    /**
     * Gets the blocked tasks for this DependencyInfo and marks them as "released"
     * If the tasks have already been released, then the return value will be null;
     * @return
     */
    public Collection<WorkFragment> getAndReleaseBlockedWorkFragments() {
        if (this.blocked_tasks_released == false) {
            this.blocked_tasks_released = true;
            if (t) LOG.trace(String.format("Unblocking %d FragmentTaskMessages for txn #%d", this.blocked_tasks.size(), this.txn_id));
            return (this.blocked_tasks);
        }
        if (t) LOG.trace(String.format("Ignoring duplicate release request for txn #%d", this.txn_id));
        return (null);
    }
    
    /**
     * Add a partition id that we expect to return a result/response for this dependency
     * @param partition
     */
    public void addPartition(int partition) {
        this.partitions.add(partition);
    }
    
    /**
     * Add a result for a PartitionId
     * Returns true if we have also stored the response for this PartitionId
     * @param partition
     * @param result
     * @return
     */
    public synchronized boolean addResult(int partition, VoltTable result) {
        if (d) LOG.debug(String.format("#%s - Storing RESULT for DependencyId #%d from Partition #%d with %d tuples",
                                       this.txn_id, this.dependency_id, partition, result.getRowCount()));
        assert(this.results.contains(partition) == false) :
            String.format("Trying to add result for {Partition:%d, Dependency:%d} twice for %s!",
                          partition, this.dependency_id, this.txn_id); 
        this.results.add(partition);
        this.results_list.add(result);
        return (true); // this.responses.contains(partition)); 
    }
    
    protected List<VoltTable> getResults() {
        return (this.results_list);
    }
    
    /**
     * This is a very important method but it actually sucks
     * In order to use a VoltTable that was produce by another partition on the same HStoreSite,
     * we have to make a copy of this data into a new ByteBuffer.
     * This is a horrible hack and will need to be revisited once we start figuring things out
     * @param local_partition
     * @param flip_local_partition
     * @return
     */
    protected List<VoltTable> getResults(Collection<Integer> local_partitions, boolean flip_local_partition) {
        if (flip_local_partition) {
            for (int i = 0, cnt = this.results.size(); i < cnt; i++) {
                Integer partition = this.results.get(i);
                if (local_partitions.contains(partition)) {
                    if (d) LOG.debug(String.format("%s - Copying VoltTable ByteBuffer for DependencyId %d from Partition %d",
                                                   this.txn_id, this.dependency_id, partition));
                    VoltTable vt = this.results_list.get(i);
                    assert(vt != null);
                    ByteBuffer buffer = vt.getTableDataReference();
                    byte arr[] = new byte[vt.getUnderlyingBufferSize()]; // FIXME
                    buffer.get(arr, 0, arr.length);
                    this.results_list.set(i, new VoltTable(ByteBuffer.wrap(arr), true));
                }
            } // FOR
        }
        return (this.results_list);
    }
    
    /**
     * Return just the first result for this DependencyInfo
     * This should only be called to get back the results for the final VoltTable of a query
     * @return
     */
    public VoltTable getResult() {
        assert(this.results.isEmpty() == false) : "There are no result available for " + this;
        assert(this.results.size() == 1) : "There are " + this.results.size() + " results for " + this + "\n-------\n" + this.results_list;
        return (this.results_list.get(0));
    }
    
    /**
     * Returns true if the task blocked by this Dependency is now ready to run 
     * @return
     */
    public boolean hasTasksReady() {
        if (t) {
            LOG.trace("Block Tasks Not Empty? " + !this.blocked_tasks.isEmpty());
            LOG.trace(String.format("# of Results:   %d / %d", this.results.size(), this.partitions.size()));
        }
        boolean ready = (this.blocked_tasks.isEmpty() == false) &&
                        (this.blocked_tasks_released == false) &&
                        (this.results.size() == this.partitions.size());
        return (ready);
    }
    
    public boolean hasTasksBlocked() {
        return (this.blocked_tasks.isEmpty() == false);
    }
    
    public boolean hasTasksReleased() {
        return (this.blocked_tasks_released);
    }
    
    @Override
    public String toString() {
        if (this.isInitialized() == false) {
            return ("<UNINITIALIZED>");
        }
        
        String status = null;
        if (this.results.size() == this.partitions.size()) {
            if (this.blocked_tasks_released == false) {
                status = "READY";
            } else {
                status = "RELEASED";
            }
        } else if (this.blocked_tasks.isEmpty()) {
            status = "WAITING";
        } else {
            status = "BLOCKED";
        }
        
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("  Hash Code", this.hashCode());
        m.put("  Internal", this.internal);
        m.put("  Partitions", this.partitions);
        
        Map<String, Object> inner = new ListOrderedMap<String, Object>();
        for (int i = 0, cnt = this.results.size(); i < cnt; i++) {
            int partition = this.results.get(i);
            VoltTable vt = this.results_list.get(i);
            inner.put(String.format("Partition %02d",partition), String.format("{%d tuples}", vt.getRowCount()));  
        }
        m.put("  Results", inner);
        m.put("  Blocked", this.blocked_tasks);
        m.put("  Status", status);

        return String.format("DependencyInfo[#%d]\n%s", this.dependency_id, StringUtil.formatMaps(m));
    }

}