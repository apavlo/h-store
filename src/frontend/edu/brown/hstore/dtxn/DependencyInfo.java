package edu.brown.hstore.dtxn;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import edu.brown.hstore.Hstoreservice.WorkFragment;
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
    private final BitSet partitions = new BitSet();
    
    /**
     * The list of VoltTable results that have been sent back partitions
     * We store it as a list so that we don't have to convert it for ExecutionSite
     */
    private final List<VoltTable> results = new ArrayList<VoltTable>();
    private int results_ctr = 0;
    
    /**
     * The last partition that we inserted data for
     */
    private int results_lastPartition = -1;
    
    /**
     * We assume a 1-to-n mapping from DependencyInfos to blocked FragmentTaskMessages
     */
    private final Set<WorkFragment> blockedTasks = new HashSet<WorkFragment>();
    
    /**
     * If set to true, that means we have already released all the tasks that were 
     * blocked on the results generated for this dependency
     */
    private boolean blockedTasksReleased = false;
    
    /**
     * Is the data for this dependency for intermediate results that
     * are only sent to another WorkFragment (as opposed to being sent back
     * to the transaction's control code). 
     */
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
    protected int getRound() {
        return (this.round);
    }
    
    public boolean inSameTxnRound(Long txn_id, int round) {
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
        this.blockedTasks.clear();
        this.blockedTasksReleased = false;
        this.internal = false;
        
        for (int i = 0, cnt = this.results.size(); i < cnt; i++)
            this.results.set(i, null);
        this.results_ctr = 0;
        this.results_lastPartition = -1;
    }
    
    public int getStatementIndex() {
        return (this.stmt_index);
    }
    public int getDependencyId() {
        return (this.dependency_id);
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
        this.blockedTasks.add(ftask);
    }
    
    /**
     * Return the set of FragmentTaskMessages that are blocked until all of the partitions
     * return results/responses for this DependencyInfo 
     * @return
     */
    protected Collection<WorkFragment> getBlockedWorkFragments() {
        return (this.blockedTasks);
    }
    
    /**
     * Gets the blocked tasks for this DependencyInfo and marks them as "released"
     * If the tasks have already been released, then the return value will be null;
     * @return
     */
    public Collection<WorkFragment> getAndReleaseBlockedWorkFragments() {
        if (this.blockedTasksReleased == false) {
            this.blockedTasksReleased = true;
            if (t) LOG.trace(String.format("Unblocking %d FragmentTaskMessages for txn #%d", this.blockedTasks.size(), this.txn_id));
            return (this.blockedTasks);
        }
        if (t) LOG.trace(String.format("Ignoring duplicate release request for txn #%d", this.txn_id));
        return (null);
    }
    
    /**
     * Add a partition id that we expect to return a result/response for this dependency
     * @param partition
     */
    public void addPartition(int partition) {
        this.partitions.set(partition);
    }
    /**
     * <B>NOTE:</B> This should only be called for DEBUG purposes only
     */
    protected int getPartitionCount() {
        return (this.partitions.cardinality());
    }
    /**
     * <B>NOTE:</B> This should only be called for DEBUG purposes only
     */
    protected List<Integer> getPartitions() {
        List<Integer> p = new ArrayList<Integer>();
        for (int i = 0, cnt = this.partitions.size(); i < cnt; i++) {
            if (this.partitions.get(i)) p.add(i);
        }
        return (p);
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
        if (partition >= this.results.size()) {
            if (d) LOG.debug(String.format("#%s - Resizing internal result list for DependencyId #%d [OLD:%d -> NEW:%d]",
                                           this.txn_id, this.dependency_id, this.results.size(), partition+1));
            int ctr = (partition+1) - this.results.size();
            while (ctr-- > 0) this.results.add(null);
        }
        assert(this.results.get(partition) == null) :
            String.format("Trying to add result for {Partition:%d, Dependency:%d} twice for %s!",
                          partition, this.dependency_id, this.txn_id); 
        this.results.set(partition, result);
        this.results_ctr++;
        this.results_lastPartition = partition;
        return (true); 
    }
    
    protected int getResultsCount() {
        return (this.results_ctr);
    }
    protected List<VoltTable> getResults() {
        return (this.results);
    }
    
    /**
     * Return just the first result for this DependencyInfo
     * This should only be called to get back the results for the final VoltTable of a query
     * @return
     */
    public VoltTable getResult() {
        assert(this.results_ctr > 0) : "There are no result available for " + this;
        assert(this.results_ctr == 1) : 
            "There are " + this.results_ctr + " results for " + this + "\n-------\n" + this.results;
        assert(this.results_lastPartition != -1);
        return (this.results.get(this.results_lastPartition));
    }
    
    /**
     * Returns true if the task blocked by this Dependency is now ready to run 
     * @return
     */
    public boolean hasTasksReady() {
        if (d) LOG.debug(String.format("txn #%d - hasTasksReady()\n" +
                                       "Block Tasks Not Empty? %s\n" + 
                                       "# of Results:   %d\n" +
                                       "# of Partitions: %d",
                                       this.txn_id,
                                       this.blockedTasks.isEmpty() == false,
                                       this.results_ctr, this.partitions.cardinality()));
        assert(this.results_ctr <= this.partitions.cardinality()) :
            String.format("Invalid DependencyInfo state for txn #%d. " +
            		      "There are %d results but %d partitions",
            		      this.txn_id, this.results_ctr, this.partitions.cardinality());
        
        return (this.blockedTasks.isEmpty() == false) &&
               (this.blockedTasksReleased == false) &&
               (this.results_ctr == this.partitions.cardinality());
    }
    
    public boolean hasTasksBlocked() {
        return (this.blockedTasks.isEmpty() == false);
    }
    
    public boolean hasTasksReleased() {
        return (this.blockedTasksReleased);
    }
    
    @Override
    public String toString() {
        if (this.isInitialized() == false) {
            return ("<UNINITIALIZED>");
        }
        
        String status = null;
        if (this.results_ctr == this.partitions.cardinality()) {
            if (this.blockedTasksReleased == false) {
                status = "READY";
            } else {
                status = "RELEASED";
            }
        } else if (this.blockedTasks.isEmpty()) {
            status = "WAITING";
        } else {
            status = "BLOCKED";
        }
        
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("  Hash Code", this.hashCode());
        m.put("  Internal", this.internal);
        m.put("  Partitions", this.partitions);
        
        Map<String, Object> inner = new ListOrderedMap<String, Object>();
        for (int partition = 0, cnt = this.results.size(); partition < cnt; partition++) {
            if (this.results.get(partition) == null) continue;
            VoltTable vt = this.results.get(partition);
            inner.put(String.format("Partition %02d",partition), String.format("{%d tuples}", vt.getRowCount()));  
        } // FOR
        m.put("  Results", inner);
        m.put("  Blocked", this.blockedTasks);
        m.put("  Status", status);

        return String.format("DependencyInfo[#%d]\n%s", this.dependency_id, StringUtil.formatMaps(m).trim());
    }

}