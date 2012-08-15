package edu.brown.profilers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.StringUtil;

public class TransactionProfiler extends AbstractProfiler implements Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionProfiler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ---------------------------------------------------------------
    // INTERNAL STATE
    // ---------------------------------------------------------------

    /**
     * 
     */
    @SuppressWarnings("serial")
    private final Stack<ProfileMeasurement> stack = new Stack<ProfileMeasurement>() {
        public ProfileMeasurement push(ProfileMeasurement item) {
            TransactionProfiler.this.history.add(item);
            return super.push(item);
        }
    };
    
    private final List<ProfileMeasurement> history = new ArrayList<ProfileMeasurement>();
    
    private transient boolean disabled = false;
    
    /**
     * 
     * @param expected_parent - The expected parent
     * @param next
     */
    private void startInner(ProfileMeasurement expected_parent, ProfileMeasurement next) {
        if (debug.get()) LOG.debug(String.format("Start PARENT[%s] -> NEXT[%s]", expected_parent, next));
        assert(this.stack.size() > 0);
        assert(this.stack.peek() == expected_parent) :
            String.format("Unexpected state %s: PARENT[%s] -> NEXT[%s]\n%s",
                          this.stack.peek(), expected_parent.getType(), next.getType(),
                          StringUtil.join("\n", this.stack));
        next.start();
        this.stack.push(next);
    }
    private void stopInner(ProfileMeasurement expected_current, ProfileMeasurement expected_next) {
        if (debug.get()) LOG.debug(String.format("Stop PARENT[%s] <- CURRENT[%s]", expected_next, expected_current));
        assert(this.stack.size() > 0);
        ProfileMeasurement pm = this.stack.pop();
        assert(pm == expected_current) :
            String.format("Expected current state %s but was %s! [expectedParent=%s]\n%s",
                          expected_current, pm, expected_next, this.stack);
        assert(expected_next == this.stack.peek()) :
            String.format("Expected current parent %s but was %s! [inner=%s]",
                          expected_next, this.stack.peek(), expected_current);
        pm.stop();
    }
    
//    private void startGlobal(ProfileMeasurement global_pm) {
//        assert(this.stack.size() > 0);
//        ProfileMeasurement parent = this.stack.peek();
//        ProfileMeasurement.swap(parent, global_pm);
//        this.stack.push(global_pm);
//    }
//    
//    private void stopGlobal(ProfileMeasurement global_pm) {
//        assert(this.stack.size() > 0);
//        ProfileMeasurement pop = this.stack.pop();
//        assert(global_pm == pop);
//        ProfileMeasurement.swap(global_pm, this.stack.peek());
//    }
    
    // ---------------------------------------------------------------
    // GLOBAL METHODS
    // ---------------------------------------------------------------
    
    /**
     * Total time spent executing the transaction
     * This time starts from when the txn first arrives in HStoreSite.procedureInvocation()
     * until it is completely removed in HStoreSite.completeTranasction()
     */
    protected final ProfileMeasurement pm_total = new ProfileMeasurement("TOTAL");
    
    protected final ProfileMeasurement pm_serialize = new ProfileMeasurement("SERIALIZE");
    protected final ProfileMeasurement pm_deserialize = new ProfileMeasurement("DESERIALIZE");
    
    
    public void startTransaction(long timestamp) {
        if (this.disabled) return;
        if (debug.get()) LOG.debug(String.format("START %s -> %s", this.pm_total.getType(), this.pm_init_total.getType()));
        this.pm_total.start(timestamp);
        this.pm_init_total.start(timestamp);
        this.stack.push(this.pm_total);
        this.stack.push(this.pm_init_total);
    }
    public void stopTransaction() {
        if (this.disabled) return;
        this.disabled = true;
        long timestamp = ProfileMeasurement.getTime();
        while (this.stack.isEmpty() == false) {
            ProfileMeasurement pm = this.stack.pop();
            assert(pm != null);
            if (debug.get()) LOG.debug("STOP " + pm.getType());
            assert(pm.isStarted()) : pm.debug(true);
            pm.stop(timestamp);
            assert(pm.isStarted() == false) : pm.debug(true);
        } // WHILE
        assert(this.stack.isEmpty());
        assert(this.isStopped());
    }
    
    public void startSerialization() {
        if (this.disabled) return;
        this.pm_serialize.start();
//        this.startGlobal(this.pm_serialize);
    }
    public void stopSerialization() {
        if (this.disabled) return;
        this.pm_serialize.stop();
//        this.stopGlobal(this.pm_serialize);
    }
    
    public void startDeserialization() {
        if (this.disabled) return;
        this.pm_deserialize.start();
//        this.startGlobal(this.pm_deserialize);
    }
    public void stopDeserialization() {
        if (this.disabled) return;
        this.pm_deserialize.stop();
//        this.stopGlobal(this.pm_deserialize);
    }
    
    // ---------------------------------------------------------------
    // INITIALIZATION METHODS
    // ---------------------------------------------------------------
    
    /**
     * The time spent setting up the transaction before it is queued in either
     * an ExecutionSite or with the Dtxn.Coordinator
     */
    protected final ProfileMeasurement pm_init_total = new ProfileMeasurement("INIT_TOTAL");
    /**
     * The amount of time spent estimating what the transaction will do in the initialization
     */
    protected final ProfileMeasurement pm_init_est = new ProfileMeasurement("INIT_EST");
    /**
     * Time spent waiting in the DTXN queue
     */
    protected final ProfileMeasurement pm_init_dtxn = new ProfileMeasurement("INIT_DTXN");
    
    /**
     * 
     */
    public void startInitEstimation() {
        if (this.disabled) return;
        this.startInner(this.pm_init_total, this.pm_init_est);
    }
    public void stopInitEstimation() {
        if (this.disabled) return;
        this.stopInner(this.pm_init_est, this.pm_init_total);
    }
    
    public void startInitDtxn() {
        if (this.disabled) return;
        this.startInner(this.pm_init_total, this.pm_init_dtxn);
    }
    public void stopInitDtxn() {
        if (this.disabled) return;
        this.stopInner(this.pm_init_dtxn, this.pm_init_total);
    }
    
    // ---------------------------------------------------------------
    // QUEUE METHODS
    // ---------------------------------------------------------------
    
    /**
     * Time spent waiting in the PartitionExecutor queue
     */
    protected final ProfileMeasurement pm_queue = new ProfileMeasurement("QUEUE");
    
    public void startQueue() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        assert(this.stack.peek() != this.pm_queue) : "Duplicate calls for " + this.pm_queue;
        long timestamp = ProfileMeasurement.getTime();
        
        // We can either be put in an PartitionExecutor queue directly in HStoreSite
        // or after we get a response from the coordinator
        ProfileMeasurement pm = null;
        while (this.stack.isEmpty() == false) {
            pm = this.stack.pop();
            assert(pm != null);
            if (debug.get()) LOG.debug("STOP " + pm.getType());
            if (pm == this.pm_init_total) break;
            pm.stop(timestamp);
        } // WHILE
        
        if (debug.get()) LOG.debug("START " + this.pm_queue.getType());
        ProfileMeasurement.swap(timestamp, pm, this.pm_queue);
        this.stack.push(this.pm_queue);
    }
    
    // ---------------------------------------------------------------
    // EXECUTION TIMES
    // ---------------------------------------------------------------
    
    /**
     * The total time spent executing the transaction
     * This starts when the transaction is removed from the ExecutionSite's queue
     * until it finishes
     */
    protected final ProfileMeasurement pm_exec_total = new ProfileMeasurement("EXEC_TOTAL");
    /**
     * The amount of time spent executing the Java-portion of the stored procedure
     */
    protected final ProfileMeasurement pm_exec_java = new ProfileMeasurement("EXEC_JAVA");
    /**
     * Time spent blocked waiting for a TransactionWorkResponse to come back
     */
    protected final ProfileMeasurement pm_exec_dtxn_work = new ProfileMeasurement("EXEC_DTXN_WORK");
    /**
     * The amount of time spent planning the transaction
     */
    protected final ProfileMeasurement pm_exec_planner = new ProfileMeasurement("EXEC_PLANNER");
    /**
     * The amount of time spent executing in the plan fragments
     */
    protected final ProfileMeasurement pm_exec_ee = new ProfileMeasurement("EXEC_EE");
    /**
     * The amount of time spent estimating what the transaction will do
     */
    protected final ProfileMeasurement pm_exec_est = new ProfileMeasurement("EXEC_EST");
    
    /**
     * Invoked when the txn has been removed from the queue and is
     * starting to execute at a local ExecutionSite 
     */
    public void startExec() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        ProfileMeasurement current = this.stack.pop();
        assert(current != this.pm_exec_total) :
            "Trying to start txn execution twice!";
        assert(current == this.pm_queue) :
            "Trying to start execution before txn was queued (" + current + ")";
        ProfileMeasurement.swap(current, this.pm_exec_total);
        this.stack.push(this.pm_exec_total);
    }
    
    public void startExecJava() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_java);
    }
    public void stopExecJava() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_java, this.pm_exec_total);
    }
    public void startExecPlanning() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_planner);
    }
    public void stopExecPlanning() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_planner, this.pm_exec_total);
    }
    
    public void startExecEstimation() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_est);
    }
    public void stopExecEstimation() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_est, this.pm_exec_total);
    }
    
    public void startExecDtxnWork() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_dtxn_work);
    }
    public void stopExecDtxnWork() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_dtxn_work, this.pm_exec_total);
    }
    
    public void startExecEE() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_ee);
    }
    public void stopExecEE() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_ee, this.pm_exec_total);
    }

    // ---------------------------------------------------------------
    // CLEAN-UP TIMES
    // ---------------------------------------------------------------

    /**
     * Time spent getting the response back to the client
     */
    protected final ProfileMeasurement pm_post_total = new ProfileMeasurement("POST_TOTAL");
    /**
     * 2PC-PREPARE
     */
    protected final ProfileMeasurement pm_post_prepare = new ProfileMeasurement("POST_PREPARE");
    /**
     * 2PC-FINISH
     */
    protected final ProfileMeasurement pm_post_finish = new ProfileMeasurement("POST_FINISH");
    /**
     * The amount of time spent commiting or aborting a txn in the EE
     */
    protected final ProfileMeasurement pm_post_ee = new ProfileMeasurement("POST_EE");

    /**
     * Indicate that the txn is the post-processing stage. This should only
     * be called after startExec() has been invoked
     */
    public void startPost() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        ProfileMeasurement current = null;
        while ((current = this.stack.pop()) != this.pm_exec_total) {
            // Keep this ball rollin'
            current.stop();
            if (trace.get()) LOG.trace("-> STOPPED: " + current + "[" + current.hashCode() + "]");
        } // WHILE
        assert(current == this.pm_exec_total) : "Unexpected " + current;
        if (trace.get()) LOG.trace("STATUS: " + current.debug(true) + "[" + current.hashCode() + "]");
        if (current.isStarted() == false) {
            this.pm_post_total.start();
        } else {
            ProfileMeasurement.swap(current, this.pm_post_total);
        }
        this.stack.push(this.pm_post_total);
    }
    
    public void startPostPrepare() {
        if (this.disabled) return;
        this.startInner(this.pm_post_total, this.pm_post_prepare);
    }
    public void stopPostPrepare() {
        if (this.disabled) return;
        this.stopInner(this.pm_post_prepare, this.pm_post_total);
    }
    
    public void startPostFinish() {
        if (this.disabled) return;
        this.startInner(this.pm_post_total, this.pm_post_finish);
    }
    public void stopPostFinish() {
        if (this.disabled) return;
        this.stopInner(this.pm_post_finish, this.pm_post_total);
    }
    
    public void startPostEE() {
        if (this.disabled) return;
        // Need to figure out whether we are in POST_FINISH or not
        ProfileMeasurement parent = this.stack.peek();
        this.startInner(parent, this.pm_post_ee);
    }
    public void stopPostEE() {
        if (this.disabled) return;
        ProfileMeasurement parent = this.stack.elementAt(this.stack.size() - 2);
        this.stopInner(this.pm_post_ee, parent);
    }
    

    // ---------------------------------------------------------------
    // UTILITY METHODS
    // ---------------------------------------------------------------
    
    @Override
    public void copy(AbstractProfiler other) {
        assert(other instanceof TransactionProfiler);
        super.copy(other);
        
        // Stop anything that is already started
        long timestamp = -1;
        for (ProfileMeasurement pm : this.getProfileMeasurements()) {
            if (pm.isStarted() && pm != this.pm_total && pm != this.pm_exec_total) {
                if (timestamp == -1) timestamp = ProfileMeasurement.getTime();
                pm.stop(timestamp);
            }
        } // FOR
        this.pm_total.reset();
        this.pm_exec_total.reset();
        this.startTransaction(((TransactionProfiler)other).pm_total.getMarker());
    }
    
    @Override
    public void finish() {
        for (ProfileMeasurement pm : this.getProfileMeasurements()) {
            pm.clear();
        } // FOR
        this.stack.clear();
        this.history.clear();
        this.disabled = false;
    }

    /**
     * Disable all profiling for this transaction
     */
    public void disableProfiling() {
        if (debug.get()) LOG.debug("Disabling transaction profiling");
        this.disabled = true;
    }
    /**
     * Enable profiling for this transaction
     * This should only be invoked before the txn starts
     */
    public void enableProfiling() {
        if (debug.get()) LOG.debug("Enabling transaction profiling");
        this.disabled = false;
    }
    
    /**
     * Return the topmost ProfileMeasurement handle on this profiler's stack
     */
    public ProfileMeasurement current() {
        return (this.stack.peek());
    }
    /**
     * Returns the history of actions for this profiler
     */
    public Collection<ProfileMeasurement> history() {
        return (Collections.unmodifiableCollection(this.history));
    }
    
    public boolean isDisabled() {
        return (this.disabled);
    }
    
    public boolean isStopped() {
        return (this.pm_total.isStarted() == false);
    }
    
    @Override
    public boolean isInitialized() {
        return true;
    }
    
    @Override
    public Map<String, Object> debugMap() {
        Map<String, Object> m = super.debugMap();
        
        // HISTORY
        String history = "";
        int i = 0; 
        for (ProfileMeasurement pm : this.history) {
            String label = pm.getType();
            if (pm.isStarted()) {
                label += " *ACTIVE*";
            }
            history += String.format("[%02d] %s\n", i++, label);
        } // FOR
        m.put("History", history);
        
        return (m);
    }
}