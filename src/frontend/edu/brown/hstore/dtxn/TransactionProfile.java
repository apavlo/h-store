package edu.brown.hstore.dtxn;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.Poolable;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;

public class TransactionProfile implements Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionProfile.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ---------------------------------------------------------------
    // DYNAMIC FIELD HANDLES
    // ---------------------------------------------------------------

    public static final Field PROFILE_FIELDS[];
    static {
        // Get all of the ProfileMeasurement fields
        Class<TransactionProfile> tsClass = TransactionProfile.class;
        List<Field> fields = new ArrayList<Field>();
        for (Field f : tsClass.getDeclaredFields()) {
            if (f.getType().equals(ProfileMeasurement.class)) {
                fields.add(f);
            }
        } // FOR
        assert(fields.isEmpty() == false);
        PROFILE_FIELDS = new Field[fields.size()];
        for (int i = 0; i < PROFILE_FIELDS.length; i++) {
            PROFILE_FIELDS[i] = fields.get(i);
        }
    } // STATIC
    
    // ---------------------------------------------------------------
    // INTERNAL STATE
    // ---------------------------------------------------------------

    /**
     * 
     */
    private final Stack<ProfileMeasurement> stack = new Stack<ProfileMeasurement>();
    
    private transient boolean disabled = false;
    
    /**
     * 
     * @param parent - The expected parent
     * @param inner
     */
    private void startInner(ProfileMeasurement parent, ProfileMeasurement inner) {
        if (debug.get()) LOG.debug(String.format("Start PARENT[%s] -> INNER[%s]", parent, inner));
        assert(this.stack.size() > 0);
        assert(this.stack.peek() == parent) : String.format("Unexpected state %s: PARENT[%s] -> INNER[%s]\n%s",
                                                            this.stack.peek(), parent.getType(), inner.getType(), StringUtil.join("\n", this.stack));
        inner.start();
        this.stack.push(inner);
    }
    private void stopInner(ProfileMeasurement expected_parent, ProfileMeasurement inner) {
        if (debug.get()) LOG.debug(String.format("Stop PARENT[%s] <- INNER[%s]", expected_parent, inner));
        assert(this.stack.size() > 0);
        ProfileMeasurement pm = this.stack.pop();
        assert(pm == inner) :
            String.format("Expected current state %s but was %s! [expectedParent=%s]\n%s",
                          inner, pm, expected_parent, this.stack);
        assert(expected_parent == this.stack.peek()) :
            String.format("Expected current parent %s but was %s! [inner=%s]",
                          expected_parent, this.stack.peek(), inner);
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
    private final ProfileMeasurement pm_total = new ProfileMeasurement("TOTAL");
    
    private final ProfileMeasurement pm_serialize = new ProfileMeasurement("SERIALIZE");
    private final ProfileMeasurement pm_deserialize = new ProfileMeasurement("DESERIALIZE");
    
    
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
            assert(pm.isStopped()) : pm.debug(true);
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
    private final ProfileMeasurement pm_init_total = new ProfileMeasurement("INIT");
    /**
     * The amount of time spent estimating what the transaction will do in the initialization
     */
    private final ProfileMeasurement pm_init_est = new ProfileMeasurement("INIT_EST");
    /**
     * Time spent waiting in the DTXN queue
     */
    private final ProfileMeasurement pm_init_dtxn = new ProfileMeasurement("INIT_DTXN");
    
    /**
     * 
     */
    public void startInitEstimation() {
        if (this.disabled) return;
        this.startInner(this.pm_init_est, this.pm_init_est);
    }
    public void stopInitEstimation() {
        if (this.disabled) return;
        this.stopInner(this.pm_init_total, this.pm_init_est);
    }
    
    public void startInitDtxn() {
        if (this.disabled) return;
        this.startInner(this.pm_init_total, this.pm_init_dtxn);
    }
    public void stopInitDtxn() {
        if (this.disabled) return;
        this.stopInner(this.pm_init_total, this.pm_init_dtxn);
    }
    
    // ---------------------------------------------------------------
    // QUEUE METHODS
    // ---------------------------------------------------------------
    
    /**
     * Time spent waiting in the ExecutionSite queue
     */
    private final ProfileMeasurement pm_queue = new ProfileMeasurement("QUEUE");
    
    public void startQueue() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        assert(this.stack.peek() != this.pm_queue) : "Duplicate calls for " + this.pm_queue;
        long timestamp = ProfileMeasurement.getTime();
        
        // We can either be put in an ExecutionSite queue directly in HStoreSite
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
    private final ProfileMeasurement pm_exec_total = new ProfileMeasurement("EXEC");
    /**
     * The amount of time spent executing the Java-portion of the stored procedure
     */
    private final ProfileMeasurement pm_exec_java = new ProfileMeasurement("EXEC_JAVA");
    /**
     * Time spent blocked waiting for a TransactionWorkResponse to come back
     */
    private final ProfileMeasurement pm_exec_dtxn_work = new ProfileMeasurement("EXEC_DTXN_WORK");
    /**
     * The amount of time spent planning the transaction
     */
    private final ProfileMeasurement pm_exec_planner = new ProfileMeasurement("EXEC_PLANNER");
    /**
     * The amount of time spent executing in the plan fragments
     */
    private final ProfileMeasurement pm_exec_ee = new ProfileMeasurement("EXEC_EE");
    /**
     * The amount of time spent estimating what the transaction will do
     */
    private final ProfileMeasurement pm_exec_est = new ProfileMeasurement("EXEC_EST");
    
    /**
     * Invoked when the txn has been removed from the queue and is
     * starting to execute at a local ExecutionSite 
     */
    public void startExec() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        assert(this.stack.peek() != this.pm_exec_total);
        ProfileMeasurement current = this.stack.pop();
        assert(current == this.pm_queue);
        ProfileMeasurement.swap(current, this.pm_exec_total);
        this.stack.push(this.pm_exec_total);
    }
    
    public void startExecJava() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_java);
    }
    public void stopExecJava() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_total, this.pm_exec_java);
    }
    public void startExecPlanning() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_planner);
    }
    public void stopExecPlanning() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_total, this.pm_exec_planner);
    }
    
    public void startExecEstimation() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_est);
    }
    public void stopExecEstimation() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_total, this.pm_exec_est);
    }
    
    public void startExecDtxnWork() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_dtxn_work);
    }
    public void stopExecDtxnWork() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_total, this.pm_exec_dtxn_work);
    }
    
    public void startExecEE() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_ee);
    }
    public void stopExecEE() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_total, this.pm_exec_ee);
    }

    // ---------------------------------------------------------------
    // CLEAN-UP TIMES
    // ---------------------------------------------------------------

    /**
     * Time spent getting the response back to the client
     */
    private final ProfileMeasurement pm_post_total = new ProfileMeasurement("POST");
    /**
     * 2PC-PREPARE
     */
    private final ProfileMeasurement pm_post_prepare = new ProfileMeasurement("POST_PREPARE");
    /**
     * 2PC-FINISH
     */
    private final ProfileMeasurement pm_post_finish = new ProfileMeasurement("POST_FINISH");
    /**
     * The amount of time spent commiting or aborting a txn in the EE
     */
    private final ProfileMeasurement pm_post_ee = new ProfileMeasurement("POST_EE");

    /**
     * 
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
        if (current.isStopped()) {
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
        this.stopInner(this.pm_post_total, this.pm_post_prepare);
    }
    
    public void startPostFinish() {
        if (this.disabled) return;
        this.startInner(this.pm_post_total, this.pm_post_finish);
    }
    public void stopPostFinish() {
        if (this.disabled) return;
        this.stopInner(this.pm_post_total, this.pm_post_finish);
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
        this.stopInner(parent, this.pm_post_ee);
    }
    

    // ---------------------------------------------------------------
    // UTILITY METHODS
    // ---------------------------------------------------------------
    
    @Override
    public void finish() {
        for (Field f : PROFILE_FIELDS) {
            try {
                ((ProfileMeasurement)f.get(this)).clear();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } // FOR
        this.stack.clear();
        this.disabled = false;
    }

    /**
     * Disable all profiling for this transaction
     */
    public void disableProfiling() {
        if (debug.get()) LOG.debug("Disabling transaction profiling");
        this.disabled = true;
    }
    
    public boolean isDisabled() {
        return (this.disabled);
    }
    
    public boolean isStopped() {
        return (this.pm_total.isStopped());
    }
    
    @Override
    public boolean isInitialized() {
        return true;
    }
    
    public long[] getTuple() {
        long tuple[] = new long[PROFILE_FIELDS.length];
        for (int i = 0; i < tuple.length; i++) {
            Field f = PROFILE_FIELDS[i];
            ProfileMeasurement pm = null;
            try {
                pm = (ProfileMeasurement)f.get(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            tuple[i] = pm.getTotalThinkTime();
            if (i == 0) assert(tuple[i] > 0) : "Missing data " + pm.debug(true); 
        } // FOR
        return (tuple);
    }
    
    protected Map<String, Object> debugMap() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : PROFILE_FIELDS) {
            ProfileMeasurement val = null;
            try {
                val = (ProfileMeasurement)f.get(this);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            m.put(StringUtil.title(f.getName().replace("_", " ")), val.debug(true)); 
        } // FOR
        return (m);
    }
}