package edu.mit.hstore.dtxn;

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
    
    // ---------------------------------------------------------------
    // GLOBAL METHODS
    // ---------------------------------------------------------------
    
    /**
     * Total time spent executing the transaction
     * This time starts from when the txn first arrives in HStoreSite.procedureInvocation()
     * until it is completely removed in HStoreSite.completeTranasction()
     */
    private final ProfileMeasurement pm_total = new ProfileMeasurement("TOTAL");
    
    public void startTransaction(long timestamp) {
        if (this.disabled) return;
        if (debug.get()) LOG.debug(String.format("START %s -> %s", this.pm_total.getType(), this.pm_init_total.getType()));
        this.pm_total.start(timestamp);
        this.pm_init_total.start(timestamp);
        this.stack.push(this.pm_total);
        this.stack.push(this.pm_init_total);
    }
    public synchronized void stopTransaction() {
        if (this.disabled) return;
        this.disabled = true;
        long timestamp = ProfileMeasurement.getTime();
        while (this.stack.isEmpty() == false) {
            ProfileMeasurement pm = this.stack.pop();
            assert(pm != null);
            if (debug.get()) LOG.debug("STOP " + pm.getType());
            assert(pm.isStarted()) : pm.toString(true);
            pm.stop(timestamp);
            assert(pm.isStopped()) : pm.toString(true);
        } // WHILE
        assert(this.stack.isEmpty());
        assert(this.isStopped());
    }
    
    private void startInner(ProfileMeasurement parent, ProfileMeasurement inner) {
        if (debug.get()) LOG.debug(String.format("Start PARENT[%s] -> INNER[%s]", parent, inner));
        assert(this.stack.size() > 0);
        assert(this.stack.peek() == parent) : String.format("Unexpected state %s: PARENT[%s] -> INNER[%s]\n%s",
                                                            this.stack.peek(), parent.getType(), inner.getType(), this.stack);
        inner.start();
        this.stack.push(inner);
    }
    private void stopInner(ProfileMeasurement parent, ProfileMeasurement inner) {
        if (debug.get()) LOG.debug(String.format("Stop PARENT[%s] <- INNER[%s]", parent, inner));
        assert(this.stack.size() > 0);
        ProfileMeasurement pm = this.stack.pop();
        assert(pm == inner) : String.format("Unexpected state %s: PARENT[%s] <- INNER[%s]\n%s",
                                            pm, parent.getType(), inner.getType(), this.stack);
        assert(parent == this.stack.peek()) : String.format("Unexpected outer state PARENT[%s] <- INNER[%s]", parent.getType(), inner.getType());
        pm.stop();
    }
    
    public void startCoordinatorBlocked() {
        if (this.disabled) return;
        ProfileMeasurement parent = this.stack.peek();
        ProfileMeasurement inner = null;
        if (parent == this.pm_init_total) {
            inner = this.pm_init_dtxn;
        } else if (parent == this.pm_exec_total) {
            inner = this.pm_exec_dtxn;
        } else if (parent == this.pm_finish_total) {
            inner = this.pm_finish_dtxn;
        } else {
            assert(false) : "Unexpected parent state " + parent;    
        }
        this.startInner(parent, inner);
    }
    public void stopCoordinatorBlocked() {
        if (this.disabled) return;
        ProfileMeasurement parent = null;
        ProfileMeasurement inner = this.stack.peek();
        if (inner == this.pm_init_dtxn) {
            parent = this.pm_init_total;
        } else if (inner == this.pm_exec_dtxn) {
            parent = this.pm_exec_total;
        } else if (inner == this.pm_finish_dtxn) {
            parent = this.pm_finish_total;
        } else {
            assert(false) : "Unexpected inner state " + inner;    
        }
        this.stopInner(parent, inner);
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
     * 
     */
    private final ProfileMeasurement pm_init_dtxn = new ProfileMeasurement("INIT_DTXN");
    
    /**
     * 
     */
    public void startInitEstimation() {
        if (this.disabled) return;
        if (debug.get()) LOG.debug("START " + this.pm_init_est.getType());
        assert(this.stack.size() == 0);
        this.pm_init_est.start();
        this.stack.push(this.pm_init_est);
    }
    public void stopInitEstimation() {
        if (this.disabled) return;
        if (debug.get()) LOG.debug("STOP " + this.pm_init_est.getType());
        assert(this.stack.size() > 0);
        ProfileMeasurement orig = this.stack.pop();
        assert(orig == this.pm_init_est) : this.stack.toString();
        orig.stop();
    }
    
    // ---------------------------------------------------------------
    // QUEUE METHODS
    // ---------------------------------------------------------------
    
    /**
     * Time spent waiting in queue
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
     * Time spent blocked on the initialization latch
     */
    private final ProfileMeasurement pm_exec_dtxn = new ProfileMeasurement("EXEC_DTXN");
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
    public void startExecEE() {
        if (this.disabled) return;
        this.startInner(this.pm_exec_total, this.pm_exec_ee);
    }
    public void stopExecEE() {
        if (this.disabled) return;
        this.stopInner(this.pm_exec_total, this.pm_exec_ee);
    }

    // ---------------------------------------------------------------
    // FINISH TIMES
    // ---------------------------------------------------------------

    /**
     * Time spent getting the response back to the client
     */
    private final ProfileMeasurement pm_finish_total = new ProfileMeasurement("FINISH");
    /**
     * 
     */
    private final ProfileMeasurement pm_finish_dtxn = new ProfileMeasurement("FINISH_DTXN");
    
    /**
     * 
     */
    public void startFinish() {
        if (this.disabled) return;
        assert(this.stack.size() > 0);
        long timestamp = ProfileMeasurement.getTime();
        
        // The transaction may have aborted in different states, so we need to 
        // pop things off the stack until we reach the outer execution time
        ProfileMeasurement pm = null;
        while (this.stack.isEmpty() == false) {
            pm = this.stack.pop();
            assert(pm != null);
            if (pm == this.pm_exec_total) break;
            if (pm.isStarted()) pm.stop(timestamp);
        } // WHILE
        assert(pm != null);
        assert(pm.isStarted());
        ProfileMeasurement.swap(timestamp, pm, this.pm_finish_total);
        this.stack.push(this.pm_finish_total);
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
            if (i == 0) assert(tuple[i] > 0) : "Missing data " + pm.toString(true); 
        } // FOR
        return (tuple);
    }
    
    protected Map<String, Object> debugMap() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : PROFILE_FIELDS) {
            Object val = null;
            try {
                val = f.get(this);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            m.put(StringUtil.title(f.getName().replace("_", " ")), val); 
        } // FOR
        return (m);
    }
}