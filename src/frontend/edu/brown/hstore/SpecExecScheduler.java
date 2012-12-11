package edu.brown.hstore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.SpecExecSchedulerPolicyType;
import org.voltdb.types.SpeculationType;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.specexec.AbstractConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.SpecExecProfiler;

/**
 * Special scheduler that can figure out what the next best single-partition
 * to speculatively execute at a partition based on the current distributed transaction 
 * @author pavlo
 */
public class SpecExecScheduler implements Loggable {
    private static final Logger LOG = Logger.getLogger(SpecExecScheduler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private static boolean d;
    private static boolean t;
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
        d = debug.get();
        t = trace.get();
    }
    
    private final int partitionId;
    private final TransactionInitPriorityQueue work_queue;
    private AbstractConflictChecker checker;
    private SpecExecSchedulerPolicyType policyType;
    private int window_size = 1;
    
    /** Ignore all LocalTransaction handles **/
    private boolean ignore_all_local = false;
    
    /** Don't reset the iterator if the queue size changes **/
    private boolean ignore_queue_size_change = false;
    
    private AbstractTransaction lastDtxn;
    private SpeculationType lastSpecType;
    private Iterator<AbstractTransaction> lastIterator;
    private int lastSize = 0;

    private final Map<SpeculationType, SpecExecProfiler> profilerMap = new HashMap<SpeculationType, SpecExecProfiler>();
    private boolean profiling = false;
    
    /**
     * Constructor
     * @param catalogContext
     * @param checker TODO
     * @param partitionId
     * @param work_queue
     */
    public SpecExecScheduler(AbstractConflictChecker checker,
                             int partitionId, TransactionInitPriorityQueue work_queue,
                             SpecExecSchedulerPolicyType schedule_policy, int window_size) {
        assert(schedule_policy != null) : "Unsupported schedule policy parameter passed in";
        
        this.partitionId = partitionId;
        this.work_queue = work_queue;
        this.checker = checker;
        this.policyType = schedule_policy;
        this.window_size = window_size;
        
        this.profiling = HStoreConf.singleton().site.specexec_profiling;
        if (this.profiling) {
            for (SpeculationType type: SpeculationType.values()) {
                this.profilerMap.put(type, new SpecExecProfiler());
            } // FOR
        }
    }
    
    /**
     * Replace the ConflictChecker. This should only be used for testing
     * @param checker
     */
    protected void setConflictChecker(AbstractConflictChecker checker) {
        LOG.warn(String.format("Replacing original checker %s with %s",
                 this.checker.getClass().getSimpleName(),
                 checker.getClass().getCanonicalName()));
        this.checker = checker;
    }
    protected void setIgnoreAllLocal(boolean ignore_all_local) {
        this.ignore_all_local = ignore_all_local;
    }
    protected void setIgnoreQueueSizeChanges(boolean ignore_queue_changes) {
        this.ignore_queue_size_change = ignore_queue_changes;
    }
    protected void setWindowSize(int window) {
        this.window_size = window;
    }
    protected void setPolicyType(SpecExecSchedulerPolicyType policy) {
        this.policyType = policy;
    }
    protected void reset() {
        this.lastIterator = null;
    }

    public boolean shouldIgnoreProcedure(Procedure catalog_proc) {
        return (this.checker.shouldIgnoreProcedure(catalog_proc));
    }
    
    /**
     * Find the next non-conflicting txn that we can speculatively execute.
     * Note that if we find one, it will be immediately removed from the queue
     * and returned. If you do this and then find out for some reason that you
     * can't execute the StartTxnMessage that is returned, you must be sure
     * to requeue it back.
     * @param dtxn The current distributed txn at this partition.
     * @return
     */
    public LocalTransaction next(AbstractTransaction dtxn, SpeculationType specType) {
        assert(dtxn != null) : "Null distributed transaction"; 
        assert(this.checker.shouldIgnoreProcedure(dtxn.getProcedure()) == false) :
            String.format("Trying to check for speculative txns for %s but the txn should have been ignored");
        
        SpecExecProfiler profiler = null;
        if (this.profiling) {
            profiler = profilerMap.get(specType);
            profiler.total_time.start();
        }
        
        if (d) {
            LOG.debug(String.format("%s - Checking queue for transaction to speculatively execute " +
        		      "[specType=%s, queueSize=%d, policy=%s]",
                      dtxn, specType, this.work_queue.size(), this.policyType));
            if (t) LOG.trace(String.format("%s - Last Invocation [lastDtxn=%s, lastSpecType=%s, lastIterator=%s]",
                             dtxn, this.lastDtxn, this.lastSpecType, this.lastIterator));
        }
        
        // If this is a LocalTransaction and all of the remote partitions that it needs are
        // on the same site, then we won't bother with trying to pick something out
        // because there is going to be very small wait times.
        if (this.ignore_all_local && dtxn instanceof LocalTransaction && ((LocalTransaction)dtxn).isPredictAllLocal()) {
            if (d) LOG.debug(String.format("%s - Ignoring current distributed txn because all of the partitions that " +
                             "it is using are on the same HStoreSite [%s]", dtxn, dtxn.getProcedure()));
            if (this.profiling) profiler.total_time.stop();
            return (null);
        }
        
        // Now peek in the queue looking for single-partition txns that do not
        // conflict with the current dtxn
        LocalTransaction next = null;
        int txn_ctr = 0;
        int examined_ctr = 0;
        long best_time = (this.policyType == SpecExecSchedulerPolicyType.LONGEST ? Long.MIN_VALUE : Long.MAX_VALUE);

        // Check whether we can use our same iterator from the last call
        if (this.policyType != SpecExecSchedulerPolicyType.FIRST ||
                this.lastDtxn != dtxn ||
                this.lastSpecType != specType ||
                this.lastIterator == null ||
                (this.ignore_queue_size_change == false && this.lastSize != this.work_queue.size())) {
            this.lastIterator = this.work_queue.iterator();    
        }
        if (this.profiling) profiler.queue_size.put(this.work_queue.size());
        while (this.lastIterator.hasNext()) {
            AbstractTransaction txn = this.lastIterator.next();
            assert(txn != null) : "Null transaction handle " + txn;
            boolean singlePartition = txn.isPredictSinglePartition();
            txn_ctr++;

            // Skip any distributed or non-local transactions
            if ((txn instanceof LocalTransaction) == false || singlePartition == false) {
                if (t) LOG.trace(String.format("%s - Skipping non-speculative candidate %s", dtxn, txn));
                continue;
            }
            LocalTransaction localTxn = (LocalTransaction)txn;
            
            // Skip anything already speculatively executed
            if (localTxn.isSpeculative()) {
                if (t) LOG.trace(String.format("%s - Skipping %s because it was already executed", dtxn, txn));
                continue;
            }

            // Let's check it out!
            if (this.profiling) profiler.compute_time.start();
            if (d) LOG.debug(String.format("Examining whether %s conflicts with current dtxn %s", localTxn, dtxn));
            if (singlePartition == false) {
                if (t) LOG.trace(String.format("%s - Skipping %s because it is not single-partitioned", dtxn, localTxn));
                continue;
            }
            try {
                if (this.checker.canExecute(dtxn, localTxn, this.partitionId)) {
                    if (next == null) {
                        next = localTxn;
                        // Scheduling Policy: FIRST MATCH
                        if (this.policyType == SpecExecSchedulerPolicyType.FIRST) {
                            break;
                        }
                    }
                    // Scheduling Policy: Estimated Time Remaining
                    else {
                        EstimatorState es = localTxn.getEstimatorState();
                        if (es != null) {
                            long remaining = es.getLastEstimate().getRemainingExecutionTime();
                            if ((this.policyType == SpecExecSchedulerPolicyType.SHORTEST && remaining < best_time) ||
                                (this.policyType == SpecExecSchedulerPolicyType.LONGEST && remaining > best_time)) {
                                best_time = remaining;
                                next = localTxn;
                                if (d) LOG.debug(String.format("[%s schedule %d] New Match -> %s / remaining=%d",
                                                 this.policyType, this.window_size, next, remaining));
                             }
                        }
                    }
                    // Stop if we've reached our window size
                    if (++examined_ctr == this.window_size) break;
                }
            } finally {
                if (this.profiling) profiler.compute_time.stop();
            }
        } // WHILE
        if (this.profiling) {
            profiler.num_comparisons.put(txn_ctr);
        }
        
        // We found somebody to execute right now!
        // Make sure that we set the speculative flag to true!
        if (next != null) {
            if (this.profiling) profiler.success++;
            this.lastIterator.remove();
            this.work_queue.clear(next);
            if (d) LOG.debug(dtxn + " - Found next non-conflicting speculative txn " + next);
        }
        else if (d && this.work_queue.isEmpty() == false) {
            LOG.debug(String.format("%s - Failed to find non-conflicting speculative txn [txnCtr=%d, examinedCtr=%d]",
                      dtxn, txn_ctr, examined_ctr));
        }
        
        this.lastDtxn = dtxn;
        this.lastSpecType = specType;
        if (this.ignore_queue_size_change == false) this.lastSize = this.work_queue.size();
        if (this.profiling) profiler.total_time.stop();
        return (next);
    }
    
    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    public class Debug implements DebugContext {
        public AbstractTransaction getLastDtxn() {
            return (lastDtxn);
        }
        public int getLastSize() {
            return (lastSize);
        }
        public Iterator<AbstractTransaction> getLastIterator() {
            return (lastIterator);
        }
        public SpeculationType getLastSpecType() {
            return (lastSpecType);
        }
        public Map<SpeculationType,SpecExecProfiler> getProfilers() {
            return (profilerMap);
        }
        public SpecExecProfiler getProfiler(SpeculationType stype) {
            return (profilerMap.get(stype));
        }
        
    } // CLASS
    
    private SpecExecScheduler.Debug cachedDebugContext;
    public SpecExecScheduler.Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new SpecExecScheduler.Debug();
        }
        return this.cachedDebugContext;
    }
}
