package edu.brown.hstore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.SpeculationType;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.StartTxnMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.hstore.specexec.AbstractConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.SpecExecProfiler;

/**
 * Special scheduler that can figure out what the next best single-partition
 * to speculatively execute at a partition based on the current distributed transaction 
 * @author pavlo
 */
public class SpecExecScheduler {
    private static final Logger LOG = Logger.getLogger(SpecExecScheduler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final CatalogContext catalogContext;
    private final int partitionId;
    private final List<InternalMessage> work_queue;
    private final AbstractConflictChecker checker;
    private boolean ignore_all_local = false;
    private final Map<SpeculationType, SpecExecProfiler> profilerMap = new HashMap<SpeculationType, SpecExecProfiler>();
    private SchedulerPolicy policy;
    private int window_size = 1;
    private boolean isProfiling = false;
    
    public static enum SchedulerPolicy {
    	  FIRST,
    	  SHORTEST,
    	  LONGEST;
    }
    public static HashMap <String,SchedulerPolicy> policyMap = new HashMap<String, SchedulerPolicy>();
    static {
    	policyMap.put("FIRST", SchedulerPolicy.FIRST);
    	policyMap.put("SHORTEST", SchedulerPolicy.SHORTEST);
    	policyMap.put("LONGEST", SchedulerPolicy.LONGEST);
    }
    /**
     * Constructor
     * @param catalogContext
     * @param checker TODO
     * @param partitionId
     * @param work_queue
     */
    public SpecExecScheduler(CatalogContext catalogContext, AbstractConflictChecker checker, int partitionId, 
    		List<InternalMessage> work_queue, SchedulerPolicy schedule_policy, int windown) {
        this.partitionId = partitionId;
        this.work_queue = work_queue;
        this.catalogContext = catalogContext;
        this.checker = checker;
        assert (schedule_policy != null) : "Unsupported schedule policy parameter passed in";
        this.policy = schedule_policy;
        this.window_size = windown;
        
        if (HStoreConf.singleton().site.specexec_profiling) {
            this.isProfiling = true;
            for (SpeculationType type: SpeculationType.getNameMap().values()) {
            	this.profilerMap.put(type, new SpecExecProfiler());
            }
        } else {
            this.isProfiling = false; // do not need, but for explicit reason
        }
    }
    
    public void setIgnoreAllLocal(boolean ignore_all_local) {
        this.ignore_all_local = ignore_all_local;
    }
    
    public void setWindowSize(int window) {
    	this.window_size = window;
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
    public StartTxnMessage next(AbstractTransaction dtxn, SpeculationType specType) {
    	SpecExecProfiler profiler = null;
    	if (this.isProfiling) {
        	profiler = profilerMap.get(specType);
        	profiler.total_time.start();
        }
        
        if (trace.get()) LOG.trace(String.format("%s - Checking queue for transaction to speculatively execute [queueSize=%d]",
                                   dtxn, this.work_queue.size()));
        
        Procedure dtxnProc = dtxn.getProcedure();
        if (dtxnProc == null || this.checker.ignoreProcedure(dtxnProc)) {
            if (debug.get())
                LOG.debug(String.format("%s - Ignoring current distributed txn because no conflict information exists", dtxn));
            if (this.isProfiling) {
            	profiler.total_time.stop();
            }
            return (null);
        }
        
        // If this is a LocalTransaction and all of the remote partitions that it needs are
        // on the same site, then we won't bother with trying to pick something out
        // because there is going to be very small wait times.
        if (this.ignore_all_local && dtxn instanceof LocalTransaction && ((LocalTransaction)dtxn).isPredictAllLocal()) {
            if (debug.get())
                LOG.debug(String.format("%s - Ignoring current distributed txn because all of the partitions that " +
                		  "it is using are on the same HStoreSite [%s]", dtxn, dtxnProc));
            if (this.isProfiling) {
            	profiler.total_time.stop();
            }
            return (null);
        }
        
        // Now peek in the queue looking for single-partition txns that do not
        // conflict with the current dtxn
        StartTxnMessage next = null;
        Iterator<InternalMessage> it = this.work_queue.iterator();
        int msg_ctr = 0;
        int size_ctr = 0;
        StartTxnMessage best_next = null;
        long best_time = Long.MAX_VALUE;
        if (policy == SchedulerPolicy.LONGEST)
        	best_time = Long.MIN_VALUE;
        
        if (this.isProfiling) {
        	profiler.queue_size.put(this.work_queue.size());
        }
        while (it.hasNext()) {
            InternalMessage msg = it.next();
            msg_ctr++;

            // Any WorkFragmentMessage has to be for our current dtxn,
            // so we want to never speculative execute stuff because we will
            // always want to immediately execute that
            if (msg instanceof WorkFragmentMessage) {
                if (debug.get())
                    LOG.debug(String.format("%s - Not choosing a txn to speculatively execute because there " +
                    		  "are still WorkFragments in the queue", dtxn));
                break;
            }
            // A StartTxnMessage will have a fully initialized LocalTransaction handle
            // that we can examine and execute right away if necessary
            else if (msg instanceof StartTxnMessage) {
                StartTxnMessage txn_msg = (StartTxnMessage)msg;
                LocalTransaction ts = txn_msg.getTransaction();
                if (debug.get())
                    LOG.debug(String.format("Examining whether %s conflicts with current dtxn %s", ts, dtxn));
                if (ts.isPredictSinglePartition() == false) {
                    if (trace.get())
                        LOG.trace(String.format("%s - Skipping %s because it is not single-partitioned", dtxn, ts));
                    continue;
                }
                if (this.isProfiling) {
                	profiler.compute_time.start();
                }
                try {
                    if (this.checker.canExecute(dtxn, ts, this.partitionId)) {
                        if (best_next == null)
                        	best_next = txn_msg;
                    	if (this.policy == SchedulerPolicy.FIRST) {
                        	next = txn_msg;
                        	//LOG.info("[FIRST schedule]");
                        } else if (this.policy == SchedulerPolicy.SHORTEST) {
                        	EstimatorState es = ts.getEstimatorState();
                        	if (es != null) {
                        		long tmp = es.getLastEstimate().getRemainingExecutionTime();
                             	if (best_time > tmp) {
                             		best_time = tmp;
                             		best_next = txn_msg;
                             	}
                             	LOG.info(String.format("[SHORTEST schedule %d] time for current txn: %d, SHORTESTS time up to now: %ld", size_ctr, tmp, best_time));
                            }
                        	if (es != null && ++size_ctr < this.window_size)
                            	continue;
                        	else {
                        		next = best_next;
                        		LOG.info(String.format("[SHORTEST schedule ] SHORTESTS time %d", best_time));
                        	}
                        } else if (this.policy == SchedulerPolicy.LONGEST) {
                        	EstimatorState es = ts.getEstimatorState();
                        	if (es != null) {
                        		long tmp = es.getLastEstimate().getRemainingExecutionTime();
                             	if (best_time < tmp) {
                             		best_time = tmp;
                             		best_next = txn_msg;
                             	}
                             	LOG.info(String.format("[LONGEST schedule %d] time for current txn: %d, LONGEST time up to now: %d", size_ctr, tmp, best_time));
                            }
                        	if (es != null && ++size_ctr < this.window_size)
                            	continue;
                        	else {
                        		next = best_next;
                        		LOG.info(String.format("[LONGEST schedule ] LONGEST time %d", best_time));
                        	}
                        }
                        
                        break;
                    }
                } finally {
                    if (this.isProfiling) {
                    	profiler.compute_time.stop();
                    }
                }
            }
        } // WHILE
        if (this.isProfiling) {
        	profiler.num_comparisons.put(msg_ctr);
        }
        
        // We found somebody to execute right now!
        // Make sure that we set the speculative flag to true!
        if (next != null) {
            if (this.isProfiling) {
            	profiler.success++;
            }
            it.remove();
            if (debug.get()) 
                LOG.debug(dtxn + " - Found next non-conflicting speculative txn " + next);
        }
        
        if (this.isProfiling) { 
        	profiler.total_time.stop();
        }
        return (next);
    }
    
    public Map<SpeculationType,SpecExecProfiler> getProfilers() {
        //return (this.profiler);
    	return (this.profilerMap);
    }
}
