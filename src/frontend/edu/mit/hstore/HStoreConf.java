package edu.mit.hstore;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;

import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.StringUtil;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);

    // ----------------------------------------------------------------------------
    // HStoreSite
    // ----------------------------------------------------------------------------
    
    /**
     * Max size of queued transactions before we stop accepting new requests
     */
    public int txn_queue_max = 1000;
    public int txn_queue_release = (int)(txn_queue_max * 0.25);  
    
    /**
     * Whether to enable speculative execution of single-partition transactions
     */
    public boolean enable_speculative_execution = true;
    
    /**
     * Whether to not use the Dtxn.Coordinator for single-partition transactions
     */
    public boolean ignore_dtxn = true;

    /**
     * Whether to use DB2-style transaction redirecting
     * When this is enabled, all txns will always start executing on a random
     * partition at the node where the request was originally sent. Then when it executes a query,
     * it will be aborted/restarted and redirected to the correct partition.
     */
    public boolean enable_db2_redirects = false;
    
    /**
     * Whether to force all transactions to be executed as single-partitioned
     */
    public boolean force_singlepartitioned = false;
    
    /**
     * Whether all transactions should execute at the local HStoreSite (i.e., they are never redirected)
     */
    public boolean force_localexecution = false;
    
    /**
     * Assume all txns are TPC-C neworder and look directly at the parameters to figure out
     * whether it is single-partitioned or not
     * @see HStoreSite.procedureInvocation() 
     */
    public boolean force_neworder_hack = false;
    
    /**
     * If this is set to true, allow the HStoreSite to set the done partitions for multi-partition txns
     * @see HStoreSite.procedureInvocation()
     */
    public boolean force_neworder_hack_done = true;
    
    /**
     * Enable txn profiling
     */
    public boolean enable_profiling = false;

    /**
     * Whether the VoltProcedure should crash the HStoreSite on a mispredict
     */
    public boolean mispredict_crash = false;
    
    // ----------------------------------------------------------------------------
    // ExecutionSiteHelper
    // ----------------------------------------------------------------------------

    /**
     * How many ms to wait initially before starting the ExecutionSiteHelper
     */
    public int helper_initial_delay = 2000;
    
    /**
     * How many ms to wait before the ExecutionSiteHelper executes again to clean up txns
     */
    public int helper_interval = 1000;
    
    /**
     * How many txns can the ExecutionSiteHelper clean-up per Partition per Round
     * Any value less than zero means that it will clean-up all txns it can per round
     */
    public int helper_txn_per_round = -1;
    
    /**
     * How long should the ExecutionSiteHelper wait before cleaning up a txn's state
     */
    public int helper_txn_expire = 1000;
    
    // ----------------------------------------------------------------------------
    // OBJECT POOLS
    // ----------------------------------------------------------------------------
    
    /**
     * Whether to track the number of objects created, passivated, and destroyed from the pool
     * @see CountingPoolableObjectFactory
     */
    public boolean pool_enable_tracking = false;
    
    /**
     * The max number of VoltProcedure instances to keep in the pool (per ExecutionSite + per Procedure)
     * @see ExecutionSite.VoltProcedureFactory 
     */
    public int pool_voltprocedure_idle = 10000;
    
    /**
     * The max number of BatchPlans to keep in the pool (per BatchPlanner)
     * @see BatchPlanner.BatchPlanFactory
     */
    public int pool_batchplan_idle = 2000;
    
    /**
     * The max number of LocalTransactionStates to keep in the pool (per ExecutionSite)
     * @see LocalTransactionState.Factory
     */
    public int pool_localtxnstate_idle = 1000;
    
    /**
     * The max number of RemoteTransactionStates to keep in the pool (per ExecutionSite)
     * @see RemoteTransactionState.Factory
     */
    public int pool_remotetxnstate_idle = 500;
    
    /**
     * The max number of MarkovPathEstimators to keep in the pool (global)
     * @see MarkovPathEstimator.Factory
     */
    public int pool_pathestimators_idle = 1000;
    
    /**
     * The max number of TransactionEstimator.States to keep in the pool (global)
     * Should be the same as the number of MarkovPathEstimators
     * @see TransactionEstimator.State.Factory
     */
    public int pool_estimatorstates_idle = 1000;
    
    /**
     * The max number of DependencyInfos to keep in the pool (global)
     * Should be the same as the number of MarkovPathEstimators
     * @see DependencyInfo.State.Factory
     */
    public int pool_dependencyinfos_idle = 50000;
    
    // ----------------------------------------------------------------------------
    // METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    private HStoreConf() {
        
    }
    
    private static HStoreConf conf;
    
    public synchronized static HStoreConf init(ArgumentsParser args) {
        if (conf != null) return (conf);
        conf = new HStoreConf();
        
        if (args != null) {
            // Ignore the Dtxn.Coordinator
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN)) {
                conf.ignore_dtxn = args.getBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN);
                if (conf.ignore_dtxn) LOG.info("Ignoring the Dtxn.Coordinator for all single-partition transactions");
            }
            // Enable speculative execution
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION)) {
                conf.enable_speculative_execution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION);
                if (conf.enable_speculative_execution) LOG.info("Enabling speculative execution");
            }
            // Enable DB2-style txn redirecting
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_DB2_REDIRECTS)) {
                conf.enable_db2_redirects = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_DB2_REDIRECTS);
                if (conf.enable_db2_redirects) LOG.info("Enabling DB2-style transaction redirects");
            }
            // Force all transactions to be single-partitioned
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION)) {
                conf.force_singlepartitioned = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION);
                if (conf.force_singlepartitioned) LOG.info("Forcing all transactions to execute as single-partitioned");
            }
            // Force all transactions to be executed at the first partition that the request arrives on
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION)) {
                conf.force_localexecution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION);
                if (conf.force_localexecution) LOG.info("Forcing all transactions to execute at the partition they arrive on");
            }
            // Enable the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT)) {
                conf.force_neworder_hack = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT);
                if (conf.force_neworder_hack) LOG.info("Enabling the inspection of incoming neworder parameters");
            }
            // Enable setting the done partitions for the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE)) {
                conf.force_neworder_hack_done = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE);
                if (conf.force_neworder_hack_done) LOG.info("Enabling the setting of done partitions for neworder inspection");
            }
            // Clean-up Interval
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL)) {
                conf.helper_interval = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL);
                LOG.debug("Setting Cleanup Interval = " + conf.helper_interval + "ms");
            }
            // Txn Expiration Time
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE)) {
                conf.helper_txn_expire = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE);
                LOG.debug("Setting Cleanup Txn Expiration = " + conf.helper_txn_expire + "ms");
            }
            // Profiling
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING)) {
                conf.enable_profiling = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING);
                if (conf.enable_profiling) LOG.info("Enabling procedure profiling");
            }
            // Mispredict Crash
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH)) {
                conf.mispredict_crash = args.getBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH);
                if (conf.mispredict_crash) LOG.info("Enabling crashing HStoreSite on mispredict");
            }
        }
        return (conf);
    }
    
    public static HStoreConf singleton() {
        return (HStoreConf.init(null));
    }
    
    @Override
    public String toString() {
        Class<?> confClass = this.getClass();
        final Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : confClass.getFields()) {
            String key = f.getName().toUpperCase();
            try {
                m.put(key, f.get(this));
            } catch (IllegalAccessException ex) {
                m.put(key, ex.getMessage());
            }
        }
        return (StringUtil.formatMaps(m));
    }
}
