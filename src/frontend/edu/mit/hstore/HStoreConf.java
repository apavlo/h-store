package edu.mit.hstore;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;

import edu.brown.catalog.CatalogUtil;
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
     * Max size of queued transactions before we stop accepting new requests and throttle clients
     */
    public int txn_queue_max_per_partition = 2500;
    public double txn_queue_release_factor = 0.25;
    public final int txn_queue_max;
    public final int txn_queue_release;  
    
    /**
     * Whether to enable speculative execution of single-partition transactions
     */
    public boolean enable_speculative_execution = true;
    
    /**
     * If this is set to true, TransactionEstimator will try to reuse MarkovPathEstimators
     * for transactions running at the same partition.
     */
    public boolean markov_path_caching = true;

    /**
     * This threshold defines how accurate our cached MarkovPathEstimators have to be in order to keep using them.
     * If (# of accurate txs / total txns) for a paritucular MarkovGraph goes below this threshold, then we will disable the caching   
     */
    public double markov_path_caching_threshold = 1.0;
    
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
    public boolean force_singlepartitioned = true;
    
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
     * The scale factor to apply to the object pool values
     */
    public double pool_scale_factor = Double.valueOf(System.getProperty("hstore.preload", "1.0"));
    
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
     * The number of LocalTransactionState objects to preload
     * @see LocalTransactionState.Factory
     */
    public int pool_localtxnstate_preload = 500;
    
    /**
     * The max number of LocalTransactionStates to keep in the pool (per ExecutionSite)
     * @see LocalTransactionState.Factory
     */
    public int pool_localtxnstate_idle = 1000;
    
    /**
     * The number of RemoteTransactionState objects to preload
     * @see RemoteTransactionState.Factory
     */
    public int pool_remotetxnstate_preload = 500;
    
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
    
    
    public int pool_preload_dependency_infos = 10000;
    
    // ----------------------------------------------------------------------------
    // METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    private HStoreConf(ArgumentsParser args, Site catalog_site) {
        int num_partitions = 1;
        int local_partitions = 1;
        
        if (args != null) {
            
            // Total number of partitions
            num_partitions = CatalogUtil.getNumberOfPartitions(args.catalog);
            
            // Partitions at this site
            local_partitions = catalog_site.getPartitions().size();
            
            // Ignore the Dtxn.Coordinator
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN)) {
                this.ignore_dtxn = args.getBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN);
                if (this.ignore_dtxn) LOG.info("Ignoring the Dtxn.Coordinator for all single-partition transactions");
            }
            // Enable speculative execution
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION)) {
                this.enable_speculative_execution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION);
                if (this.enable_speculative_execution) LOG.info("Enabling speculative execution");
            }
            // Enable DB2-style txn redirecting
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_DB2_REDIRECTS)) {
                this.enable_db2_redirects = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_DB2_REDIRECTS);
                if (this.enable_db2_redirects) LOG.info("Enabling DB2-style transaction redirects");
            }
            // Force all transactions to be single-partitioned
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION)) {
                this.force_singlepartitioned = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION);
                if (this.force_singlepartitioned) LOG.info("Forcing all transactions to execute as single-partitioned");
            }
            // Force all transactions to be executed at the first partition that the request arrives on
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION)) {
                this.force_localexecution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION);
                if (this.force_localexecution) LOG.info("Forcing all transactions to execute at the partition they arrive on");
            }
            // Enable the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT)) {
                this.force_neworder_hack = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT);
                if (this.force_neworder_hack) LOG.info("Enabling the inspection of incoming neworder parameters");
            }
            // Enable setting the done partitions for the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE)) {
                this.force_neworder_hack_done = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE);
                if (this.force_neworder_hack_done) LOG.info("Enabling the setting of done partitions for neworder inspection");
            }
            // Clean-up Interval
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL)) {
                this.helper_interval = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL);
                LOG.debug("Setting Cleanup Interval = " + this.helper_interval + "ms");
            }
            // Txn Expiration Time
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE)) {
                this.helper_txn_expire = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE);
                LOG.debug("Setting Cleanup Txn Expiration = " + this.helper_txn_expire + "ms");
            }
            // Profiling
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING)) {
                this.enable_profiling = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING);
                if (this.enable_profiling) LOG.info("Enabling procedure profiling");
            }
            // Mispredict Crash
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH)) {
                this.mispredict_crash = args.getBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH);
                if (this.mispredict_crash) LOG.info("Enabling crashing HStoreSite on mispredict");
            }
        }
        
        // Compute Parameters
        this.txn_queue_max = Math.round(local_partitions * this.txn_queue_max_per_partition);
        this.txn_queue_release = Math.max((int)(this.txn_queue_max * this.txn_queue_release_factor), 1);
        
    }
    
    private static HStoreConf conf;
    
    public synchronized static HStoreConf init(ArgumentsParser args, Site catalog_site) {
        if (conf != null) throw new RuntimeException("Trying to initialize HStoreConf more than once");
        conf = new HStoreConf(args, catalog_site);
        return (conf);
    }
    
    public synchronized static HStoreConf singleton() {
        if (conf == null) throw new RuntimeException("Requesting HStoreConf before it is initialized");
        return (conf);
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
