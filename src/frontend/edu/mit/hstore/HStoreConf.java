package edu.mit.hstore;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;
import org.voltdb.catalog.Site;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.MarkovPathEstimator;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.RemoteTransactionState;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);

    // ============================================================================
    // GLOBAL
    // ============================================================================
    public final class GlobalConf {
        
        /**
         * Temporary directory used to store various artifacts
         */
        public String temp_dir = "/tmp/hstore";

        /**
         * Options used when logging into client/server hosts
         * We assume that there will be no spaces in paths or options listed here
         */
        public String sshoptions = "-x";

        /**
         * 
         */
        public String defaulthost = "localhost";
    }
    
    // ============================================================================
    // SITE
    // ============================================================================
    public final class SiteConf {
    
        /**
         * Site Log Directory
         */
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/sites";

        // ----------------------------------------------------------------------------
        // Execution Options
        // ----------------------------------------------------------------------------
        
        /**
         * Whether to enable speculative execution of single-partition transactions
         */
        public boolean exec_speculative_execution = true;
        
        /**
         * Whether the HStoreSite will try to avoid using the Dtxn.Coordinator for single-partition transactions
         */
        public boolean exec_avoid_coordinator = true;
        
        /**
         * Whether to use DB2-style transaction redirecting
         * When this is enabled, all txns will always start executing on a random
         * partition at the node where the request was originally sent. Then when it executes a query,
         * it will be aborted/restarted and redirected to the correct partition.
         */
        public boolean exec_db2_redirects = false;
        
        /**
         * Whether to force all transactions to be executed as single-partitioned
         */
        public boolean exec_force_singlepartitioned = true;
        
        /**
         * Whether all transactions should execute at the local HStoreSite (i.e., they are never redirected)
         */
        public boolean exec_force_localexecution = false;
        
        /**
         * Assume all txns are TPC-C neworder and look directly at the parameters to figure out
         * whether it is single-partitioned or not
         * @see HStoreSite.procedureInvocation() 
         */
        public boolean exec_neworder_cheat = false;
        
        /**
         * If this is set to true, allow the HStoreSite to set the done partitions for multi-partition txns
         * @see HStoreSite.procedureInvocation()
         */
        public boolean exec_neworder_cheat_done_partitions = true;
        
        /**
         * Enable txn profiling
         */
        public boolean exec_txn_profiling = false;
    
        /**
         * Whether the VoltProcedure should crash the HStoreSite on a mispredict
         */
        public boolean exec_mispredict_crash = false;
        
        /**
         * If this enabled, HStoreSite will use a separate thread to process every outbound ClientResponse
         * for all of the ExecutionSites.
         */
        public final boolean exec_postprocessing_thread = true; 
        
        /**
         * TODO
         */
        public final boolean exec_queued_response_ee_bypass = true;
        
        // ----------------------------------------------------------------------------
        // Incoming Transaction Queue Options
        // ----------------------------------------------------------------------------
        
        /**
         * Max size of queued transactions before we stop accepting new requests and throttle clients
         */
        public int txn_queue_max_per_partition = 1000;
        
        /**
         * TODO
         */
        public double txn_queue_release_factor = 0.25;
        
        /**
         * TODO
         */
        public int txn_queue_max;
        
        /**
         * TODO
         */
        public int txn_queue_release;  
        
        // ----------------------------------------------------------------------------
        // Markov Transaction Estimator Options
        // ----------------------------------------------------------------------------
        
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
        // HSTORESITE STATUS UPDATES
        // ----------------------------------------------------------------------------
        
        /**
         * Enable HStoreSite's StatusThread (# of seconds to print update)
         * Set this to be -1 if you want to disable the status messages 
         */
        public int status_interval = 20;

        /**
         * Allow the HStoreSite StatusThread to kill the cluster if it looks hung 
         */
        public boolean status_kill_if_hung = true;
        
        // ----------------------------------------------------------------------------
        // OBJECT POOLS
        // ----------------------------------------------------------------------------
        
        /**
         * The scale factor to apply to the object pool values
         */
        public double pool_scale_factor = 1.0d;
        
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
        
        /**
         * TODO
         */
        public int pool_preload_dependency_infos = 10000;
    }

    // ============================================================================
    // COORDINATOR
    // ============================================================================
    public final class CoordinatorConf {
        
        /**
         * Coordinator Host
         */
        public String host = HStoreConf.this.global.defaulthost;
        
        /**
         * Coordinator Port
         */
        public int port = 12348;

        /**
         * How long should we wait before starting the dtxn coordinator (in milliseconds) 
         */
        public int delay = 10000;

        /**
         * Coordinator Log Directory 
         */
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/coordinator";
        
    }
    
    // ============================================================================
    // CLIENT
    // ============================================================================
    public final class ClientConf {
        /**
         * The amount of memory to allocate for each client process (in MB)
         */
        public int memory = 512;

        /**
         * Default client host name
         */
        public String host = HStoreConf.this.global.defaulthost;

        /**
         * The number of txns that each client submits (per ms)
         * Actual TXN rate sent to cluster will be:
         * TXNRATE * CLIENTCOUNT * PROCESSESPERCLIENT
         */
        public int txnrate = 10000;

        /**
         * Number of processes to use per client
         */
        public int processesperclient = 1;

        /**
         * Number of clients
         */
        public int count = 1;

        /**
         * How long should the client run (in ms)
         */
        public int duration = 60000;

        /**
         * How long should the system be allowed to warmup?
         * Any stats collected during this period are not counted.
         */
        public int warmup = 0;

        /**
         * Polling interval (ms)
         */
        public int interval = 10000;

        /**
         * Whether to use the BlockingClient
         */
        public boolean blocking = false;

        /**
         * Scale Factor
         */
        public int scalefactor = 10;

        /**
         * Skew Factor
         */
        public double skewfactor = 0.0;

        /**
         * Temporal Skew
         * Define the number of partitions used in a skew block
         */
        public int temporalwindow = 0;
        public int temporaltotal = 100;

        /**
         * Client Log Directory
         */
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/clients";

        /**
         * If this enabled, then each DBMS will dump their entire database contents into
         * CSV files after executing a benchmark run
         */
        public boolean dump_database = false;
        public String dump_database_dir = HStoreConf.this.global.temp_dir + "/dumps";
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL 
    // ----------------------------------------------------------------------------
    
    private PropertiesConfiguration config = null;

    public final GlobalConf global = new GlobalConf();
    public final SiteConf site = new SiteConf();
    public final CoordinatorConf coordinator = new CoordinatorConf();
    public final ClientConf client = new ClientConf();
    
    private final Map<String, Object> confHandles = new ListOrderedMap<String, Object>();
    {
        confHandles.put("global", this.global);
        confHandles.put("site", this.site);
        confHandles.put("coordinator", this.coordinator);
        confHandles.put("client", this.client);    
    }
    
    // ----------------------------------------------------------------------------
    // METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    protected void loadFromFile(File path) throws Exception {
        this.config = new PropertiesConfiguration(path);

        Pattern p = Pattern.compile("(global|site|coordinator|client)\\.(.*)");
        for (Object obj_k : CollectionUtil.wrapIterator(this.config.getKeys())) {
            String k = obj_k.toString();
            Matcher m = p.matcher(k);
            boolean found = m.matches();
            assert(m != null && found) : "Invalid key '" + k + "' from configuration file '" + path + "'";
            
            Object handle = confHandles.get(m.group(1));
            Class<?> confClass = handle.getClass();
            assert(confClass != null);
            Field f = confClass.getField(m.group(2));
            Class<?> f_class = f.getType();
            Object value = null;
            
            if (f_class.equals(int.class)) {
                value = this.config.getInt(k);
            } else if (f_class.equals(long.class)) {
                value = this.config.getLong(k);
            } else if (f_class.equals(double.class)) {
                value = this.config.getDouble(k);
            } else if (f_class.equals(boolean.class)) {
                value = this.config.getBoolean(k);
            }
            
            f.set(handle, value);
        } // FOR
    }
    
    /**
     * 
     */
    public String makeDefaultConfig() {
        StringBuilder sb = new StringBuilder();
        for (String group : this.confHandles.keySet()) {
            Object handle = this.confHandles.get(group);
            Class<?> handleClass = handle.getClass();

            sb.append("## ").append(StringUtil.repeat("-", 100)).append("\n")
              .append("## ").append(StringUtil.title(group)).append(" Parameters\n")
              .append("## ").append(StringUtil.repeat("-", 100)).append("\n\n");
            
            for (Field f : handleClass.getFields()) {
                String key = String.format("%s.%s", group, f.getName());
                Object val = null;
                try {
                    val = f.get(handle);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to get " + key, ex);
                }
                if (val instanceof String) {
                    String str = (String)val;
                    if (str.startsWith(global.temp_dir)) {
                        val = str.replace(global.temp_dir, "${global.temp_dir}");
                    } else if (str.equals(global.defaulthost)) {
                        val = str.replace(global.defaulthost, "${global.defaulthost}");
                    }
                }
                
                sb.append(String.format("%-50s= %s\n", key, val));
            } // FOR
            sb.append("\n");
        } // FOR
        return (sb.toString());
    }
    
    protected void computeDerivedValues() {
        
    }
    
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
                site.exec_avoid_coordinator = args.getBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN);
                if (site.exec_avoid_coordinator) LOG.info("Ignoring the Dtxn.Coordinator for all single-partition transactions");
            }
            // Enable speculative execution
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION)) {
                site.exec_speculative_execution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION);
                if (site.exec_speculative_execution) LOG.info("Enabling speculative execution");
            }
            // Enable DB2-style txn redirecting
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_DB2_REDIRECTS)) {
                site.exec_db2_redirects = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_DB2_REDIRECTS);
                if (site.exec_db2_redirects) LOG.info("Enabling DB2-style transaction redirects");
            }
            // Force all transactions to be single-partitioned
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION)) {
                site.exec_force_singlepartitioned = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION);
                if (site.exec_force_singlepartitioned) LOG.info("Forcing all transactions to execute as single-partitioned");
            }
            // Force all transactions to be executed at the first partition that the request arrives on
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION)) {
                site.exec_force_localexecution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION);
                if (site.exec_force_localexecution) LOG.info("Forcing all transactions to execute at the partition they arrive on");
            }
            // Enable the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT)) {
                site.exec_neworder_cheat = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT);
                if (site.exec_neworder_cheat) LOG.info("Enabling the inspection of incoming neworder parameters");
            }
            // Enable setting the done partitions for the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE)) {
                site.exec_neworder_cheat_done_partitions = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE);
                if (site.exec_neworder_cheat_done_partitions) LOG.info("Enabling the setting of done partitions for neworder inspection");
            }
            // Clean-up Interval
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL)) {
                site.helper_interval = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL);
                LOG.debug("Setting Cleanup Interval = " + site.helper_interval + "ms");
            }
            // Txn Expiration Time
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE)) {
                site.helper_txn_expire = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE);
                LOG.debug("Setting Cleanup Txn Expiration = " + site.helper_txn_expire + "ms");
            }
            // Profiling
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING)) {
                site.exec_txn_profiling = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING);
                if (site.exec_txn_profiling) LOG.info("Enabling procedure profiling");
            }
            // Mispredict Crash
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH)) {
                site.exec_mispredict_crash = args.getBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH);
                if (site.exec_mispredict_crash) LOG.info("Enabling crashing HStoreSite on mispredict");
            }
        }
        
        // Compute Parameters
        site.txn_queue_max = Math.round(local_partitions * site.txn_queue_max_per_partition);
        site.txn_queue_release = Math.max((int)(site.txn_queue_max * site.txn_queue_release_factor), 1);
        
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
        final Map<String, Object> m = new TreeMap<String, Object>();
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
