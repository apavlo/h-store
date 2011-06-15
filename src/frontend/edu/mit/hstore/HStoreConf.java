package edu.mit.hstore;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;
import org.voltdb.catalog.Site;

import edu.brown.markov.MarkovPathEstimator;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.RemoteTransactionState;
import edu.mit.hstore.interfaces.ConfigProperty;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);

    /**
     * Base Configuration Class
     */
    private abstract class Conf {
        
        final Map<Field, ConfigProperty> properties;
        final String prefix;
        
        {
            Class<?> confClass = this.getClass();
            this.prefix = confClass.getSimpleName().replace("Conf", "").toLowerCase();
            HStoreConf.this.confHandles.put(this.prefix, this);
            
            this.properties =  ClassUtil.getFieldAnnotations(confClass.getFields(), ConfigProperty.class);
            this.setDefaultValues();
        }
        
        private void setDefaultValues() {
            // Set the default values for the parameters based on their annotations
            for (Entry<Field, ConfigProperty> e : this.properties.entrySet()) {
                Field f = e.getKey();
                ConfigProperty cp = e.getValue();
                Class<?> f_class = f.getType();
                Object value = null;
                
                if (cp.computed()) continue;
                
                if (f_class.equals(int.class)) {
                    value = e.getValue().defaultInt();
                } else if (f_class.equals(long.class)) {
                    value = e.getValue().defaultLong();
                } else if (f_class.equals(double.class)) {
                    value = e.getValue().defaultDouble();
                } else if (f_class.equals(boolean.class)) {
                    value = e.getValue().defaultBoolean();
                } else if (f_class.equals(String.class)) {
                    value = e.getValue().defaultString();
                }
                
                try {
                    f.set(this, value);
                } catch (Exception ex) {
                    throw new RuntimeException(String.format("Failed to set default value '%s' for field '%s'", value, f.getName()), ex);
                }
//                System.err.println(String.format("%-20s = %s", f.getName(), value));
            } // FOR   
        }
        
        @Override
        public String toString() {
            final Map<String, Object> m = new TreeMap<String, Object>();
            for (Entry<Field, ConfigProperty> e : this.properties.entrySet()) {
                Field f = e.getKey();
                ConfigProperty cp = e.getValue();
                String key = f.getName().toUpperCase();
                
                if (cp.advanced() == false) {
                    try {
                        m.put(key, f.get(this));
                    } catch (IllegalAccessException ex) {
                        m.put(key, ex.getMessage());
                    }
                }
            }
            return (StringUtil.formatMaps(m));
        }
        
    }
    
    // ============================================================================
    // GLOBAL
    // ============================================================================
    public final class GlobalConf extends Conf {
        
        @ConfigProperty(
            description="Temporary directory used to store various artifacts",
            defaultString="/tmp/hstore"
        )
        public String temp_dir = "/tmp/hstore";

        @ConfigProperty(
            description="Options used when logging into client/server hosts. " + 
                        "We assume that there will be no spaces in paths or options listed here.",
            defaultString="-x"
        )
        public String sshoptions;

        @ConfigProperty(
            description="The default hostname used when generating cluster configurations.",
            defaultString="localhost"
        )
        public String defaulthost = "localhost";
    }
    
    // ============================================================================
    // SITE
    // ============================================================================
    public final class SiteConf extends Conf {
    
        @ConfigProperty(
            description="Site log directory",
            defaultString="${global.temp_dir}/logs/sites",
            advanced=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/sites";

        // ----------------------------------------------------------------------------
        // Execution Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If this feature is enabled, then each HStoreSite will attempt to speculatively execute " +
                        "single-partition transactions whenever it completes a work request for a multi-partition " +
                        "transaction running on a different node.",
            defaultBoolean=true,
            advanced=false,
            experimental=true
        )
        public boolean exec_speculative_execution;
        
        @ConfigProperty(
            description="If this parameter is set to true, then each HStoreSite will not send every transaction request " +
                        "through the Dtxn.Coordinator. Only multi-partition transactions will be sent to the " +
                        "Dtxn.Coordinator (in order to ensure global ordering). Setting this property to true provides a " +
                        "major throughput improvement.",
            defaultBoolean=true
        )
        public boolean exec_avoid_coordinator;
        
        @ConfigProperty(
            description="If this feature is true, then H-Store will use DB2-style transaction redirects. Each request will " +
                        "execute as a single-partition transaction at a random partition on the node that the request " +
                        "originally arrives on. When the transaction makes a query request that needs to touch data from " +
                        "a partition that is different than its base partition, then that transaction is immediately aborted, " +
                        "rolled back, and restarted on the partition that has the data that it was requesting. If the " +
                        "transaction requested more than partition when it was aborted, then it will be executed as a " +
                        "multi-partition transaction on the partition that was requested most often by queries " +
                        "(using random tie breakers).",
            defaultBoolean=false,
            advanced=true,
            experimental=true
        )
        public boolean exec_db2_redirects;
        
        @ConfigProperty(
            description="Always execute transactions as single-partitioned (excluding sysprocs). If a transaction requests " +
                        "data on a partition that is different than where it is executing, then it is aborted, rolled back, " +
                        "and re-executed on the same partition as a multi-partition transaction that touches all partitions. " +
                        "Note that this is independent of how H-Store decides what partition to execute the transaction’s Java " +
                        "control code on.",
            defaultBoolean=true
        )
        public boolean exec_force_singlepartitioned;
        
        @ConfigProperty(
            description="Always execute each transaction on a random partition on the node where the request originally " +
                        "arrived on. Note that this is independent of whether the transaction is selected to be " +
                        "single-partitioned or not. It is likely that you do not want to use this option.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean exec_force_localexecution;
        
        @ConfigProperty(
            description="Enable a hack for TPC-C where we inspect the arguments of the TPC-C neworder transaction and figure " +
                        "out what partitions it needs without having to use the TransactionEstimator. This will crash the " +
                        "system when used with other benchmarks.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean exec_neworder_cheat;
        
        @ConfigProperty(
            description="Used in conjunction with ${site.force_neworderinspect} to figure out when TPC-C NewOrder transactions " +
                        "are finished with partitions. This will crash the system when used with other benchmarks.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean exec_neworder_cheat_done_partitions;
        
        @ConfigProperty(
            description="Enable txn profiling.",
            defaultBoolean=false,
            advanced=true,
            experimental=true
        )
        public boolean exec_txn_profiling;
    
        @ConfigProperty(
            description="Whether the VoltProcedure should crash the HStoreSite on a mispredict.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean exec_mispredict_crash;
        
        @ConfigProperty(
            description="If this enabled, HStoreSite will use a separate thread to process every outbound ClientResponse for " +
                        "all of the ExecutionSites.",
            defaultBoolean=false,
            advanced=false
        )
        public boolean exec_postprocessing_thread; 

        @ConfigProperty(
            description="If this enabled with speculative execution, then HStoreSite only invoke the commit operation in the " +
                        "EE for the last transaction in the queued responses. This will cascade to all other queued responses " +
                        "successful transactions that were speculatively executed.",
            defaultBoolean=true,
            advanced=false
        )
        public boolean exec_queued_response_ee_bypass;
        
        // ----------------------------------------------------------------------------
        // Incoming Transaction Queue Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Max size of queued transactions before we stop accepting new requests and throttle clients",
            defaultInt=1000,
            advanced=false
        )
        public int txn_queue_max_per_partition;
        
        @ConfigProperty(
            description="", // TODO
            defaultDouble=0.25,
            advanced=false
        )
        public double txn_queue_release_factor;
        
        @ConfigProperty(
            description="", // TODO
            computed=true
        )
        public int txn_queue_max;

        @ConfigProperty(
            description="", // TODO
            computed=true
        )
        public int txn_queue_release;  
        
        // ----------------------------------------------------------------------------
        // Markov Transaction Estimator Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If this is set to true, TransactionEstimator will try to reuse MarkovPathEstimators" +
                        "for transactions running at the same partition.",
            defaultBoolean=true,
            advanced=false
        )
        public boolean markov_path_caching;
    
        @ConfigProperty(
            description="This threshold defines how accurate our cached MarkovPathEstimators have to be in order " +
                        "to keep using them. If (# of accurate txs / total txns) for a paritucular MarkovGraph " +
                        "goes below this threshold, then we will disable the caching",
            defaultDouble=1.0,
            advanced=true
        )
        public double markov_path_caching_threshold;
        
        // ----------------------------------------------------------------------------
        // ExecutionSiteHelper
        // ----------------------------------------------------------------------------
    
        @ConfigProperty(
            description="How many ms to wait initially before starting the ExecutionSiteHelper",
            defaultInt=2000,
            advanced=false
        )
        public int helper_initial_delay;
        
        @ConfigProperty(
            description="How many ms to wait before the ExecutionSiteHelper executes again to clean up txns",
            defaultInt=1000,
            advanced=true
        )
        public int helper_interval;
        
        @ConfigProperty(
            description="How many txns can the ExecutionSiteHelper clean-up per partition per round. Any value less " +
                        "than zero means that it will clean-up all txns it can per round",
            defaultInt=-1,
            advanced=true
        )
        public int helper_txn_per_round;
        
        @ConfigProperty(
            description="How long should the ExecutionSiteHelper wait before cleaning up a txn's state",
            defaultInt=1000,
            advanced=true
        )
        public int helper_txn_expire;

        // ----------------------------------------------------------------------------
        // HSTORESITE STATUS UPDATES
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable HStoreSite's StatusThread (# of milliseconds to print update). " +
                        "Set this to be -1 if you want to disable the status messages.",
            defaultInt=20000,
            advanced=false
        )
        public int status_interval;

        @ConfigProperty(
            description="Allow the HStoreSite StatusThread to kill the cluster if it looks hung.",
            defaultBoolean=true,
            advanced=false
        )
        public boolean status_kill_if_hung;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include transaction information",
            defaultBoolean=true,
            advanced=true
        )
        public boolean status_show_txn_info;

        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include information about each ExecutionSite, " +
                        "such as the number of transactions currently queued, blocked for execution, or waiting to have their results " +
                        "returned to the client.",
            defaultBoolean=true,
            advanced=true
        )
        public boolean status_show_executor_info;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include a snapshot of running threads",
            defaultBoolean=false,
            advanced=true
        )
        public boolean status_show_thread_info;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include pool allocation/deallocation statistics. " +
                        "Must be used in conjunction with ${site.pool_enable_tracking}",
            defaultBoolean=true,
            advanced=true
        )
        public boolean status_show_pool_info;
        
        // ----------------------------------------------------------------------------
        // OBJECT POOLS
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="The scale factor to apply to the object pool values.",
            defaultDouble=1.0,
            advanced=false
        )
        public double pool_scale_factor;
        
        @ConfigProperty(
            description="Whether to track the number of objects created, passivated, and destroyed from the pool. " + 
                        "Must be used with ${site.status_show_pool_info}",
            defaultBoolean=true,
            advanced=true
        )
        public boolean pool_enable_tracking;

        @ConfigProperty(
            description="The max number of VoltProcedure instances to keep in the pool " + 
                        "(per ExecutionSite + per Procedure)",
            defaultInt=10000,
            advanced=true
        )
        public int pool_voltprocedure_idle;
        
        @ConfigProperty(
            description="The max number of BatchPlans to keep in the pool (per BatchPlanner)",
            defaultInt=2000,
            advanced=true
        )
        public int pool_batchplan_idle;
    
        @ConfigProperty(
            description="The number of LocalTransactionState objects to preload",
            defaultInt=500,
            advanced=true
        )
        public int pool_localtxnstate_preload;
        
        @ConfigProperty(
            description="The max number of LocalTransactionStates to keep in the pool (per ExecutionSite)",
            defaultInt=5000,
            advanced=true
        )
        public int pool_localtxnstate_idle;
        
        @ConfigProperty(
            description="The number of RemoteTransactionState objects to preload",
            defaultInt=500,
            advanced=true
        )
        public int pool_remotetxnstate_preload;
        
        @ConfigProperty(
            description="The max number of RemoteTransactionStates to keep in the pool (per ExecutionSite)",
            defaultInt=500,
            advanced=true
        )
        public int pool_remotetxnstate_idle;
        
        @ConfigProperty(
            description="The max number of MarkovPathEstimators to keep in the pool (global)",
            defaultInt=1000,
            advanced=true
        )
        public int pool_pathestimators_idle;
        
        @ConfigProperty(
            description="The max number of TransactionEstimator.States to keep in the pool (global). " + 
                        "Should be the same as the number of MarkovPathEstimators.",
            defaultInt=1000,
            advanced=true
        )
        public int pool_estimatorstates_idle;
        
        @ConfigProperty(
            description="The max number of DependencyInfos to keep in the pool (global). " +
                        "Should be the same as the number of MarkovPathEstimators. ",
            defaultInt=50000,
            advanced=true
        )
        public int pool_dependencyinfos_idle;
        
        @ConfigProperty(
            description="The number of DependencyInfo objects to preload in the pool.",
            defaultInt=10000,
            advanced=true
        )
        public int pool_preload_dependency_infos;
        
        @ConfigProperty(
            description="The max number of ForwardTxnRequestCallbacks to keep in the pool",
            defaultInt=1500,
            advanced=false
        )
        public int pool_forwardtxnrequests_idle;
    }

    // ============================================================================
    // COORDINATOR
    // ============================================================================
    public final class CoordinatorConf extends Conf {
        
        @ConfigProperty(
            description="The hostname to deploy the Dtxn.Coordinator on in the cluster.",
            defaultString="${global.defaulthost}",
            advanced=false
        )
        public String host = HStoreConf.this.global.defaulthost;
        
        @ConfigProperty(
            description="The port number that the Dtxn.Coordinator will listen on.",
            defaultInt=12348,
            advanced=false
        )
        public int port;

        @ConfigProperty(
            description="How long should we wait before starting the dtxn coordinator (in milliseconds)",
            defaultInt=10000,
            advanced=false
        )
        public int delay;

        @ConfigProperty(
            description="Dtxn.Coordinator Log Directory",
            defaultString="${global.temp_dir}/logs/coordinator",
            advanced=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/coordinator";
        
    }
    
    // ============================================================================
    // CLIENT
    // ============================================================================
    public final class ClientConf extends Conf {
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

    /**
     * Prefix -> Configuration
     */
    private final Map<String, Conf> confHandles = new ListOrderedMap<String, Conf>();
    
    /**
     * Easy Access Handles
     */
    public final GlobalConf global = new GlobalConf();
    public final SiteConf site = new SiteConf();
    public final CoordinatorConf coordinator = new CoordinatorConf();
    public final ClientConf client = new ClientConf();
    
    /**
     * Singleton Object
     */
    private static HStoreConf conf;
    
    // ----------------------------------------------------------------------------
    // METHODS
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     */
    private HStoreConf(ArgumentsParser args, Site catalog_site) {
        if (args != null) {
            
            // Ignore the Dtxn.Coordinator
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN)) {
                site.exec_avoid_coordinator = args.getBooleanParam(ArgumentsParser.PARAM_NODE_IGNORE_DTXN);
                if (site.exec_avoid_coordinator) LOG.info("Ignoring the Dtxn.Coordinator for all single-partition transactions");
            }
//            // Enable speculative execution
//            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION)) {
//                site.exec_speculative_execution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION);
//                if (site.exec_speculative_execution) LOG.info("Enabling speculative execution");
//            }
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
        
        this.computeDerivedValues(catalog_site);
    }
    
    /**
     * 
     * @param catalog_site
     */
    protected void computeDerivedValues(Site catalog_site) {
        int local_partitions = 1;
        
        if (catalog_site != null) {
            
            // Partitions at this site
            local_partitions = catalog_site.getPartitions().size();
            
        }
        
        // Compute Parameters
        site.txn_queue_max = Math.round(local_partitions * site.txn_queue_max_per_partition);
        site.txn_queue_release = Math.max((int)(site.txn_queue_max * site.txn_queue_release_factor), 1);
    }
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
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
        return (this.makeConfig(false, false));
    }
    
    public String makeConfig(boolean experimental, boolean advanced) {
        StringBuilder sb = new StringBuilder();
        for (String group : this.confHandles.keySet()) {
            Conf handle = this.confHandles.get(group);

            sb.append("## ").append(StringUtil.repeat("-", 100)).append("\n")
              .append("## ").append(StringUtil.title(group)).append(" Parameters\n")
              .append("## ").append(StringUtil.repeat("-", 100)).append("\n\n");
            
            for (Field f : handle.properties.keySet()) {
                ConfigProperty cp = handle.properties.get(f);
                if (cp.advanced() && advanced == false) continue;
                if (cp.experimental() && experimental == false) continue;
                
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
    
    // ----------------------------------------------------------------------------
    // STATIC ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    public synchronized static HStoreConf init(ArgumentsParser args, Site catalog_site) {
        if (conf != null) throw new RuntimeException("Trying to initialize HStoreConf more than once");
        conf = new HStoreConf(args, catalog_site);
        return (conf);
    }
    
    public synchronized static HStoreConf singleton() {
        if (conf == null) throw new RuntimeException("Requesting HStoreConf before it is initialized");
        return (conf);
    }
    
    public synchronized static boolean isInitialized() {
        return (conf != null);
    }

}
