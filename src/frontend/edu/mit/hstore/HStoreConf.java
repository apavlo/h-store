package edu.mit.hstore;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Site;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.interfaces.ConfigProperty;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ============================================================================
    // GLOBAL
    // ============================================================================
    public final class GlobalConf extends Conf {
        
        @ConfigProperty(
            description="Temporary directory used to store various artifacts related to H-Store.",
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
            description="HStoreSite log directory on the host that the BenchmarkController is invoked from.",
            defaultString="${global.temp_dir}/logs/sites",
            advanced=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/sites";
        
        @ConfigProperty(
            description="The amount of memory to allocate for each site process (in MB)",
            defaultInt=1024,
            advanced=false
        )
        public int memory;

        @ConfigProperty(
            description="",
            defaultBoolean=true,
            experimental=true
        )
        public boolean cpu_affinity;
        
        @ConfigProperty(
            description="",
            defaultBoolean=false,
            experimental=true
        )
        public boolean cpu_affinity_one_partition_per_core;
        
        // ----------------------------------------------------------------------------
        // Execution Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable execution site profiling. This will keep track of how busy each ExecutionSite thread" +
                        "is during execution (i.e., the percentage of time that it spends executing a transaction versus " +
                        "waiting for work to be added to its queue).",
            defaultBoolean=false,
            advanced=false
        )
        public boolean exec_profiling;
        
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
            description="If this feature is enabled, then those non-speculative single partition transactions that are " +
                        "deemed to never abort will be executed without undo logging. Requires Markov model estimations.",
            defaultBoolean=false,
            advanced=true,
            experimental=true
        )
        public boolean exec_no_undo_logging;

        @ConfigProperty(
            description="All transactions are executed without any undo logging. For testing purposes only.",
            defaultBoolean=false,
            advanced=true,
            experimental=true
        )
        public boolean exec_no_undo_logging_all;
        
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
                        "Note that this is independent of how H-Store decides what partition to execute the transaction's Java " +
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
            description="Whether the VoltProcedure should crash the HStoreSite on a mispredict.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean exec_mispredict_crash;
        
        @ConfigProperty(
            description="If this enabled, HStoreSite will use a separate thread to process every outbound ClientResponse for " +
                        "all of the ExecutionSites. This may help with multi-partition transactions but will be the bottleneck " +
                        "for single-partition txn heavy workloads.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean exec_postprocessing_thread;
        
        @ConfigProperty(
            description="The number of post-processing threads to use per HStoreSite. " +
                        "The ${site.exec_postprocessing_thread} must be set to true.",
            defaultInt=1,
            advanced=true
        )
        public int exec_postprocessing_thread_count;
        

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
            description="Enable transaction profiling. This will measure the amount of time a transaction spends" +
            		    "in different parts of the system (e.g., waiting in the work queue, planning, executing).",
            defaultBoolean=false,
            advanced=true,
            experimental=true
        )
        public boolean txn_profiling;
        
        @ConfigProperty(
            description="Max size of queued transactions before an HStoreSite will stop accepting new requests " +
                        "from clients and will send back a ClientResponse with the throttle flag enabled.",
            defaultInt=1000,
            advanced=false
        )
        public int txn_incoming_queue_max_per_partition;
        
        @ConfigProperty(
            description="If the HStoreSite is throttling incoming client requests, then that HStoreSite " +
                        "will not accept new requests until the number of queued transactions is less than " +
                        "this percentage. " +
                        "The incoming queue release is calculated as " +
                        "${site.txn_incoming_queue_max} * ${site.txn_incoming_queue_release_factor}",
            defaultDouble=0.25,
            advanced=false
        )
        public double txn_incoming_queue_release_factor;
        
        @ConfigProperty(
            description="Max size of the total transaction queue per partition before an HStoreSite will stop " +
                        "accepting redirected requests from other HStoreSites.",
            defaultInt=2000,
            advanced=false
        )
        public int txn_redirect_queue_max_per_partition;
        
        @ConfigProperty(
            description="The number transactions that can be stored in the HStoreSite's internal queue before " +
                        "it will begin to reject redirected transaction requests from other HStoreSites. This " +
                        "includes all transactions that are waiting to be executed, executing, and those that " +
                        "have already executed and are waiting for their results to be sent back to the client. " +
                        "The redirect queue release is calculated as " +
                        "${site.txn_redirect_queue_max} * ${site.txn_redirect_queue_release_factor}",
            defaultDouble=0.50,
            advanced=false
        )
        public double txn_redirect_queue_release_factor;
        
        @ConfigProperty(
            description="Allow queued distributed transctions to be rejected.",
            defaultBoolean=false,
            experimental=true,
            advanced=true
        )
        public boolean txn_enable_queue_pruning;
        
        // ----------------------------------------------------------------------------
        // Markov Transaction Estimator Options
        // ----------------------------------------------------------------------------

        @ConfigProperty(
            description="Recompute a Markov model's execution state probabilities every time a transaction " +
                        "is aborted due to a misprediction. The Markov model is queued in the ExecutionSiteHelper " +
                        "for processing rather than being executed directly within the ExecutionSite's thread.",
            defaultBoolean=true,
            experimental=false,
            advanced=false
        )
        public boolean markov_mispredict_recompute;

        
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
        
        @ConfigProperty(
            description="The minimum number of queries that must be in a batch for the TransactionEstimator " +
                        "to cache the path segment in the procedure's MarkovGraph. Provides a minor speed improvement " +
                        "for large batches with little variability in their execution paths.",
            defaultInt=3,
            advanced=false
        )
        public int markov_batch_caching_min;

        // ----------------------------------------------------------------------------
        // BatchPlanner
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable BatchPlanner profiling. This will keep of how long the BatchPlanner spends performing " +
                        "certain operations.",
            defaultBoolean=false,
            experimental=false,
            advanced=false
        )
        public boolean planner_profiling;
        
        // ----------------------------------------------------------------------------
        // HStoreMessenger
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If this enabled, HStoreMesseger will use a separate thread to incoming redirect " +
                        "requests from other HStoreSites. ",
            defaultBoolean=false,
            advanced=true
        )
        public boolean messenger_redirect_thread;

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
            defaultInt=100,
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
            defaultInt=500,
            advanced=true
        )
        public int helper_txn_expire;
        
        // ----------------------------------------------------------------------------
        // Output Tracing
        // ----------------------------------------------------------------------------
        @ConfigProperty(
            description="When this property is set to true, all TransactionTrace records will include the stored procedure output result",
            defaultBoolean=false,
            advanced=true
        )
        public boolean trace_txn_output;

        @ConfigProperty(
            description="When this property is set to true, all QueryTrace records will include the query output result",
            defaultBoolean=false,
            advanced=true
        )
        public boolean trace_query_output;
        
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
            description="Allow the HStoreSiteStatus thread to kill the cluster if it's local HStoreSite has " +
                        "not executed and completed any new transactions since the last time it took a status snapshot.", 
            defaultBoolean=true,
            advanced=false
        )
        public boolean status_kill_if_hung;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include transaction information",
            defaultBoolean=false,
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
                        "Results are shown in HStoreSiteStatus updates.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean pool_profiling;

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
            description="The max number of ForwardTxnRequestCallbacks to keep idle in the pool",
            defaultInt=2500,
            advanced=false
        )
        public int pool_forwardtxnrequests_idle;
        
        @ConfigProperty(
            description="The max number of ForwardTxnResponseCallbacks to keep idle in the pool.",
            defaultInt=2500,
            advanced=false
        )
        public int pool_forwardtxnresponses_idle;
    }

    // ============================================================================
    // COORDINATOR
    // ============================================================================
    public final class CoordinatorConf extends Conf {
        
        @ConfigProperty(
            description="Dtxn.Coordinator log directory  on the host that the BenchmarkController " +
                        "is invoked from.",
            defaultString="${global.temp_dir}/logs/coordinator",
            advanced=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/coordinator";
        
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
            description="How long should we wait before starting the Dtxn.Coordinator (in milliseconds)",
            defaultInt=10000,
            advanced=false
        )
        public int delay;
    }
    
    // ============================================================================
    // CLIENT
    // ============================================================================
    public final class ClientConf extends Conf {
        
        @ConfigProperty(
            description="Benchmark client log directory on the host that the BenchmarkController " +
                        "is invoked from.",
            defaultString="${global.temp_dir}/logs/clients",
            advanced=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/clients";
        
        @ConfigProperty(
            description="The amount of memory to allocate for each client process (in MB)",
            defaultInt=512,
            advanced=false
        )
        public int memory;

        @ConfigProperty(
            description="Default client host name",
            defaultString="${global.defaulthost}",
            advanced=false
        )
        public String host = HStoreConf.this.global.defaulthost;

        @ConfigProperty(
            description="The number of txns that client process submits (per ms). The underlying " +
                        "BenchmarkComponent will continue invoke the client driver's runOnce() method " +
                        "until it has submitted enough transactions to satisfy ${client.txnrate}. " +
                        "If ${client.blocking} is disabled, then the total transaction rate for a benchmark run is " +
                        "${client.txnrate} * ${client.processesperclient} * ${client.count}.",
            defaultInt=10000,
            advanced=false
        )
        public int txnrate;

        @ConfigProperty(
            description="Number of processes to use per client host.",
            defaultInt=1,
            advanced=false
        )
        public int processesperclient;

        @ConfigProperty(
            description="Number of clients hosts to use in the benchmark run.",
            defaultInt=1,
            advanced=false
        )
        public int count;

        @ConfigProperty(
            description="How long should the benchmark trial run (in milliseconds). Does not " +
                        "include ${client.warmup time}.",
            defaultInt=60000,
            advanced=false
        )
        public int duration;

        @ConfigProperty(
            description="How long should the system be allowed to warmup (in milliseconds). Any stats " +
                        "collected during this period are not counted in the final totals.",
            defaultInt=0,
            advanced=false
        )
        public int warmup;

        @ConfigProperty(
            description="How often (in milliseconds) should the BenchmarkController poll the individual " +
                        "client processes and get their intermediate results.",
            defaultInt=10000,
            advanced=false
        )
        public int interval;

        @ConfigProperty(
            description="Whether to use the BlockingClient. When this is true, then each client process will " +
                        "submit one transaction at a time and wait until the result is returned before " +
                        "submitting the next. The clients still follow the ${client.txnrate} parameter.",
            defaultBoolean=false,
            advanced=false
        )
        public boolean blocking;
        
        @ConfigProperty(
            description="When the BlockingClient is enabled with ${client.blocking}, this defines the number " +
                        "of concurrent transactions that each client instance can submit to the H-Store cluster " +
                        "before it will block.",
            defaultInt=1,
            advanced=false
        )
        public int blocking_concurrent;

        @ConfigProperty(
            description="The scaling factor determines how large to make the target benchmark's data set. " +
                        "A scalefactor less than one makes the data set larger, while greater than one " +
                        "makes it smaller. Implementation depends on benchmark specification.",
            defaultDouble=10.0,
            advanced=false
        )
        public double scalefactor;

        @ConfigProperty(
            description="How much skew to use when generating the benchmark data set. " +
                        "Default is zero (no skew). The amount skew gets larger for values " +
                        "greater than one. Implementation depends on benchmark specification. ",
            defaultDouble=0.0,
            experimental=true
        )
        public double skewfactor;

        @ConfigProperty(
            description="Used to define the amount of temporal skew in the benchmark data set. " +
                        "Implementation depends on benchmark specification.",
            defaultInt=0,
            experimental=true
        )
        public int temporalwindow;
        
        @ConfigProperty(
            description="Used to define the amount of temporal skew in the benchmark data set. " +
                        "Implementation depends on benchmark specification.",
            defaultInt=100,
            experimental=true
        )
        public int temporaltotal;
        
        @ConfigProperty(
            description="If ${client.tick_interval} is greater than one, then it determines how often " +
                        "(in ms) the BenchmarkComponent will execute tick(). " +
                        "A client driver implementation can reliably use this to perform some " +
                        "maintence operation or change data distributions. By default, tick() will be " +
                        "invoked at the interval defined by ${client.interval}.",
            defaultInt=-1,
            advanced=true
        )
        public int tick_interval;

        @ConfigProperty(
            description="The amount of time (in ms) that the client will back-off from sending requests " +
                        "to an HStoreSite when told that the site is throttled.",
            defaultInt=500,
            advanced=false
        )
        public int throttle_backoff;
        
        @ConfigProperty(
            description="If this enabled, then each DBMS will dump their entire database contents into " +
                        "CSV files after executing a benchmark run.",
            defaultBoolean=false,
            advanced=false
        )
        public boolean dump_database = false;
        
        @ConfigProperty(
            description="If ${client.dump_database} is enabled, then each DBMS will dump their entire " +
                        "database contents into CSV files in the this directory after executing a benchmark run.",
            defaultString="${global.temp_dir}/dumps",
            advanced=false
        )
        public String dump_database_dir = HStoreConf.this.global.temp_dir + "/dumps";
        
        @ConfigProperty(
            description="If set to true, then the benchmark data loader will generate a WorkloadStatistics " +
                        "based on the data uploaded to the server. These stats will be written to the path " +
                        "specified by ${client.tablestats_output}.",
            defaultBoolean=false,
            advanced=true
        )
        public boolean tablestats = false;
        
        @ConfigProperty(
            description="If ${client.tablestats} is enabled, then the loader will write out a database statistics " +
                        "file in the directory defined in this parameter.",
            defaultString="${global.temp_dir}/stats",
            advanced=true
        )
        public String tablestats_dir = HStoreConf.this.global.temp_dir + "/stats";
        
        @ConfigProperty(
            description="If set to true, then the client calculate the base partition needed by each transaction " +
                        "request and send that request to the HStoreSite that has that partition.",
            defaultBoolean=false,
            experimental=true,
            advanced=true
        )
        public boolean txn_hints = false;
        
        @ConfigProperty(
            description="If a node is executing multiple client processes, then the node may become overloaded if " +
                        "all the clients are started at the same time. This parameter defines the threshold for when " +
                        "the BenchmarkController will stagger the start time of clients. For example, if a node will execute " +
                        "ten clients and ${client.delay_threshold} is set to five, then the first five processes will start " +
                        "right away and the remaining five will wait until the first ones finish before starting themselves.", 
            defaultInt=8,
            experimental=false,
            advanced=false
        )
        public int delay_threshold = 8;
    }
    
    /**
     * Base Configuration Class
     */
    private abstract class Conf {
        
        final Map<Field, ConfigProperty> properties;
        final String prefix;
        final Class<? extends Conf> confClass; 
        
        {
            this.confClass = this.getClass();
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
                Object value = getDefaultValue(f, cp);
                
                try {
                    if (value != null) f.set(this, value);
                } catch (Exception ex) {
                    throw new RuntimeException(String.format("Failed to set default value '%s' for field '%s'", value, f.getName()), ex);
                }
//                System.err.println(String.format("%-20s = %s", f.getName(), value));
            } // FOR   
        }

        @SuppressWarnings("unchecked")
        public <T> T getValue(String name) {
            T val = null;
            try {
                Field f = this.confClass.getField(name);
                val = (T)f.get(this);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid field '" + name + "' for " + this.confClass.getSimpleName(), ex);
            }
            return (val);
        }
        
        @Override
        public String toString() {
            return (this.toString(false, false));
        }
        
        public String toString(boolean advanced, boolean experimental) {
            final Map<String, Object> m = new TreeMap<String, Object>();
            for (Entry<Field, ConfigProperty> e : this.properties.entrySet()) {
                ConfigProperty cp = e.getValue();
                if (advanced == false && cp.advanced()) continue;
                if (experimental == false && cp.experimental()) continue;
                
                Field f = e.getKey();
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
    
    private final Map<Conf, Set<String>> loaded_params = new HashMap<Conf, Set<String>>();
    
    // ----------------------------------------------------------------------------
    // METHODS
    // ----------------------------------------------------------------------------

    private HStoreConf() {
        // Empty configuration...
    }
    
    /**
     * Constructor
     */
    private HStoreConf(ArgumentsParser args, Site catalog_site) {
        if (args != null) {
            
            // Configuration File
            if (args.hasParam(ArgumentsParser.PARAM_CONF)) {
                this.loadFromFile(args.getFileParam(ArgumentsParser.PARAM_CONF));
            }
            
            // Ignore the Dtxn.Coordinator
            if (args.hasBooleanParam(ArgumentsParser.PARAM_SITE_IGNORE_DTXN)) {
                site.exec_avoid_coordinator = args.getBooleanParam(ArgumentsParser.PARAM_SITE_IGNORE_DTXN);
                if (site.exec_avoid_coordinator) LOG.info("Ignoring the Dtxn.Coordinator for all single-partition transactions");
            }
//            // Enable speculative execution
//            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION)) {
//                site.exec_speculative_execution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_SPECULATIVE_EXECUTION);
//                if (site.exec_speculative_execution) LOG.info("Enabling speculative execution");
//            }
            // Enable DB2-style txn redirecting
            if (args.hasBooleanParam(ArgumentsParser.PARAM_SITE_ENABLE_DB2_REDIRECTS)) {
                site.exec_db2_redirects = args.getBooleanParam(ArgumentsParser.PARAM_SITE_ENABLE_DB2_REDIRECTS);
                if (site.exec_db2_redirects) LOG.info("Enabling DB2-style transaction redirects");
            }
            // Force all transactions to be single-partitioned
            if (args.hasBooleanParam(ArgumentsParser.PARAM_SITE_FORCE_SINGLEPARTITION)) {
                site.exec_force_singlepartitioned = args.getBooleanParam(ArgumentsParser.PARAM_SITE_FORCE_SINGLEPARTITION);
                if (site.exec_force_singlepartitioned) LOG.info("Forcing all transactions to execute as single-partitioned");
            }
            // Force all transactions to be executed at the first partition that the request arrives on
            if (args.hasBooleanParam(ArgumentsParser.PARAM_SITE_FORCE_LOCALEXECUTION)) {
                site.exec_force_localexecution = args.getBooleanParam(ArgumentsParser.PARAM_SITE_FORCE_LOCALEXECUTION);
                if (site.exec_force_localexecution) LOG.info("Forcing all transactions to execute at the partition they arrive on");
            }
            // Enable the "neworder" parameter hashing hack for the VLDB paper
//            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT)) {
//                site.exec_neworder_cheat = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT);
//                if (site.exec_neworder_cheat) LOG.info("Enabling the inspection of incoming neworder parameters");
//            }
//            // Enable setting the done partitions for the "neworder" parameter hashing hack for the VLDB paper
//            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE)) {
//                site.exec_neworder_cheat_done_partitions = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT_DONE);
//                if (site.exec_neworder_cheat_done_partitions) LOG.info("Enabling the setting of done partitions for neworder inspection");
//            }
            // Clean-up Interval
            if (args.hasIntParam(ArgumentsParser.PARAM_SITE_CLEANUP_INTERVAL)) {
                site.helper_interval = args.getIntParam(ArgumentsParser.PARAM_SITE_CLEANUP_INTERVAL);
                LOG.debug("Setting Cleanup Interval = " + site.helper_interval + "ms");
            }
            // Txn Expiration Time
            if (args.hasIntParam(ArgumentsParser.PARAM_SITE_CLEANUP_TXN_EXPIRE)) {
                site.helper_txn_expire = args.getIntParam(ArgumentsParser.PARAM_SITE_CLEANUP_TXN_EXPIRE);
                LOG.debug("Setting Cleanup Txn Expiration = " + site.helper_txn_expire + "ms");
            }
            // Profiling
            if (args.hasBooleanParam(ArgumentsParser.PARAM_SITE_ENABLE_PROFILING)) {
                site.txn_profiling = args.getBooleanParam(ArgumentsParser.PARAM_SITE_ENABLE_PROFILING);
                if (site.txn_profiling) LOG.info("Enabling procedure profiling");
            }
            // Mispredict Crash
            if (args.hasBooleanParam(ArgumentsParser.PARAM_SITE_MISPREDICT_CRASH)) {
                site.exec_mispredict_crash = args.getBooleanParam(ArgumentsParser.PARAM_SITE_MISPREDICT_CRASH);
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
        // Negate Parameters
        if (site.exec_neworder_cheat) {
            site.exec_force_singlepartitioned = false;
            site.exec_force_localexecution = false;
        }
    }
    
    private Object getDefaultValue(Field f, ConfigProperty cp) {
        Class<?> f_class = f.getType();
        Object value = null;
        
        if (f_class.equals(int.class)) {
            value = cp.defaultInt();
        } else if (f_class.equals(long.class)) {
            value = cp.defaultLong();
        } else if (f_class.equals(double.class)) {
            value = cp.defaultDouble();
        } else if (f_class.equals(boolean.class)) {
            value = cp.defaultBoolean();
        } else if (f_class.equals(String.class)) {
            value = cp.defaultString();
        } else {
            LOG.warn(String.format("Unexpected default value type '%s' for property '%s'", f_class.getSimpleName(), f.getName()));
        }
        return (value);
    }
    
    private Pattern makePattern() {
        return Pattern.compile(String.format("(%s)\\.(.*)", StringUtil.join("|", this.confHandles.keySet())));
    }
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
    public void loadFromFile(File path) {
        try {
            this.config = new PropertiesConfiguration(path);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load configuration file " + path);
        }

        Pattern p = this.makePattern();
        for (Object obj_k : CollectionUtil.wrapIterator(this.config.getKeys())) {
            String k = obj_k.toString();
            Matcher m = p.matcher(k);
            boolean found = m.matches();
            if (m == null || found == false) {
                if (debug.get()) LOG.warn("Invalid key '" + k + "' from configuration file '" + path + "'");
                continue;
            }
            assert(m != null);
            
            Conf handle = confHandles.get(m.group(1));
            Class<?> confClass = handle.getClass();
            assert(confClass != null);
            Field f = null;
            String f_name = m.group(2);
            try {
                f = confClass.getField(f_name);
            } catch (Exception ex) {
                if (debug.get()) LOG.warn("Invalid configuration property '" + k + "'. Ignoring...");
                continue;
            }
            ConfigProperty cp = handle.properties.get(f);
            assert(cp != null) : "Missing ConfigProperty for " + f;
            Class<?> f_class = f.getType();
            Object defaultValue = (cp != null ? this.getDefaultValue(f, cp) : null);
            Object value = null;
            
            if (f_class.equals(int.class)) {
                value = this.config.getInt(k, (Integer)defaultValue);
            } else if (f_class.equals(long.class)) {
                value = this.config.getLong(k, (Long)defaultValue);
            } else if (f_class.equals(double.class)) {
                value = this.config.getDouble(k, (Double)defaultValue);
            } else if (f_class.equals(boolean.class)) {
                value = this.config.getBoolean(k, (Boolean)defaultValue);
            } else if (f_class.equals(String.class)) {
                value = this.config.getString(k, (String)defaultValue);
            } else {
                LOG.warn(String.format("Unexpected value type '%s' for property '%s'", f_class.getSimpleName(), f_name));
            }
            
            try {
                f.set(handle, value);
//                if (defaultValue != null && defaultValue.equals(value) == false) LOG.info(String.format("SET %s = %s", k, value));
                if (debug.get()) LOG.debug(String.format("SET %s = %s", k, value));
            } catch (Exception ex) {
                throw new RuntimeException("Failed to set value '" + value + "' for field '" + f_name + "'", ex);
            }
        } // FOR
    }
    
    public void loadFromArgs(String args[]) {
        final Pattern split_p = Pattern.compile("=");
        
        final Map<String, String> argsMap = new ListOrderedMap<String, String>();
        for (int i = 0, cnt = args.length; i < cnt; i++) {
            final String arg = args[i];
            final String[] parts = split_p.split(arg, 2);
            String k = parts[0].toLowerCase();
            String v = parts[1];
            if (k.startsWith("-")) k = k.substring(1);
            
            if (parts.length == 1) {
                continue;
            } else if (k.equalsIgnoreCase("tag")) {
                continue;
            } else if (v.startsWith("${") || k.startsWith("#")) {
                continue;
            } else {
                argsMap.put(k, v);
            }
        } // FOR
        this.loadFromArgs(argsMap);
    }
    
    public void loadFromArgs(Map<String, String> args) {
        Pattern p = this.makePattern();
        for (Entry<String, String> e : args.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            
            Matcher m = p.matcher(k);
            boolean found = m.matches();
            if (m == null || found == false) {
                if (debug.get()) LOG.warn("Invalid key '" + k + "'");
                continue;
            }
            assert(m != null);

            String confName = m.group(1);
            Conf confHandle = confHandles.get(confName);
            Class<?> confClass = confHandle.getClass();
            assert(confClass != null);
            Field f = null;
            String f_name = m.group(2).toLowerCase();
            try {
                f = confClass.getField(f_name);
            } catch (Exception ex) {
                if (debug.get()) LOG.warn("Invalid configuration property '" + k + "'. Ignoring...");
                continue;
            }
            ConfigProperty cp = confHandle.properties.get(f);
            assert(cp != null) : "Missing ConfigProperty for " + f;
            Class<?> f_class = f.getType();
            Object value = null;
            
            if (f_class.equals(int.class)) {
                value = Integer.parseInt(v);
            } else if (f_class.equals(long.class)) {
                value = Long.parseLong(v);
            } else if (f_class.equals(double.class)) {
                value = Double.parseDouble(v);
            } else if (f_class.equals(boolean.class)) {
                value = Boolean.parseBoolean(v);
            } else if (f_class.equals(String.class)) {
                value = v;
            } else {
                LOG.warn(String.format("Unexpected value type '%s' for property '%s'", f_class.getSimpleName(), f_name));
                continue;
            }
            try {
                f.set(confHandle, value);
                if (debug.get()) LOG.debug(String.format("PARAM SET %s = %s", k, value));
            } catch (Exception ex) {
                throw new RuntimeException("Failed to set value '" + value + "' for field '" + f_name + "'", ex);
            } finally {
                Set<String> s = this.loaded_params.get(confHandle);
                if (s == null) {
                    s = new HashSet<String>();
                    this.loaded_params.put(confHandle, s);
                }
                s.add(f_name);
            }
        } // FOR
    }
    
    public Map<String, String> getParametersLoadedFromArgs() {
        Map<String, String> m = new HashMap<String, String>();
        for (Conf confHandle : this.loaded_params.keySet()) {
            for (String f_name : this.loaded_params.get(confHandle)) {
                Object val = confHandle.getValue(f_name);
                if (val != null) m.put(f_name, val.toString());
            } // FOR
        } // FOR
        return (m);
    }
    
    
    public String makeHTML(String group) {
        StringBuilder inner = new StringBuilder();
        StringBuilder top = new StringBuilder();
        
        // Parameters:
        //  (1) parameter
        //  (2) parameter
        //  (3) experimental
        //  (4) default value
        //  (5) description 
        final String template = "<a name=\"@@PROP@@\"></a>\n" +
                                "<li><tt class=\"property\">@@PROPFULL@@</tt>@@EXP@@\n" +
                                "<table>\n" +
                                "<tr><td class=\"prop-default\">Default:</td><td><tt>@@DEFAULT@@</tt></td>\n" +
                                "<tr><td class=\"prop-type\">Permitted Type:</td><td><tt>@@TYPE@@</tt></td>\n" +
                                "<tr><td colspan=\"2\">@@DESC@@</td></tr>\n" +
                                "</table></li>\n\n";
        
        final Pattern regex = Pattern.compile("\\$\\{([\\w]+)\\.([\\w\\_]+)\\}");
        final String regex_replace = "<a href=\"/documentation/configuration/properties-file/$1#$2\" class=\"property\">\\${$1.$2}</a>";
        
        Conf handle = this.confHandles.get(group);
        
        top.append(String.format("<h2>%s Parameters</h2>\n<ul>\n", StringUtil.title(group)));
        inner.append("<ul class=\"property-list\">\n\n");
        
        Map<String, String> values = new HashMap<String, String>();
        for (Field f : handle.properties.keySet()) {
            ConfigProperty cp = handle.properties.get(f);

            // PROP
            values.put("PROP", f.getName());
            values.put("PROPFULL", String.format("%s.%s", group, f.getName()));
            
            // DEFAULT
            Object defaultValue = this.getDefaultValue(f, cp);
            if (defaultValue != null) {
                String value = defaultValue.toString();
                Matcher m = regex.matcher(value);
                if (m.find()) value = m.replaceAll(regex_replace);
                defaultValue = value;
            }
            values.put("DEFAULT", (defaultValue != null ? defaultValue.toString() : "null"));
            
            // TYPE
            values.put("TYPE", f.getType().getSimpleName().toLowerCase());
            
            // EXPERIMENTAL
            if (cp.experimental()) {
                values.put("EXP", " <b class=\"experimental\">Experimental</b>");
            } else {
                values.put("EXP", "");   
            }
            
            // DESC
            String desc = cp.description();
            Matcher m = regex.matcher(desc);
            if (m.find()) {
                desc = m.replaceAll(regex_replace);
            }
            values.put("DESC", desc);
            
            // CREATE HTML FROM TEMPLATE
            String copy = template;
            for (String key : values.keySet()) {
                copy = copy.replace("@@" + key.toUpperCase() + "@@", values.get(key));
            }
            inner.append(copy);
            
            // INDEX
            copy = regex_replace;
            copy = copy.replace("$1", group).replace("$2", f.getName()).replace("\\$", "$");
            top.append("  <li>  ").append(copy).append("\n");
        } // FOR
        inner.append("</ul>\n\n");
        top.append("</ul>\n\n");
        
        return (top.toString() + inner.toString());
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
        return (this.toString(false, false));
    }
        
    public String toString(boolean advanced, boolean experimental) {
        Class<?> confClass = this.getClass();
        final Map<String, Object> m = new TreeMap<String, Object>();
        for (Field f : confClass.getFields()) {
            String key = f.getName().toUpperCase();
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                m.put(key, ex.getMessage());
            }
            
            if (obj instanceof Conf) {
                m.put(key, ((Conf)obj).toString(advanced, experimental));
            }
        }
        return (StringUtil.formatMaps(m));
    }
    
    // ----------------------------------------------------------------------------
    // STATIC ACCESS METHODS
    // ----------------------------------------------------------------------------

    public synchronized static HStoreConf init(File f, String args[]) {
        if (conf != null) throw new RuntimeException("Trying to initialize HStoreConf more than once");
        conf = new HStoreConf();
        if (f != null && f.exists()) conf.loadFromFile(f);
        if (args != null) conf.loadFromArgs(args);
        return (conf);
    }
    
    public synchronized static HStoreConf init(File f) {
        return HStoreConf.init(f, null);
    }
    
    public synchronized static HStoreConf initArgumentsParser(ArgumentsParser args, Site catalog_site) {
        if (conf != null) throw new RuntimeException("Trying to initialize HStoreConf more than once");
        conf = new HStoreConf(args, catalog_site);
        return (conf);
    }
    
    public synchronized static HStoreConf singleton() {
        return singleton(false);
    }
    
    public synchronized static HStoreConf singleton(boolean init) {
        if (conf == null && init == true) return init(null);
        if (conf == null) throw new RuntimeException("Requesting HStoreConf before it is initialized");
        return (conf);
    }
    
    public synchronized static boolean isInitialized() {
        return (conf != null);
    }

}
