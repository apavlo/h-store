package edu.brown.hstore.conf;

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

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.interfaces.ConfigProperty;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    
    static final Pattern REGEX_PARSE = Pattern.compile("(site|client|global)\\.([\\w\\_]+)");
    
    // ============================================================================
    // GLOBAL
    // ============================================================================
    public final class GlobalConf extends Conf {
        
        @ConfigProperty(
            description="Temporary directory used to store various artifacts related to H-Store.",
            defaultString="obj",
            experimental=false
        )
        public String temp_dir = "obj";

        @ConfigProperty(
            description="Options used when logging into client/server hosts. " + 
                        "We assume that there will be no spaces in paths or options listed here.",
            defaultString="-x",
            experimental=false
        )
        public String sshoptions;
        
        @ConfigProperty(
            description="An optional command that is added as a prefix that is executed before " +
                        "starting the HStoreSite and clients. The command must exit with " +
                        "status code zero.",
            defaultString="",
            experimental=false
        )
        public String sshprefix;

        @ConfigProperty(
            description="The default hostname used when generating cluster configurations.",
            defaultString="localhost",
            experimental=false
        )
        public String defaulthost;
        
        @ConfigProperty(
            description="The name of the AbstractHasher class to use to figure out what partitions " +
                        "transactions and queries need to go to. We should not need to change this.",
            defaultString="edu.brown.hashing.DefaultHasher",
            experimental=true
        )
        public String hasherClass;
    }
    
    // ============================================================================
    // SITE
    // ============================================================================
    public final class SiteConf extends Conf {
    
        @ConfigProperty(
            description="HStoreSite log directory on the host that the BenchmarkController is invoked from.",
            defaultString="${global.temp_dir}/logs/sites",
            experimental=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/sites";
        
        @ConfigProperty(
            description="Whether to back-up log files before the benchmark is exceuted",
            defaultBoolean=true,
            experimental=false
        )
        public boolean log_backup;
        
        @ConfigProperty(
            description="The amount of memory to allocate for each site process (in MB)",
            defaultInt=2048,
            experimental=false
        )
        public int memory;
        
        @ConfigProperty(
            description="When enabled, the HStoreSite will preload objects when the system is started. " +
            		    "This should only be disabled for regression test cases.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean preload;

        @ConfigProperty(
            description="When enabled, the PartitionExecutor threads will be pinned to the first n CPU cores (where " +
                        "n is the total number of partitions hosted by the local HStoreSite). All other threads " +
                        "(e.g., for network handling) will be pinned to the remaining CPU cores. If there are fewer " +
                        "CPU cores than partitions, then this option will be disabled. ",
            defaultBoolean=true,
            experimental=false
        )
        public boolean cpu_affinity;
        
        @ConfigProperty(
            description="When used in conjunction with ${site.cpu_affinity}, each PartitionExecutor thread will be " +
                        "assigned to one and only CPU core. No other thread within the HStoreSite (including all " +
                        "other PartitionExecutors) will be allowed to execute on that core. This configuration option is " +
                        "mostly used for debugging and is unlikely to provide any speed improvement because the " +
                        "operating system will automatically maintain CPU affinity.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean cpu_affinity_one_partition_per_core;
        
        @ConfigProperty(
                description="one blank node for Live Migration.",
                defaultString="",
                experimental=true
            )
        public String newsiteinfo;
        
        // ----------------------------------------------------------------------------
        // Execution Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="ExecutionEngine log level.",
            defaultInt=500,
            experimental=false
        )
        public int exec_ee_log_level;
        
        @ConfigProperty(
            description="Enable execution site profiling. This will keep track of how busy each PartitionExecutor thread" +
                        "is during execution (i.e., the percentage of time that it spends executing a transaction versus " +
                        "waiting for work to be added to its queue).",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_profiling;
        
        @ConfigProperty(
            description="If this feature is enabled, then each HStoreSite will attempt to speculatively execute " +
                        "single-partition transactions whenever it completes a work request for a multi-partition " +
                        "transaction running on a different node.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean exec_speculative_execution;
        
        @ConfigProperty(
            description="If this feature is enabled, then those non-speculative single partition transactions that are " +
                        "deemed to never abort will be executed without undo logging. Requires Markov model estimations.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_no_undo_logging;

        @ConfigProperty(
            description="All transactions are executed without any undo logging. For testing purposes only.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_no_undo_logging_all;
        
        @ConfigProperty(
            description="Force all transactions to execute with undo logging. For testing purposes only.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_force_undo_logging_all;
        
        @ConfigProperty(
            description="If this parameter is set to true, then each HStoreSite will not send every transaction request " +
                        "through the Dtxn.Coordinator. Only multi-partition transactions will be sent to the " +
                        "Dtxn.Coordinator (in order to ensure global ordering). Setting this property to true provides a " +
                        "major throughput improvement.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean exec_avoid_coordinator;
        
        @ConfigProperty(
            description="If this configuration parameter is true, then H-Store will use DB2-style transaction redirects. " +
            		    "Each request will execute as a single-partition transaction at a random partition on the node " +
            		    "that the request originally arrives on. When the transaction makes a query request that needs " +
            		    "to touch data from a partition that is different than its base partition, then that transaction " +
            		    "is immediately aborted, rolled back, and restarted on the partition that has the data that it " +
            		    "was requesting. If the transaction requested more than partition when it was aborted, then it " +
            		    "will be executed as a multi-partition transaction on the partition that was requested most often " +
            		    "by queries using random tie breakers). " +
                        "See http://ibm.co/fLR2cH for more information.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean exec_db2_redirects;
        
        @ConfigProperty(
            description="Always execute transactions as single-partitioned (excluding sysprocs). If a transaction requests " +
                        "data on a partition that is different than where it is executing, then it is aborted, rolled back, " +
                        "and re-executed on the same partition as a multi-partition transaction that touches all partitions. " +
                        "Note that this is independent of how H-Store decides what partition to execute the transaction's Java " +
                        "control code on.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean exec_force_singlepartitioned;
        
        @ConfigProperty(
            description="Use the VoltDB @ProcInfo annotations for stored procedures to determine whether " +
            		    "a new request will be executed as a single-partitioned or distributed transaction. " +
            		    "Note that if this option is enabled, any distributed transaction will have to lock all " +
            		    "of the partitions in the cluster.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_voltdb_procinfo;
        
        @ConfigProperty(
            description="Always execute each transaction on a random partition on the node where the request originally " +
                        "arrived on. Note that this is independent of whether the transaction is selected to be " +
                        "single-partitioned or not. It is likely that you do not want to use this option.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_force_localexecution;
    
        @ConfigProperty(
            description="Whether the VoltProcedure should crash the HStoreSite when a transaction is mispredicted. A " +
                        "mispredicted transaction is one that was originally identified as single-partitioned but then " +
                        "executed a query that attempted to access multiple partitions. This is primarily used for debugging " +
                        "the TransactionEstimator.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_mispredict_crash;
        
        @ConfigProperty(
            description="If this enabled, HStoreSite will use a separate thread to process every outbound ClientResponse for " +
                        "all of the PartitionExecutors. This may help with multi-partition transactions but will be the bottleneck " +
                        "for single-partition txn heavy workloads because the thread must acquire the lock on each partition's " +
                        "ExecutionEngine in order to commit or abort a transaction.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_postprocessing_thread;
        
        @ConfigProperty(
            description="The number of post-processing threads to use per HStoreSite. " +
                        "The ${site.exec_postprocessing_thread} parameter must be set to true.",
            defaultInt=1,
            experimental=true
        )
        public int exec_postprocessing_thread_count;
        
        @ConfigProperty(
            description="If this enabled with speculative execution, then HStoreSite only invoke the commit operation in the " +
                        "EE for the last transaction in the queued responses. This will cascade to all other queued responses " +
                        "successful transactions that were speculatively executed.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean exec_queued_response_ee_bypass;
        
        @ConfigProperty(
            description="The maximum amount of time that the PartitionExecutor will wait for the results of a distributed  " +
                        "query to return to the transaction's base partition. Usually if this limit is reached, then there " +
                        "is something very wrong with the distributed transaction protocol.",
            defaultInt=10000,
            experimental=true
        )
        public int exec_response_timeout;
        
        @ConfigProperty(
            description="If this parameter is enabled, then the PartitionExecutor will check for every SQLStmt batch " +
                        "for each distributed transaction contains valid WorkFragments.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_validate_work;

        @ConfigProperty(
            description="If enabled, log all transaction requests to disk",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_command_logging;
        
        @ConfigProperty(
            description="Directory for storage of command logging files",
            defaultString="${global.temp_dir}/wal",
            experimental=true
        )
        public String exec_command_logging_directory = HStoreConf.this.global.temp_dir + "/wal";
        
        @ConfigProperty(
            description="Transactions to queue before flush for group commit command logging optimization (0 = no group commit)",
            defaultInt=0,
            experimental=true
        )
        public int exec_command_logging_group_commit;
        
        @ConfigProperty(
            description="Setting this configuration parameter to true allows clients to " +
                        "issue ad hoc query requests use the @AdHoc sysproc.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean exec_adhoc_sql;
        
        @ConfigProperty(
            description="If this parameter is enabled, then the DBMS will attempt to prefetch commutative " +
            		    "queries on remote partitions for distributed transactions.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_prefetch_queries;
        
        @ConfigProperty(
            description="If this parameter is enabled, then the DBMS will queue up any single-partitioned " +
            		    "queries for later execution if they are marked as deferrable.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_deferrable_queries;
        
        // ----------------------------------------------------------------------------
        // MapReduce Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
                description="the way to execute reduce job, blocking or non-blocking by MapReduceHelperThread",
                defaultBoolean=true,
                experimental=true
        )
        public boolean mapreduce_reduce_blocking;
       
        // ----------------------------------------------------------------------------
        // Incoming Transaction Queue Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable transaction profiling. This will measure the amount of time a transaction spends" +
                        "in different parts of the system (e.g., waiting in the work queue, planning, executing).",
            defaultBoolean=false,
            experimental=false
        )
        public boolean txn_profiling;
        
        @ConfigProperty(
            description="The amount of time the TransactionQueueManager will wait before letting a " +
                        "distributed transaction id from aquiring a lock on a partition.",
            defaultInt=1,
            experimental=true
        )
        public int txn_incoming_delay;
        
        @ConfigProperty(
            description="The number of times that a distributed transaction is allowed to be restarted " +
                        "(due to things like network delays) before it is outright rejected and the request " +
                        "is returned to the client.",
            defaultInt=10,
            experimental=false
        )
        public int txn_restart_limit;
        
        @ConfigProperty(
            description="", // TODO
            defaultInt=10,
            experimental=false
        )
        public int txn_restart_limit_sysproc;
        
        @ConfigProperty(
            description="", // TODO
            defaultBoolean=false,
            experimental=true
        )
        public boolean txn_partition_id_managers;
        
        // ----------------------------------------------------------------------------
        // Distributed Transaction Queue Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Max size of queued transactions before an HStoreSite will stop accepting new requests " +
                        "from clients and will send back a ClientResponse with the throttle flag enabled.",
            defaultInt=150,
            experimental=false
        )
        public int queue_incoming_max_per_partition;
        
        @ConfigProperty(
            description="If the HStoreSite is throttling incoming client requests, then that HStoreSite " +
                        "will not accept new requests until the number of queued transactions is less than " +
                        "this percentage. This includes all transactions that are waiting to be executed, " +
                        "executing, and those that have already executed and are waiting for their results " +
                        "to be sent back to the client. The incoming queue release is calculated as " +
                        "${site.txn_incoming_queue_max} * ${site.txn_incoming_queue_release_factor}",
            defaultDouble=0.75,
            experimental=false
        )
        public double queue_incoming_release_factor;
        
        @ConfigProperty(
            description="Whenever a transaction completes, the HStoreSite will check whether the work queue " +
                        "for that transaction's base partition is empty (i.e., the PartitionExecutor is idle). " +
                        "If it is, then the HStoreSite will increase the ${site.txn_incoming_queue_max_per_partition} " +
                        "value by this amount. The release limit will also be recalculated using the new value " +
                        "for ${site.txn_incoming_queue_max_per_partition}. Note that this will only occur after " +
                        "the first non-data loading transaction has been issued from the clients.",
            defaultInt=10,
            experimental=false
        )
        public int queue_incoming_increase;
        
        @ConfigProperty(
            description="The maximum amount that the ${site.queue_incoming_max_per_partition} parameter " +
                        "can be increased by per partition.",
            defaultInt=300,
            experimental=false
        )
        public int queue_incoming_increase_max;
        
        @ConfigProperty(
            description="If a transaction is rejected by an PartitionExecutor because its queue is full, then " +
                        "this parameter determines what kind of response will be sent back to the client. " +
                        "Setting this parameter to true causes the client to recieve an ABORT_THROTTLED " +
                        "status response, which means it will wait for ${client.throttle_backoff} ms before " +
                        "sending another transaction request. Otherwise, the client will recieve an " +
                        "ABORT_REJECT status response and will be allowed to queue another transaction immediately.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean queue_incoming_throttle;
        
        @ConfigProperty(
            description="Max size of queued transactions before an HStoreSite will stop accepting new requests " +
                        "from clients and will send back a ClientResponse with the throttle flag enabled.",
            defaultInt=100,
            experimental=false
        )
        public int queue_dtxn_max_per_partition;
        
        @ConfigProperty(
            description="If the HStoreSite is throttling incoming client requests, then that HStoreSite " +
                        "will not accept new requests until the number of queued transactions is less than " +
                        "this percentage. This includes all transactions that are waiting to be executed, " +
                        "executing, and those that have already executed and are waiting for their results " +
                        "to be sent back to the client. The incoming queue release is calculated as " +
                        "${site.txn_incoming_queue_max} * ${site.txn_incoming_queue_release_factor}",
            defaultDouble=0.50,
            experimental=false
        )
        public double queue_dtxn_release_factor;
        
        @ConfigProperty(
            description="Whenever a transaction completes, the HStoreSite will check whether the work queue " +
                        "for that transaction's base partition is empty (i.e., the PartitionExecutor is idle). " +
                        "If it is, then the HStoreSite will increase the ${site.txn_incoming_queue_max_per_partition} " +
                        "value by this amount. The release limit will also be recalculated using the new value " +
                        "for ${site.txn_incoming_queue_max_per_partition}. Note that this will only occur after " +
                        "the first non-data loading transaction has been issued from the clients.",
            defaultInt=10,
            experimental=false
        )
        public int queue_dtxn_increase;
        
        @ConfigProperty(
            description="The maximum amount that the ${site.queue_dtxn_max_per_partition} parameter " +
                        "can be increased by per partition.",
            defaultInt=300,
            experimental=false
        )
        public int queue_dtxn_increase_max;
        
        @ConfigProperty(
            description="If a transaction is rejected by the HStoreSite's distributed txn queue manager, then " +
                        "this parameter determines what kind of response will be sent back to the client. " +
                        "Setting this parameter to true causes the client to recieve an ABORT_THROTTLED " +
                        "status response, which means it will wait for ${client.throttle_backoff} ms before " +
                        "sending another transaction request. Otherwise, the client will recieve an " +
                        "ABORT_REJECT status response and will be allowed to queue another transaction immediately.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean queue_dtxn_throttle;
        
        // ----------------------------------------------------------------------------
        // Parameter Mapping Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="", // TODO
            defaultNull=true,
            experimental=false
        )
        public String mappings_path;
        
        // ----------------------------------------------------------------------------
        // Markov Transaction Estimator Options
        // ----------------------------------------------------------------------------

        @ConfigProperty(
            description="Recompute a Markov model's execution state probabilities every time a transaction " +
                        "is aborted due to a misprediction. The Markov model is queued in the PartitionExecutorHelper " +
                        "for processing rather than being executed directly within the PartitionExecutor's thread.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean markov_mispredict_recompute;

        @ConfigProperty(
            description="", // TODO
            defaultNull=true,
            experimental=false
        )
        public String markov_path;
        
        @ConfigProperty(
            description="If this is set to true, TransactionEstimator will try to reuse MarkovPathEstimators" +
                        "for transactions running at the same partition.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean markov_path_caching;
    
        @ConfigProperty(
            description="This threshold defines how accurate our cached MarkovPathEstimators have to be in order " +
                        "to keep using them. If (# of accurate txs / total txns) for a paritucular MarkovGraph " +
                        "goes below this threshold, then we will disable the caching",
            defaultDouble=1.0,
            experimental=true
        )
        public double markov_path_caching_threshold;
        
        @ConfigProperty(
            description="The minimum number of queries that must be in a batch for the TransactionEstimator " +
                        "to cache the path segment in the procedure's MarkovGraph. Provides a minor speed improvement " +
                        "for large batches with little variability in their execution paths.",
            defaultInt=3,
            experimental=true
        )
        public int markov_batch_caching_min;
        
        @ConfigProperty(
            description="Enable a hack for TPC-C where we inspect the arguments of the TPC-C neworder transaction and figure " +
                        "out what partitions it needs without having to use the TransactionEstimator. This will crash the " +
                        "system when used with other benchmarks. See edu.brown.hstore.util.NewOrderInspector",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_neworder_cheat;

        // ----------------------------------------------------------------------------
        // BatchPlanner
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable BatchPlanner profiling. This will keep of how long the BatchPlanner spends performing " +
                        "certain operations.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean planner_profiling;
        
        @ConfigProperty(
            description="Enable caching in the BatchPlanner. This will provide a significant speed improvement for " +
                        "single-partitioned queries because the BatchPlanner is able to quickly identify what partitions " +
                        "a batch of queries will access without having to process the request using the PartitionEstimator. " +
                        "This parameter is so great I should probably just hardcode to be always on, but maybe you don't " +
                        "believe me and want to see how slow things go with out this...",
            defaultBoolean=true,
            experimental=false
        )
        public boolean planner_caching;
        
        @ConfigProperty(
            description="The maximum number of execution rounds allowed per batch.",
            defaultInt=10,
            experimental=false
        )
        public int planner_max_round_size;
        
        @ConfigProperty(
            description="The maximum number of SQLStmts that can be queued per batch in a transaction.",
            defaultInt=128,
            experimental=false
        )
        public int planner_max_batch_size;
        
        @ConfigProperty(
            description="Use globally unique Dependency Ids for each unique SQLStmt batch when generating WorkFragments " +
                        "at run time.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean planner_unique_dependency_ids;
        
        // ----------------------------------------------------------------------------
        // HStoreCoordinator
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If this enabled, HStoreCoordinator will use a separate thread to process incoming initialization " +
                        "requests from other HStoreSites. This is useful when ${client.txn_hints} is disabled.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean coordinator_init_thread;
        
        @ConfigProperty(
            description="If this enabled, HStoreCoordinator will use a separate thread to process incoming finish " +
                        "requests for restarted transactions from other HStoreSites. ",
            defaultBoolean=false,
            experimental=false
        )
        public boolean coordinator_finish_thread;
        
        @ConfigProperty(
            description="If this enabled, HStoreCoordinator will use a separate thread to process incoming redirect " +
                        "requests from other HStoreSites. This is useful when ${client.txn_hints} is disabled.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean coordinator_redirect_thread;
        
        @ConfigProperty(
            description="If this enabled, HStoreCoordinator will use an NTP sytle protocol to find the time difference " +
                        "between sites.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean coordinator_sync_time;

        // ----------------------------------------------------------------------------
        // PartitionExecutorHelper
        // ----------------------------------------------------------------------------
    
        @ConfigProperty(
            description="How many ms to wait initially before starting the PartitionExecutorHelper after " +
                        "the HStoreSite has started.",
            defaultInt=2000,
            experimental=true
        )
        public int helper_initial_delay;
        
        @ConfigProperty(
            description="How often (in ms) should the PartitionExecutorHelper execute to clean up completed transactions.",
            defaultInt=100,
            experimental=false
        )
        public int helper_interval;
        
        @ConfigProperty(
            description="How many txns can the PartitionExecutorHelper clean-up per partition per round. Any value less " +
                        "than zero means that it will clean-up all txns it can per round",
            defaultInt=-1,
            experimental=true
        )
        public int helper_txn_per_round;
        
        @ConfigProperty(
            description="The amount of time after a transaction completes before its resources can be garbage collected " +
                        "and returned back to the various object pools in the HStoreSite.",
            defaultInt=100,
            experimental=true
        )
        public int helper_txn_expire;
        
        // ----------------------------------------------------------------------------
        // Output Tracing
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="When this property is set to true, all TransactionTrace records will include the stored procedure output result",
            defaultBoolean=false,
            experimental=false
        )
        public boolean trace_txn_output;

        @ConfigProperty(
            description="When this property is set to true, all QueryTrace records will include the query output result",
            defaultBoolean=false,
            experimental=false
        )
        public boolean trace_query_output;
        
        // ----------------------------------------------------------------------------
        // HSTORESITE STATUS UPDATES
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable HStoreSite's Status thread.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean status_enable;
        
        @ConfigProperty(
            description="How often the HStoreSite's StatusThread will print a status update (in milliseconds).",
            defaultInt=20000,
            experimental=false
        )
        public int status_interval;

        @ConfigProperty(
            description="Allow the HStoreSiteStatus thread to kill the cluster if the local HStoreSite appears to be hung. " +
                        "The site is considered hung if it has executed at least one transaction and has " +
                        "not completed (either committed or aborted) any new transactions since the last time " +
                        "it took a status snapshot.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean status_kill_if_hung;
        
        @ConfigProperty(
            description="Allow the HStoreSiteStatus thread to check whether there any zombie transactions. " +
                        "This can occur if the transaction has already sent back the ClientResponse, but " +
                        "its internal state has not been cleaned up.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean status_check_for_zombies;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include transaction information",
            defaultBoolean=false,
            experimental=false
        )
        public boolean status_show_txn_info;

        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include information about each PartitionExecutor, " +
                        "such as the number of transactions currently queued, blocked for execution, or waiting to have their results " +
                        "returned to the client.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean status_show_executor_info;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include a snapshot of running threads",
            defaultBoolean=false,
            experimental=false
        )
        public boolean status_show_thread_info;
        
        // ----------------------------------------------------------------------------
        // OBJECT POOLS
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="The scale factor to apply to the object pool configuration values.",
            defaultDouble=1.0,
            experimental=false
        )
        public double pool_scale_factor;
        
        @ConfigProperty(
            description="Whether to track the number of objects created, passivated, and destroyed from the pool. " + 
                        "Results are shown in HStoreSiteStatus updates.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean pool_profiling;
        
        @ConfigProperty(
            description="The max number of LocalTransactionStates to keep in the pool",
            defaultInt=5000,
            experimental=false
        )
        public int pool_localtxnstate_idle;
        
        @ConfigProperty(
            description="The max number of MapReduceTransactionStates to keep in the pool",
            defaultInt=100,
            experimental=false
        )
        public int pool_mapreducetxnstate_idle;
        
        @ConfigProperty(
            description="The max number of RemoteTransactionStates to keep in the pool",
            defaultInt=500,
            experimental=false
        )
        public int pool_remotetxnstate_idle;
        
        @ConfigProperty(
            description="The max number of MarkovPathEstimators to keep in the pool",
            defaultInt=1000,
            experimental=false
        )
        public int pool_pathestimators_idle;
        
        @ConfigProperty(
            description="The max number of TransactionEstimator.States to keep in the pool. " + 
                        "Should be the same as the number of MarkovPathEstimators.",
            defaultInt=1000,
            experimental=false
        )
        public int pool_estimatorstates_idle;
        
        @ConfigProperty(
            description="The max number of DependencyInfos to keep in the pool. " +
                        "Should be the same as the number of MarkovPathEstimators. ",
            defaultInt=500,
            experimental=false
        )
        public int pool_dependencyinfos_idle;
        
        @ConfigProperty(
            description="The max number of DistributedStates to keep in the pool.",
            defaultInt=500,
            experimental=false
        )
        public int pool_dtxnstates_idle;
        
        @ConfigProperty(
            description="The max number of PrefetchStates to keep in the pool.",
            defaultInt=100,
            experimental=false
        )
        public int pool_prefetchstates_idle;
        
        @ConfigProperty(
            description="The max number of TransactionRedirectCallbacks to keep idle in the pool",
            defaultInt=10000,
            experimental=false
        )
        public int pool_txnredirect_idle;
        
        @ConfigProperty(
            description="The max number of TransactionRedirectResponseCallbacks to keep idle in the pool.",
            defaultInt=2500,
            experimental=false
        )
        public int pool_txnredirectresponses_idle;
        
        @ConfigProperty(
            description="The max number of TransactionInitCallbacks to keep idle in the pool.",
            defaultInt=2500,
            experimental=false
        )
        @Deprecated
        public int pool_txninit_idle;
        
        @ConfigProperty(
            description="The max number of TransactionInitWrapperCallbacks to keep idle in the pool.",
            defaultInt=2500,
            experimental=false,
            replacedBy="site.pool_txninitqueue_idle"
        )
        public int pool_txninitwrapper_idle;
        
        @ConfigProperty(
            description="The max number of TransactionInitQueueCallbacks to keep idle in the pool.",
            defaultInt=2500,
            experimental=false
        )
        public int pool_txninitqueue_idle;
        
        @ConfigProperty(
            description="The max number of TransactionPrepareCallbacks to keep idle in the pool.",
            defaultInt=2500,
            experimental=false
        )
        @Deprecated
        public int pool_txnprepare_idle;
    }
    
    // ============================================================================
    // CLIENT
    // ============================================================================
    public final class ClientConf extends Conf {
        
        @ConfigProperty(
            description="Benchmark client log directory on the host that the BenchmarkController " +
                        "is invoked from.",
            defaultString="${global.temp_dir}/logs/clients",
            experimental=false
        )
        public String log_dir = HStoreConf.this.global.temp_dir + "/logs/clients";
        
        @ConfigProperty(
            description="Whether to back-up log files before the benchmark is exceuted",
            defaultBoolean=false,
            experimental=false
        )
        public boolean log_backup;
        
        @ConfigProperty(
            description="The directory that benchmark project jars will be stored in.",
            defaultString="",
            experimental=false
        )
        public String jar_dir = ".";
        
        @ConfigProperty(
            description="The amount of memory to allocate for each client process (in MB)",
            defaultInt=512,
            experimental=false
        )
        public int memory;

        @ConfigProperty(
            description="Default client host name",
            defaultString="${global.defaulthost}",
            experimental=false
        )
        public String host = HStoreConf.this.global.defaulthost;

        @ConfigProperty(
            description="The number of txns that client process submits (per ms). The underlying " +
                        "BenchmarkComponent will continue invoke the client driver's runOnce() method " +
                        "until it has submitted enough transactions to satisfy ${client.txnrate}. " +
                        "If ${client.blocking} is disabled, then the total transaction rate for a benchmark run is " +
                        "${client.txnrate} * ${client.processesperclient} * ${client.count}.",
            defaultInt=10000,
            experimental=false
        )
        public int txnrate;
        
        @ConfigProperty(
            description="", // TODO
            defaultNull=true,
            experimental=false
        )
        public String weights;

        @ConfigProperty(
            description="Number of processes to use per client host.",
            defaultInt=10,
            experimental=false
        )
        public int processesperclient;
        
        @ConfigProperty(
            description="Multiply the ${client.processesperclient} parameter by " +
                        "the number of partitions in the target cluster.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean processesperclient_per_partition;

        @ConfigProperty(
            description="Number of clients hosts to use in the benchmark run.",
            defaultInt=1,
            experimental=false
        )
        public int count;

        @ConfigProperty(
            description="How long should the benchmark trial run (in milliseconds). Does not " +
                        "include ${client.warmup time}.",
            defaultInt=60000,
            experimental=false
        )
        public int duration;

        @ConfigProperty(
            description="How long should the system be allowed to warmup (in milliseconds). Any stats " +
                        "collected during this period are not counted in the final totals.",
            defaultInt=0,
            experimental=false
        )
        public int warmup;

        @ConfigProperty(
            description="How often (in milliseconds) should the BenchmarkController poll the individual " +
                        "client processes and get their intermediate results.",
            defaultInt=10000,
            experimental=false
        )
        public int interval;

        @ConfigProperty(
            description="Whether to use the BlockingClient. When this is true, then each client process will " +
                        "submit one transaction at a time and wait until the result is returned before " +
                        "submitting the next. The clients still follow the ${client.txnrate} parameter.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean blocking;
        
        @ConfigProperty(
            description="When the BlockingClient is enabled with ${client.blocking}, this defines the number " +
                        "of concurrent transactions that each client instance can submit to the H-Store cluster " +
                        "before it will block.",
            defaultInt=1,
            experimental=false
        )
        public int blocking_concurrent;
        
        @ConfigProperty(
            description="When this parameter is enabled, the benchmark's loaders will only be " +
                        "allowed to load tables into the database cluster one at a time. This is " +
                        "only useful for debugging.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean blocking_loader;

        @ConfigProperty(
            description="The scaling factor determines how large to make the target benchmark's data set. " +
                        "A scalefactor greater than one makes the data set larger, while less than one " +
                        "makes it smaller. Implementation depends on benchmark specification.",
            defaultDouble=0.1,
            experimental=false
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
            experimental=false
        )
        public int tick_interval;

        @ConfigProperty(
            description="The amount of time (in ms) that the client will back-off from sending requests " +
                        "to an HStoreSite when told that the site is throttled.",
            defaultInt=500,
            experimental=false
        )
        public int throttle_backoff;
        
        @ConfigProperty(
            description="If this enabled, then each DBMS will dump their entire database contents into " +
                        "CSV files after executing a benchmark run.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean dump_database;
        
        @ConfigProperty(
            description="If ${client.dump_database} is enabled, then each DBMS will dump their entire " +
                        "database contents into CSV files in the this directory after executing a benchmark run.",
            defaultString="${global.temp_dir}/dumps",
            experimental=false
        )
        public String dump_database_dir = HStoreConf.this.global.temp_dir + "/dumps";
        
        @ConfigProperty(
            description="If set to true, then the benchmark data loader will generate a WorkloadStatistics " +
                        "based on the data uploaded to the server. These stats will be written to the path " +
                        "specified by ${client.tablestats_output}.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean tablestats;
        
        @ConfigProperty(
            description="If ${client.tablestats} is enabled, then the loader will write out a database statistics " +
                        "file in the directory defined in this parameter.",
            defaultString="${global.temp_dir}/stats",
            experimental=false
        )
        public String tablestats_dir = HStoreConf.this.global.temp_dir + "/stats";
        
        @ConfigProperty(
            description="If this parameter is set to true, then each the client will calculate the base partition " +
                        "needed by each transaction request before it sends to the DBMS. This base partition is " +
                        "embedded in the StoreProcedureInvocation wrapper and is automatically sent to the HStoreSite " +
                        "that has that partition. Note that the HStoreSite will not use the PartitionEstimator to " +
                        "determine whether the client is correct, but the transaction can be restarted and re-executed " +
                        "if ${site.exec_db2_redirects} is enabled.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean txn_hints;
        
        @ConfigProperty(
            description="If a node is executing multiple client processes, then the node may become overloaded if " +
                        "all the clients are started at the same time. This parameter defines the threshold for when " +
                        "the BenchmarkController will stagger the start time of clients. For example, if a node will execute " +
                        "ten clients and ${client.delay_threshold} is set to five, then the first five processes will start " +
                        "right away and the remaining five will wait until the first ones finish before starting themselves.", 
            defaultInt=8,
            experimental=false
        )
        public int delay_threshold;
        
        @ConfigProperty(
            description="The URL of the CodeSpeed site that the H-Store BenchmarkController will post the transaction " +
                        "throughput rate after a benchmark invocation finishes. This parameter must be a well-formed HTTP URL. " +
                        "See the CodeSpeed documentation page for more info (https://github.com/tobami/codespeed).", 
            defaultNull=true,
            experimental=false
        )
        public String codespeed_url;
        
        @ConfigProperty(
            description="The name of the project to use when posting the benchmark result to CodeSpeed." +
                        "This parameter is required by CodeSpeed and cannot be empty. " +
                        "Note that the the ${client.codespeed_url} parameter must also be set.", 
            defaultString="H-Store",
            experimental=false
        )
        public String codespeed_project;
        
        @ConfigProperty(
            description="The name of the environment to use when posting the benchmark result to CodeSpeed. " +
                        "The value of this parameter must already exist in the CodeSpeed site. " +
                        "This parameter is required by CodeSpeed and cannot be empty. " +
                        "Note that the the ${client.codespeed_url} parameter must also be set.",
            defaultNull=true,
            experimental=false
        )
        public String codespeed_environment;

        @ConfigProperty(
            description="The name of the executable to use when posting the benchmark result to CodeSpeed. " +
                        "This parameter is required by CodeSpeed and cannot be empty. " +
                        "Note that the the ${client.codespeed_url} parameter must also be set.",
            defaultNull=true,
            experimental=false
        )
        public String codespeed_executable;
        
        @ConfigProperty(
            description="The Subversion revision number of the H-Store source code that is reported " +
                        "when posting the benchmark result used to CodeSpeed. " +
                        "This parameter is required by CodeSpeed and cannot be empty. " +
                        "Note that the the ${client.codespeed_url} parameter must also be set.", 
            defaultNull=true,
            experimental=false
        )
        public String codespeed_commitid;
        
        @ConfigProperty(
            description="The branch corresponding for this version of H-Store used when posting the benchmark " +
                        "result to CodeSpeed. This is parameter is optional.",
            defaultNull=true,
            experimental=false
        )
        public String codespeed_branch;
        
        @ConfigProperty(
            description="",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_clients;
        
        @ConfigProperty(
            description="",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_basepartitions;
        
        @ConfigProperty(
            description="",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_json;
    }
    
    /**
     * Base Configuration Class
     */
    protected abstract class Conf {
        
        final String prefix;
        final Class<? extends Conf> confClass; 
        
        {
            this.confClass = this.getClass();
            this.prefix = confClass.getSimpleName().replace("Conf", "").toLowerCase();
            HStoreConf.this.confHandles.put(this.prefix, this);
            this.setDefaultValues();
        }
        
        protected Map<Field, ConfigProperty> getConfigProperties() {
            return ClassUtil.getFieldAnnotations(confClass.getFields(), ConfigProperty.class);
        }
        
        private void setDefaultValues() {
            // Set the default values for the parameters based on their annotations
            for (Entry<Field, ConfigProperty> e : this.getConfigProperties().entrySet()) {
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
        
        /**
         * Returns true if this configuration handle as a parameter for the given name
         * @param name
         * @return
         */
        public boolean hasParameter(String name) {
            try {
                return (this.confClass.getField(name) != null);
            } catch (NoSuchFieldException ex) {
                return (false);
            }
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
            return (this.toString(false));
        }
        
        public String toString(boolean experimental) {
            final Map<String, Object> m = new TreeMap<String, Object>();
            for (Entry<Field, ConfigProperty> e : this.getConfigProperties().entrySet()) {
                ConfigProperty cp = e.getValue();
                if (experimental == false && cp.experimental()) continue;
                
                Field f = e.getKey();
                String key = f.getName().toUpperCase();
                Object val = null;
                try {
                    val = f.get(this);
                    if (isLoadedFromArgs(this, f.getName())) {
                        String val_str = (val != null ? val.toString() : "null");
                        val_str += "   **EXTERNAL**";
                        val = val_str;
                    }
                } catch (IllegalAccessException ex) {
                    m.put(key, ex.getMessage());
                }
                m.put(key, val);
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
    public final ClientConf client = new ClientConf();
    
    /**
     * Singleton Object
     */
    private static HStoreConf conf;
    
    private final Map<Conf, Set<String>> loaded_params = new HashMap<Conf, Set<String>>();
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
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
            
            // Markov Path
            if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
                this.site.markov_path = args.getParam(ArgumentsParser.PARAM_MARKOV);
            }
            
            // ParameterMappings Path
            if (args.hasParam(ArgumentsParser.PARAM_MAPPINGS)) {
                this.site.mappings_path = args.getParam(ArgumentsParser.PARAM_MAPPINGS);
            }
            
            Map<String, String> confParams = args.getHStoreConfParameters();
            if (confParams != null && confParams.isEmpty() == false) {
                this.loadFromArgs(confParams);
            }
            
        }
        // TODO: Remove
        if (site.exec_neworder_cheat) {
            site.exec_force_singlepartitioned = false;
            site.exec_force_localexecution = false;
        }
    }
    
    // ----------------------------------------------------------------------------
    // LOADING METHODS
    // ----------------------------------------------------------------------------
    
    public void set(String k, Object value) {
        Matcher m = REGEX_PARSE.matcher(k);
        boolean found = m.matches();
        if (m == null || found == false) {
            LOG.warn("Invalid key '" + k + "'");
            return;
        }
        assert(m != null);
        Conf handle = confHandles.get(m.group(1));
        this.set(handle, m.group(2), value);
    }
    
    private void set(Conf handle, String f_name, Object value) {
        Class<?> confClass = handle.getClass();
        assert(confClass != null);
        Field f = null;
        
        try {
            f = confClass.getField(f_name);
        } catch (Exception ex) {
            if (debug.get()) LOG.warn(String.format("Invalid configuration property '%s.%s'. Ignoring...",
                                      handle.prefix, f_name));
            return;
        }
        ConfigProperty cp = handle.getConfigProperties().get(f);
        assert(cp != null) : "Missing ConfigProperty for " + f;
        
        try {
            f.set(handle, value);
            if (debug.get()) LOG.debug(String.format("SET %s.%s = %s",
                                       handle.prefix, f_name, value));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to set value '" + value + "' for field '" + f_name + "'", ex);
        }
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

        for (Object obj_k : CollectionUtil.iterable(this.config.getKeys())) {
            String k = obj_k.toString();
            Matcher m = REGEX_PARSE.matcher(k);
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
            ConfigProperty cp = handle.getConfigProperties().get(f);
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
        for (Entry<String, String> e : args.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            
            Matcher m = REGEX_PARSE.matcher(k);
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
            ConfigProperty cp = confHandle.getConfigProperties().get(f);
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
                if (val != null) m.put(String.format("%s.%s", confHandle.prefix, f_name), val.toString());
            } // FOR
        } // FOR
        return (m);
    }
    
    protected Object getDefaultValue(Field f, ConfigProperty cp) {
        Class<?> f_class = f.getType();
        Object value = null;
        
        if (cp.defaultNull() == false) {
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
        }
        return (value);
    }
    
    /**
     * Returns true if the given parameter name was loaded from an input argument
     * @param confHandle
     * @param name
     * @return
     */
    private boolean isLoadedFromArgs(Conf confHandle, String name) {
        Set<String> params = this.loaded_params.get(confHandle);
        if (params != null) {
            return (params.contains(name));
        }
        return (false);
    }
    

    protected Map<String, Conf> getHandles() {
        return (this.confHandles);
    }
    
    @Override
    public String toString() {
        return (this.toString(false));
    }
        
    public String toString(boolean experimental) {
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
                m.put(key, ((Conf)obj).toString(experimental));
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
    
    public static HStoreConf initArgumentsParser(ArgumentsParser args) {
        return HStoreConf.initArgumentsParser(args, null);
    }
    
    public synchronized static HStoreConf initArgumentsParser(ArgumentsParser args, Site catalog_site) {
        if (conf != null) throw new RuntimeException("Trying to initialize HStoreConf more than once");
        conf = new HStoreConf(args, catalog_site);
        return (conf);
    }
    
    public synchronized static HStoreConf singleton() {
        return singleton(false);
    }
    
    public synchronized static HStoreConf singleton(boolean init_if_null) {
        if (conf == null && init_if_null == true) return init(null, null);
        if (conf == null) throw new RuntimeException("Requesting HStoreConf before it is initialized");
        return (conf);
    }
    
    public synchronized static boolean isInitialized() {
        return (conf != null);
    }
    
    private static HStoreConf confHelper;
    public static boolean isConfParameter(String name) {
        Matcher m = REGEX_PARSE.matcher(name);
        if (m.find()) {
            if (confHelper == null) {
                synchronized (HStoreConf.class) {
                    if (confHelper == null) {
                        confHelper = new HStoreConf();
                    }
                } // SYNCH
            }

            Conf c = confHelper.confHandles.get(m.group(1));
            assert(c != null) : "Unexpected null Conf for '" + m.group(1) + "'";
            return (c.hasParameter(m.group(2)));
        }
        return (false);
    }

}
