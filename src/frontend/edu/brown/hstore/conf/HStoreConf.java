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

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Regular expression for splitting a parameter into
     * prefix and suffix components
     */
    protected static final String REGEX_STR = "(site|client|global)\\.([\\w\\_]+)";
    protected static final Pattern REGEX_PARSE = Pattern.compile(REGEX_STR);
    
    // ============================================================================
    // GLOBAL
    // ============================================================================
    public final class GlobalConf extends Conf {
        
        @ConfigProperty(
            description="What version of the Java JDK should be used in this H-Store installation. " +
                        "Accepted values are '1.6' or '1.7'. This will only be used when compiling " +
                        "the system source code. It is up to you to configure your environment " +
                        "appropriately to match whatever this is used for this option.",
            defaultString="1.7",
            experimental=false
        )
        public String jvm_version;
        
        @ConfigProperty(
            description="Temporary directory used to store various artifacts related to H-Store.",
            defaultString="obj",
            experimental=false
        )
        public String temp_dir;
        
        @ConfigProperty(
            description="Default log directory for H-Store.",
            defaultString="${global.temp_dir}/logs",
            experimental=false
        )
        public String log_dir;

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
        
        @ConfigProperty(
            description="How often in milliseconds the log4j refresh thread will check to see " +
            		    "whether the log4j.properties file has changed. We have to do this manually " +
            		    "because Java doesn't have the ability to get a callback when a file changes.",
            defaultInt=30000,
            experimental=false
        )
        public int log_refresh;
        
        @ConfigProperty(
            description="Measure all latencies using nanoseconds instead of milliseconds.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean nanosecond_latencies;

    }
    
    // ============================================================================
    // SITE
    // ============================================================================
    public final class SiteConf extends Conf {
    
        @ConfigProperty(
            description="HStoreSite log directory on the host that the BenchmarkController is invoked from.",
            defaultString="${global.log_dir}/sites",
            experimental=false
        )
        public String log_dir;
        
        @ConfigProperty(
            description="Whether to back-up log files before the benchmark is exceuted",
            defaultBoolean=false,
            experimental=false
        )
        public boolean log_backup;
        
        @ConfigProperty(
            description="Execute each HStoreSite with JVM asserts enabled. " +
            		    "This should be set to false when running benchmark experiments.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean jvm_asserts;
        
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
                        "other PartitionExecutors) will be allowed to execute on that core. This configuration " +
                        "option is mostly used for debugging and is unlikely to provide any speed improvement " +
                        "because the operating system will automatically maintain CPU affinity.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean cpu_affinity_one_partition_per_core;
        
        @ConfigProperty(
            description="Enable profiling for the HStoreSite. " +
                        "This data can be retrieved using the @Statistics sysproc.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean profiling;
        
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
            description="Enable execution site profiling. This will keep track of how busy each PartitionExecutor " +
            		    "thread is during execution (i.e., the percentage of time that it spends executing a " +
            		    "transaction versus waiting for work to be added to its queue).",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_profiling;
        
        @ConfigProperty(
            description="If this feature is enabled, then each HStoreSite will attempt to speculatively execute " +
                        "single-partition transactions whenever it completes a work request for a multi-partition " +
                        "transaction running on a different node.",
            defaultBoolean=true,
            replacedBy="site.specexec_enable",
            experimental=true
        )
        @Deprecated
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
            defaultBoolean=true,
            experimental=true
        )
        public boolean exec_force_undo_logging_all;
        
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
            description="Always execute transactions as single-partitioned (excluding sysprocs). If a transaction " +
            		    "requests data on a partition that is different than where it is executing, then it is " +
            		    "aborted, rolled back, and re-executed on the same partition as a multi-partition transaction " +
            		    "that touches all partitions. Note that this is independent of how H-Store decides what" +
            		    "partition to execute the transaction's Java control code on.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean exec_force_singlepartitioned;
        
        @ConfigProperty(
            description="Always execute all requests as distributed transactions that lock all " +
                        "partitions in the cluster.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_force_allpartitions;
        
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
            description="Always execute each transaction on a random partition on the node where the request " +
            		    "originally arrived on. Note that this is independent of whether the transaction is " +
            		    "selected to be single-partitioned or not. " +
            		    "It is likely that you do not want to use this option.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_force_localexecution;
    
        @ConfigProperty(
            description="Whether the VoltProcedure should crash the HStoreSite when a transaction is mispredicted. " +
            		    "A mispredicted transaction is one that was originally identified as single-partitioned " +
            		    "but then executed a query that attempted to access multiple partitions. This is primarily " +
            		    "used for debugging the TransactionEstimator.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_mispredict_crash;
        
        @ConfigProperty(
            description="If this enabled, HStoreSite will use a separate thread to process inbound requests " +
            		    "from the clients.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean exec_preprocessing_threads;
        
        @ConfigProperty(
            description="The number of TransactionPreProcessor threads to use per HStoreSite. " +
                        "If this parameter is set to -1, then the system will automatically use all " +
                        "of the non-PartitionExecutor cores for these processing threads. " +
                        "The ${site.exec_preprocessing_threads} parameter must be set to true. ",
            defaultInt=-1,
            experimental=true
        )
        public int exec_preprocessing_threads_count;
        
        @ConfigProperty(
            description="Use a single TransactionPostProcessor thread per partition on the HStoreSite. " +
                        "The ${site.exec_postprocessing_thread} parameter must be set to true.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean exec_postprocessing_threads;
        
        @ConfigProperty(
            description="The number of TransactionPostProcessor threads to use per HStoreSite. " +
                        "If this parameter is set to -1, then the system will automatically use all " +
                        "of the non-PartitionExecutor cores for these processing threads. " +
                        "The ${site.exec_postprocessing_threads} parameter must be set to true. ",
            defaultInt=-1,
            experimental=true
        )
        public int exec_postprocessing_threads_count;
        
        @ConfigProperty(
            description="If this enabled with speculative execution, then HStoreSite only invoke the commit " +
            		    "operation in the EE for the last transaction in the queued responses. This will cascade " +
            		    "to all other queued responses successful transactions that were speculatively executed.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean exec_queued_response_ee_bypass;
        
        @ConfigProperty(
            description="The maximum amount of time that the PartitionExecutor will wait for the results of a " +
            		    "distributed query to return to the transaction's base partition. Usually if this limit " +
            		    "is reached, then there is something very wrong with the distributed transaction protocol.",
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
            replacedBy="site.commandlog_enable",
            experimental=true
        )
        @Deprecated
        public boolean exec_command_logging;
        
        @ConfigProperty(
            description="Directory for storage of command logging files",
            defaultString="${global.temp_dir}/wal",
            replacedBy="site.commandlog_dir",
            experimental=true
        )
        @Deprecated
        public String exec_command_logging_directory = HStoreConf.this.global.temp_dir + "/wal";
        
        @ConfigProperty(
            description="Timeout in milliseconds before group commit buffer flushes, if it does not fill",
            defaultInt=500,
            replacedBy="site.commandlog_timeout",
            experimental=true
        )
        @Deprecated
        public int exec_command_logging_group_commit_timeout;
        
        @ConfigProperty(
            description="If enabled, then the CommandLogWriter will keep track of various internal " +
            		    "profile statistics.",
            defaultBoolean=false,
            replacedBy="site.commandlog_profiling",
            experimental=true
        )
        @Deprecated
        public boolean exec_command_logging_profile;
        
        @ConfigProperty(
            description="Setting this configuration parameter to true allows clients to " +
                        "issue ad hoc query requests use the @AdHoc sysproc. This should be " +
                        "set to false if you are running benchmarking experiments because it " +
                        "will reduce the number of threads that are started per HStoreSite.",
            defaultBoolean=true,
            experimental=false
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
        // Speculative Execution Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If this feature is enabled, then each HStoreSite will attempt to speculatively execute " +
                        "single-partition transactions whenever it completes a work request for a multi-partition " +
                        "transaction running on a different node.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean specexec_enable;
        
        @ConfigProperty(
            description="",
            defaultBoolean=false,
            experimental=true
        )
        public boolean specexec_idle;
        
        @ConfigProperty(
            description="If this parameter is true, then the SpecExecScheduler will not attempt to " +
                        "speculatively execute any transactions if the current distributed transaction " +
                        "is using only partitions that are all on the same site.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean specexec_ignore_all_local;
        
        @ConfigProperty(
            description="Special non-blocking remote query execution.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean specexec_nonblocking;
        
        @ConfigProperty(
            description="Use the row-based MarkovConflictChecker to determine whether queued transactions " +
            		    "conflict with the current distributed transaction. This is used to selecte " +
            		    "speculative execution candidates at runtime. " +
            		    "Note that ${site.markov_enable} must be set to true.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean specexec_markov;
        
        @ConfigProperty(
            description="If enabled, then the SpecExecScheduler will keep track of various internal " +
                        "profile statistics.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean specexec_profiling;
        
        @ConfigProperty(
            description="Speculative pollicy to pick the transactions to run speculatively. " +
                        "Allowed values are 'FIRST', 'SHORTEST', or 'LONGEST'." ,
            defaultString="FIRST",
            experimental=false
        )
        public String specexec_scheduler_policy;
        
        @ConfigProperty(
            description="The window size to pick up txn to run speculatively. ",
            defaultInt= 10,
            experimental=false
        )
        public int specexec_scheduler_window;
        
        // ----------------------------------------------------------------------------
        // Command Logging Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If enabled, log all transaction requests to disk",
            defaultBoolean=false,
            experimental=true
        )
        public boolean commandlog_enable;
        
        @ConfigProperty(
            description="Directory for storage of command logging files",
            defaultString="${global.temp_dir}/wal",
            experimental=true
        )
        public String commandlog_dir;
        
        @ConfigProperty(
            description="Timeout in milliseconds before group commit buffer flushes, if it does not fill",
            defaultInt=100,
            experimental=true
        )
        public int commandlog_timeout;
        
        @ConfigProperty(
            description="If enabled, then the CommandLogWriter will keep track of various internal " +
                        "profile statistics.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean commandlog_profiling;
        
        // ----------------------------------------------------------------------------
        // AntiCache Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable the anti-cache feature.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean anticache_enable;
        
        @ConfigProperty(
            description="The directory to use to store the evicted tuples.",
            defaultString="${global.temp_dir}/anticache",
            experimental=true
        )
        public String anticache_dir;
        
        @ConfigProperty(
            description="Reset the anti-cache database directory for each partition when " +
            		    "the HStoreSite is started.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean anticache_reset;
        
        @ConfigProperty(
            description="How often in milliseconds should the AntiCacheManager check whether " +
            		    "the HStoreSite is using too much memory and should start evicting tuples.",
            defaultInt=30000,
            experimental=true
        )
        public int anticache_check_interval;
        
        @ConfigProperty(
            description="", // TODO
            defaultDouble=0.75,
            experimental=true
        )
        public double anticache_threshold;
        
        // ----------------------------------------------------------------------------
        // MapReduce Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
                description="If set to true, then the MAP phase of a MapReduceTransaction will be " +
                		    "executed as a distributed transaction that blocks the entire cluster. This " +
                		    "ensures that the aggregates computed by the MAP phase reads from consistent " +
                		    "a consistent state of the database.",
                defaultBoolean=true,
                experimental=true
        )
        public boolean mr_map_blocking;
        
        @ConfigProperty(
                description="The way to execute reduce job, blocking or non-blocking by MapReduceHelperThread",
                defaultBoolean=true,
                experimental=true
        )
        public boolean mr_reduce_blocking;

        // ----------------------------------------------------------------------------
        // Networking Options
        // ----------------------------------------------------------------------------

        @ConfigProperty(
            description="How long in milliseconds should the HStoreCoordinator wait to establish " +
            		    "the initial connections to other nodes in the cluster at start-up. " +
            		    "Increasing this number will help with larger cluster deployments.",
            defaultInt=15000,
            experimental=false
        )
        public int network_startup_wait;
        
        @ConfigProperty(
            description="If the HStoreCoordinator fails to connect to all of the other " +
            		    "nodes in the cluster after ${site.network_startup_wait} has passed, " +
            		    "this parameter defines the number of times that it is allowed to attempt " +
            		    "to reconnect to them. This helps with some rare network issues with the " +
            		    "ProtoRpc framework where the initial network connection attempt hangs " +
            		    "or fails, even though both sites are available.",
            defaultInt=2,
            experimental=false
        )
        public int network_startup_retries;
        
        @ConfigProperty(
            description="If set to true, then incoming transaction requests will be processed " +
                        "using the TransactionInitializer using the same thread that processed " +
                        "network messages from the client. Otherwise, the transactions will be " +
                        "processed using the PartitionExecutor's thread. " +
                        "It is not clear which approach is better.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean network_txn_initialization;
        
        @ConfigProperty(
            description="Max size of queued transactions before an HStoreSite will stop accepting new requests " +
                        "from clients and will block the network connections.",
            defaultInt=1000,
            experimental=false
        )
        public int network_incoming_max_per_partition;
        
        // ----------------------------------------------------------------------------
        // Transaction Execution Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="If this parameter is set to true, then the ClientResponse returned by the " +
            		    "server will include a special ClientResponseDebug handle that contains " +
            		    "additional information about the transaction. " + 
            		    "Note that enabling this option will break compatibility with VoltDB's " +
            		    "client libraries.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean txn_client_debug;
        
        @ConfigProperty(
            description="Enable transaction profiling. This will measure the amount of time a " +
            		    "transaction spends in different parts of the system (e.g., waiting in " +
            		    "the work queue, planning, executing).",
            defaultBoolean=false,
            experimental=false
        )
        public boolean txn_profiling;
        
        @ConfigProperty(
            description="Enable transaction execution mode counting. This will cause the HStoreSite to keep " +
            		    "track of various properties about tranasctions, such as the number that were speculatively " +
            		    "executed or had to be restarted.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean txn_counters;
        
        @ConfigProperty(
            description="The amount of time in milliseconds that the TransactionQueueManager will wait " +
            		    "before letting a distributed transaction acquire a lock on a partition.",
            defaultInt=5,
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
            description="If set to true, then the HStoreSite will use a separate TransactionIdManager" +
            		    "per partition. This can reduce some lock contention for workloads where " +
            		    "transactions are restarted a lot. This actually doesn't work very well, " +
            		    "so you probably do not want to bother with this parameter.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean txn_partition_id_managers;
        
        // ----------------------------------------------------------------------------
        // Transaction Queue Options
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable profiling in the TransactionQueueManager.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean queue_profiling;
        
//        @ConfigProperty(
//            description="Max size of queued transactions before an HStoreSite will stop accepting new requests " +
//                        "from clients and will send back a ClientResponse with the throttle flag enabled.",
//            defaultInt=250,
//            experimental=false
//        )
//        @Deprecated
//        public int queue_incoming_max_per_partition;
//        
//        @ConfigProperty(
//            description="If the HStoreSite is throttling incoming client requests, then that HStoreSite " +
//                        "will not accept new requests until the number of queued transactions is less than " +
//                        "this percentage. This includes all transactions that are waiting to be executed, " +
//                        "executing, and those that have already executed and are waiting for their results " +
//                        "to be sent back to the client. The incoming queue release is calculated as " +
//                        "${site.txn_incoming_queue_max} * ${site.txn_incoming_queue_release_factor}",
//            defaultDouble=0.75,
//            experimental=false
//        )
//        @Deprecated
//        public double queue_incoming_release_factor;
//        
//        @ConfigProperty(
//            description="Whenever a transaction completes, the HStoreSite will check whether the work queue " +
//                        "for that transaction's base partition is empty (i.e., the PartitionExecutor is idle). " +
//                        "If it is, then the HStoreSite will increase the ${site.txn_incoming_queue_max_per_partition} " +
//                        "value by this amount. The release limit will also be recalculated using the new value " +
//                        "for ${site.txn_incoming_queue_max_per_partition}. Note that this will only occur after " +
//                        "the first non-data loading transaction has been issued from the clients.",
//            defaultInt=100,
//            experimental=false
//        )
//        @Deprecated
//        public int queue_incoming_increase;
//        
//        @ConfigProperty(
//            description="The maximum amount that the ${site.queue_incoming_max_per_partition} parameter " +
//                        "can be increased by per partition.",
//            defaultInt=500,
//            experimental=false
//        )
//        @Deprecated
//        public int queue_incoming_increase_max;
//        
//        @ConfigProperty(
//            description="Max size of queued transactions before an HStoreSite will stop accepting new requests " +
//                        "from clients and will send back a ClientResponse with the throttle flag enabled.",
//            defaultInt=100,
//            experimental=false
//        )
//        @Deprecated
//        public int queue_dtxn_max_per_partition;
//        
//        @ConfigProperty(
//            description="If the HStoreSite is throttling incoming client requests, then that HStoreSite " +
//                        "will not accept new requests until the number of queued transactions is less than " +
//                        "this percentage. This includes all transactions that are waiting to be executed, " +
//                        "executing, and those that have already executed and are waiting for their results " +
//                        "to be sent back to the client. The incoming queue release is calculated as " +
//                        "${site.txn_incoming_queue_max} * ${site.txn_incoming_queue_release_factor}",
//            defaultDouble=0.50,
//            experimental=false
//        )
//        @Deprecated
//        public double queue_dtxn_release_factor;
//        
//        @ConfigProperty(
//            description="Whenever a transaction completes, the HStoreSite will check whether the work queue " +
//                        "for that transaction's base partition is empty (i.e., the PartitionExecutor is idle). " +
//                        "If it is, then the HStoreSite will increase the ${site.txn_incoming_queue_max_per_partition} " +
//                        "value by this amount. The release limit will also be recalculated using the new value " +
//                        "for ${site.txn_incoming_queue_max_per_partition}. Note that this will only occur after " +
//                        "the first non-data loading transaction has been issued from the clients.",
//            defaultInt=100,
//            experimental=false
//        )
//        @Deprecated
//        public int queue_dtxn_increase;
//        
//        @ConfigProperty(
//            description="The maximum amount that the ${site.queue_dtxn_max_per_partition} parameter " +
//                        "can be increased by per partition.",
//            defaultInt=500,
//            experimental=false
//        )
//        @Deprecated
//        public int queue_dtxn_increase_max;
        
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
            description="Predict what transactions will do before they execute using " +
                        "TransactionEstimator's Markov models.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean markov_enable;
        
        @ConfigProperty(
            description="If this parameter is set to true, then the PartitionExecutor will use its " +
                        "TransactionEstimator to calculate updated estimates after a single-partition " +
                        "transaction submits a new batch of queries for execution.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean markov_singlep_updates;
        
        @ConfigProperty(
            description="If this parameter is set to true, then the PartitionExecutor will use its " +
            		    "TransactionEstimator to calculate updated estimates after a distributed transaction " +
            		    "submits a new batch of queries for execution.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean markov_dtxn_updates;
        
        @ConfigProperty(
            description="Recompute a Markov model's execution state probabilities every time a transaction " +
                        "is aborted due to a misprediction.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean markov_mispredict_recompute;

        @ConfigProperty(
            description="", // TODO
            defaultNull=true,
            experimental=true
        )
        public String markov_path;
        
        @ConfigProperty(
            description="If this is set to true, TransactionEstimator will try to reuse the last " +
            		    "successfully estimate path in a MarkovGraph for transactions that use the" +
            		    "same graph.",
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
            description="If this is set to true, the MarkovEstimator will attempt to use the initial " +
            		    "path estimate to quickly calculate the new path for a running transaction.",
            defaultBoolean=true,
            experimental=true
        )
        public boolean markov_fast_path;
        
        @ConfigProperty(
            description="This enables the ability for the MarkovEstimator to cache the end points of " +
            		    "path segments in a MarkovGraph so that it can just quickly identify the " +
            		    "last MarkovVertex for a new batch of queries requested by the transaction.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean markov_endpoint_caching;
        
        @ConfigProperty(
            description="The minimum number of queries that must be in a batch for the TransactionEstimator " +
                        "to cache the path segment in the procedure's MarkovGraph. Provides a minor speed improvement " +
                        "for large batches with little variability in their execution paths.",
            defaultInt=3,
            experimental=true
        )
        public int markov_batch_caching_min;
        
        @ConfigProperty(
            description="Enable a hack for TPC-C where we inspect the arguments of the TPC-C neworder transaction" +
            		    "and figure out what partitions it needs without having to use the TransactionEstimator. " +
            		    "This will crash the system when used with other benchmarks. ",
            defaultBoolean=false,
            replacedBy="site.markov_fixed",
            experimental=true
        )
        @Deprecated
        public boolean exec_neworder_cheat;
        
        @ConfigProperty(
            description="Use a fixed transaction estimator to predict the initial properties of an incoming " +
                        "transaction request from the client. This is a quick and dirty approximation. " +
                        "Not all benchmarks are supported and it does not generate predictions updates after " +
                        "the transaction starts running",
            defaultBoolean=false,
            experimental=true
        )
        public boolean markov_fixed;
        
        @ConfigProperty(
            description="Enable profiling in the MarkovEstimator. " +
            		    "This data can be retrieved using the @Statistics sysproc.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean markov_profiling;

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
            description="If this enabled, HStoreCoordinator will use a separate thread to process incoming " +
            		    "initialization requests from other HStoreSites. This is useful when ${client.txn_hints} " +
            		    "is disabled.",
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
            description="If this enabled, HStoreCoordinator will use an NTP style protocol to find the time " +
            		    "difference between sites in the cluster.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean coordinator_sync_time;

        // ----------------------------------------------------------------------------
        // Output Tracing
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="When this property is set to true, all TransactionTrace records will include the stored " +
            		    "procedure output result",
            defaultBoolean=false,
            experimental=false
        )
        public boolean trace_txn_output;

        @ConfigProperty(
            description="When this property is set to true, all QueryTrace records will include the query output " +
            		    "result",
            defaultBoolean=false,
            experimental=false
        )
        public boolean trace_query_output;
        
        // ----------------------------------------------------------------------------
        // HSTORESITE STATUS UPDATES
        // ----------------------------------------------------------------------------
        
        @ConfigProperty(
            description="Enable HStoreSite's Status thread.",
            defaultBoolean=false,
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
            description="Allow the HStoreSiteStatus thread to kill the cluster if the local HStoreSite appears to be " +
            		    "hung. The site is considered hung if it has executed at least one transaction and has " +
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
            description="When this property is set to true, HStoreSite status will include information about each " +
            		    "PartitionExecutor, such as the number of transactions currently queued, blocked for " +
            		    "execution, or waiting to have their results returned to the client.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean status_show_executor_info;
        
        @ConfigProperty(
            description="When this property is set to true, HStoreSite status will include a snapshot of running " +
            		    "threads",
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
            description="Whether to enable object pooling for AbstractTransaction handles. This includes" +
            		    "all local transactions, remote transactions, and MapReduce transactions.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean pool_txn_enable;
        
        @ConfigProperty(
            description="The max number of LocalTransaction handles to keep in the pool per partition. " +
            		    "This should be roughly equivalent to ${site.network_incoming_max_per_partition}.",
            defaultInt=1000,
            experimental=false
        )
        public int pool_localtxnstate_idle;
        
        @ConfigProperty(
            description="The max number of MapReduceTransactionStates to keep in the pool per partition.",
            defaultInt=10,
            experimental=false
        )
        public int pool_mapreducetxnstate_idle;
        
        @ConfigProperty(
            description="The max number of RemoteTransactionStates to keep in the pool per partition. " +
                        "Depending on the workload, this should be roughly equivalent to " +
                        "${site.network_incoming_max_per_partition}.",
            defaultInt=1000,
            experimental=false
        )
        public int pool_remotetxnstate_idle;
        
        @ConfigProperty(
            description="The max number of MarkovPathEstimators to keep in the pool per partition",
            defaultInt=100,
            experimental=false
        )
        public int pool_pathestimators_idle;
        
        @ConfigProperty(
            description="The max number of TransactionEstimator.States to keep in the pool. " + 
                        "This should be the same as ${site.pool_localtxnstate_idle}.",
            defaultInt=1000,
            experimental=false
        )
        public int pool_estimatorstates_idle;
        
        @ConfigProperty(
            description="The max number of DistributedStates to keep in the pool per partition." +
                        "This should be the same as ${site.pool_localtxnstate_idle}.",
            defaultInt=1000,
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
            defaultInt=1000,
            experimental=false
        )
        public int pool_txnredirect_idle;
        
        @ConfigProperty(
            description="The max number of TransactionRedirectResponseCallbacks to keep idle in the pool.",
            defaultInt=2500,
            experimental=false
        )
        public int pool_txnredirectresponses_idle;
    }
    
    // ============================================================================
    // CLIENT
    // ============================================================================
    public final class ClientConf extends Conf {
        
        @ConfigProperty(
            description="Benchmark client log directory on the host that the BenchmarkController " +
                        "is invoked from.",
            defaultString="${global.log_dir}/clients",
            experimental=false
        )
        public String log_dir;
        
        @ConfigProperty(
            description="Whether to back-up log files before the benchmark is exceuted",
            defaultBoolean=false,
            experimental=false
        )
        public boolean log_backup;
        
        @ConfigProperty(
            description="Execute each HStoreSite with JVM asserts enabled. " +
                        "The client asserts will not affect the runtime performance of the " +
                        "database cluster, but it may increase the overhead of each client thread.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean jvm_asserts;
        
        @ConfigProperty(
            description="Additional JVM arguments to include when launching each benchmark client process. " +
            		    "These arguments will be automatically split and escaped based on spaces.",
            defaultNull=true,
            experimental=true
        )
        public String jvm_args;
        
        @ConfigProperty(
            description="The directory that benchmark project jars will be stored in.",
            defaultString=".",
            experimental=false
        )
        public String jar_dir;
        
        @ConfigProperty(
            description="The amount of memory to allocate for each client process (in MB)",
            defaultInt=512,
            experimental=false
        )
        public int memory;

        @ConfigProperty(
            description="Default client host name",
            defaultString="${global.defaulthost}",
            replacedBy="client.hosts",
            experimental=false
        )
        @Deprecated
        public String host;
        
        @ConfigProperty(
            description="A semi-colon separated list of hostnames that the BenchmarkController will " +
            		    "invoke benchmark clients on. Like the HStoreSite hosts, these machines must " +
            		    "have passwordless SSH enabled and have the H-Store distribution installed in" +
            		    "the same directory heirarchy as where the BenchmarkController was invoked from. " +
            		    "Each client host represents a unique JVM that will spawn the number of client " +
            		    "threads defined by the ${client.threads_per_host} parameter.", 
            defaultString="${global.defaulthost}",
            experimental=false
        )
        public String hosts;

        @ConfigProperty(
            description="The number of txns that client process submits (per ms). The underlying " +
                        "BenchmarkComponent will continue invoke the client driver's runOnce() method " +
                        "until it has submitted enough transactions to satisfy ${client.txnrate}. " +
                        "If ${client.blocking} is disabled, then the total transaction rate for a " +
                        "benchmark invocation is " +
                        "${client.txnrate} * ${client.processesperclient} * ${client.count}.",
            defaultInt=1000,
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
            description="Number of benchmark client threads to use per client host.",
            defaultInt=10,
            replacedBy="client.threads_per_host",
            experimental=false
        )
        @Deprecated
        public int processesperclient;
        
        @ConfigProperty(
            description="Number of benchmark client threads to invoke per client host. " +
            		    "If ${client.shared_connection} is set to true, then all of these threads " +
            		    "will share the same Client handle to the HStoreSite cluster.",
            defaultInt=10,
            experimental=false
        )
        public int threads_per_host;
        
        @ConfigProperty(
            description="Multiply the ${client.processesperclient} parameter by " +
                        "the number of partitions in the target cluster.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean processesperclient_per_partition;
        
        @ConfigProperty(
            description="", // TODO
            defaultBoolean=false,
            experimental=false
        )
        public boolean shared_connection;

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
            defaultDouble=1,
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
            description="Output a status update about the benchmark run at the end of each interval defined "+
                        "by ${client.interval}.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean output_interval;
        
        @ConfigProperty(
            description="Output a breakdown at the end of a benchmark run of the number of transactions " +
            		    "that each unique client thread executed successfully.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_clients;
        
        @ConfigProperty(
            description="Output a histogram at the end of a benchmark run of the number of transactions " +
            		    "that each partition executed.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_basepartitions;
        
        @ConfigProperty(
            description="Output a histogram at the end of a benchmark run of the different transaction " +
            		    "response status codes that the database returned to the clients.",
            defaultBoolean=false,
            replacedBy="client.output_status",
            experimental=false
        )
        @Deprecated
        public boolean output_response_status;
        
        @ConfigProperty(
            description="Output a histogram at the end of a benchmark run of the different transaction " +
                        "response status codes that the database returned to the clients.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean output_status;
        
        @ConfigProperty(
            description="Include the percentage of txns that were speculatively executed. " +
            		    "This will enable ${site.txn_client_debug}",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_specexec;
        
        @ConfigProperty(
            description="Print the benchmark results in a JSON parseable format. This is useful for " +
            		    "running experiments inside of scripts.",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_json;
        
        @ConfigProperty(
            description="",
            defaultBoolean=false,
            experimental=false
        )
        public boolean output_csv;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
            		    "the complete listing of all transactions executed by the clients.",
            defaultNull=true,
            experimental=false
        )
        public String output_full_csv;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
            		    "PartitionExecutor profiling stats. Note that this will automatically enable " +
            		    "${site.exec_profiling}, which will affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_exec_profiling;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
                        "TransactionQueueManager profiling stats. Note that this will automatically enable " +
                        "${site.queue_profiling}, which will affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_queue_profiling;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
                        "HStoreSite profiling stats. Note that this will automatically enable " +
                        "${site.profiling}, which will affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_site_profiling;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
                    "transaction profiling stats. Note that this will automatically enable " +
                    "${site.txn_profiling}, which will affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_txn_profiling;
        
        @ConfigProperty(
            description="If set to true, then the data generated for ${client.output_txn_profiling} will " +
            		    "be aggregated based on the Procedure handle.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean output_txn_profiling_combine;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
                        "the speculative execution stats. Note that this will automatically enable " +
                        "${site.specexec_profiling}, which will affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_specexec_profiling;
        
        @ConfigProperty(
            description="If set to true, then the data generated for ${client.output_specexec_profiling} will " +
                        "be aggregated based on the SpeculateType handle.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean output_specexec_profiling_combine;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
                        "the MarkovEstimator profiling stats. Note that this will automatically enable " +
                        "${site.markov_profiling}, which will affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_markov_profiling;
        
        @ConfigProperty(
            description="Defines the path where the BenchmarkController will dump a CSV containing " +
                        "transaction counter stats. This will contain information about how the " +
                        "transactions were executed (i.e., whether they were single-partitioned or not," +
                        "whether they were speculatively executed). " +
                        "Note that this will automatically enable ${site.txn_counters}, which will " +
                        "affect the runtime performance.",
            defaultNull=true,
            experimental=false
        )
        public String output_txn_counters;
        
        @ConfigProperty(
            description="If set to true, then the data generated for ${client.output_txn_counters} will " +
                        "be aggregated based on the Procedure handle.",
            defaultBoolean=true,
            experimental=false
        )
        public boolean output_txn_counters_combine;
        
        @ConfigProperty(
            description="", // TODO
            defaultBoolean=false,
            experimental=false
        )
        public boolean profiling;
        
        @ConfigProperty(
            description="If set to true, then the BenchmarkController will periodically send requests to " +
                        "the H-Store cluster to evict tuples into the anti-cache database. Note that " + 
                        "${site.anticache_enable} must be set to true when the cluster is started.",
            defaultBoolean=false,
            experimental=true
        )
        public boolean anticache_enable;
        
        @ConfigProperty(
            description="This parameter defines how often in milliseconds the BenchmarkController will " +
                        "send request to evict tuples from all of the tables marked as evictable. " +
                        "Both ${site.anticache_enable} and ${client.anticache_enable} must be set to true.",
            defaultInt=30000,
            experimental=true
        )
        public int anticache_evict_interval;
        
        @ConfigProperty(
            description="Defines the block size in bytes that will be evicted for each eviction request" +
                        "Both ${site.anticache_enable} and ${client.anticache_enable} must be set to true.",
            defaultInt=2097152,
            experimental=true
        )
        public int anticache_evict_size;
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
                if (trace.get()) LOG.trace(String.format("%-20s = %s", f.getName(), value));
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
    
    private final Map<Conf, Set<String>> externalParams = new HashMap<Conf, Set<String>>();
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    private HStoreConf() {
        this.populateDependencies();
    }
    
    /**
     * Constructor
     */
    private HStoreConf(ArgumentsParser args) {
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
        if (site.markov_fixed) {
            site.exec_force_singlepartitioned = false;
            site.exec_force_localexecution = false;
        }
        
        this.populateDependencies();
    }
    
    protected void set(Conf handle, String f_name, Object value) {
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
        this.set(handle, f, value);
    }
    
    /**
     * Set value for the given Conf handle's field 
     * This method should always be used because it knows how to map values from 
     * deprecated parameters to their new replacements. 
     * @param handle
     * @param f
     * @param value
     */
    protected void set(Conf handle, Field f, Object value) {
        try {
            f.set(handle, value);
             if (debug.get())
                LOG.debug(String.format("SET %s.%s = %s",
                                        handle.prefix, f.getName(), value));
        } catch (Exception ex) {
            String msg = String.format("Failed to set value '%s' for '%s.%s'",
                                       value, handle.prefix, f.getName()); 
            throw new RuntimeException(msg, ex);
        }
        
        // If this option has been deprecated and replaced, then we 
        // need to also set the new configuration parameter
        // Make sure that we don't do this for externally set parameters
        ConfigProperty cp = handle.getConfigProperties().get(f);
        assert(cp != null) : "Missing ConfigProperty for " + f;
        if (cp.replacedBy() != null && cp.replacedBy().isEmpty() == false) {
            if (debug.get())
                LOG.debug(String.format("Automatically updating replaceBy parameter: %s.%s => %s",
                                        handle.prefix, f.getName(), cp.replacedBy()));
            this.set(cp.replacedBy(), value, true);
        }
    }
    
    /**
     * This will set the values of any parameter that references another
     * This can only be invoked after all of the Conf handles are initialized
     */
    protected void populateDependencies() {
        if (debug.get()) LOG.debug("Populating dependent parameters");
        
        Pattern p = Pattern.compile("\\$\\{" + REGEX_STR + "\\}", Pattern.CASE_INSENSITIVE);
        for (Conf handle : confHandles.values()) {
            for (Entry<Field, ConfigProperty> e : handle.getConfigProperties().entrySet()) {
                // Skip anything that we set externally
                Field f = e.getKey();
                if (this.isMarkedExternal(handle, f.getName())) continue;
                
                // FIXME: This only works with strings
                ConfigProperty cp = e.getValue();
                String defaultString = cp.defaultString();
                if (defaultString == null) continue;
                
                defaultString = defaultString.trim();
                if (defaultString.isEmpty()) continue;
                
                Matcher m = p.matcher(defaultString);
                boolean found = m.find();
                if (m == null || found == false) continue;
                
                String dependencyKey = m.group(1) + "." + m.group(2);
                if (trace.get())
                    LOG.trace(String.format("Found dependency: %s -> %s", f.getName(), dependencyKey));
                
                Object dependencyValue = this.get(dependencyKey);
                String newValue = defaultString.substring(0, m.start()) +
                                  dependencyValue +
                                  defaultString.substring(m.end());
                this.set(handle, f, newValue);
                if (debug.get())
                    LOG.debug(String.format("Updated dependent parameter %s.%s [%s] ==> %s",
                              handle.prefix, f.getName(), defaultString, newValue));
            } // FOR
        } // FOR
    }
    
    /**
     * Keep track of what parameters we set manually (either from a file or from 
     * input arguments). This is needed so that we know what parameters to forward to
     * remote clients and sites in the BenchmarkController
     * @param handle
     * @param f_name
     */
    protected void markAsExternal(Conf handle, String f_name) {
        Set<String> s = this.externalParams.get(handle);
        if (s == null) {
            s = new HashSet<String>();
            this.externalParams.put(handle, s);
        }
        s.add(f_name);
    }
    
    /**
     * Returns true if this parameter was set manually from an external source
     * @param handle
     * @param f_name
     * @return
     */
    protected boolean isMarkedExternal(Conf handle, String f_name) {
        Set<String> s = this.externalParams.get(handle);
        boolean ret = (s != null && s.contains(f_name));
        if (debug.get())
            LOG.debug(String.format("Checking whether %s.%s is externally set: %s",
                               handle.prefix, f_name, ret));
        return (ret);
    }
    
    // ----------------------------------------------------------------------------
    // REFLECTIVE ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the value for the given option key name.
     * Must be in the proper format (i.e., "<handle>.<param-name>")
     * @param k
     * @return
     */
    public Object get(String k) {
        Matcher m = REGEX_PARSE.matcher(k);
        boolean found = m.matches();
        if (m == null || found == false) {
            String msg = "Invalid configuration property '" + k + "'";
            throw new RuntimeException(msg);
        }
        
        Conf handle = confHandles.get(m.group(1));
        Class<?> confClass = handle.getClass();
        assert(confClass != null);
        
        String f_name = m.group(2);
        Field f = null;
        Object value = null;
        try {
            f = confClass.getField(f_name);
            value = f.get(handle);
        } catch (Exception ex) {
            String msg = "Invalid configuration property '" + k + "'";
            throw new RuntimeException(msg, ex);
        }
        return (value);
    }
    
    public boolean set(String k, Object value) {
        return this.set(k, value, false);
    }
        
    protected boolean set(String k, Object value, boolean skip_external) {
        Matcher m = REGEX_PARSE.matcher(k);
        boolean found = m.matches();
        if (m == null || found == false) {
            String msg = "Invalid configuration property '" + k + "'";
            throw new RuntimeException(msg);
        }
        assert(m != null);
        Conf handle = confHandles.get(m.group(1));
        
        if (skip_external && this.isMarkedExternal(handle, m.group(2))) {
            return (false);
        }
        this.set(handle, m.group(2), value);
        return (true);
    }
    
    // ----------------------------------------------------------------------------
    // LOADING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
    public void loadFromFile(File path) {
        if (debug.get()) LOG.debug("Loading from input file [" + path + "]");
        
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
                LOG.warn("Invalid configuration property '" + k + "'. Ignoring...");
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
            
            this.set(handle, f, value);
            this.markAsExternal(handle, f_name);
        } // FOR
    }
    
    public void loadFromArgs(String args[]) {
        if (debug.get()) LOG.debug("Loading from commandline input arguments");
        final Pattern split_p = Pattern.compile("=");
        
        final Map<String, String> argsMap = new ListOrderedMap<String, String>();
        for (int i = 0, cnt = args.length; i < cnt; i++) {
            final String arg = args[i];
            final String[] parts = split_p.split(arg, 2);
            if (parts.length == 1) {
                LOG.warn("Unexpected argument format '" + arg + "'");
                continue;
            }
            
            String k = parts[0].toLowerCase();
            String v = parts[1];
            if (k.startsWith("-")) k = k.substring(1);
            
            // 'hstore.tag' is special argument that we use in killstragglers.py 
            if (k.equalsIgnoreCase("tag")) {
                continue;
            // This command is undefined from the commandline
            } else if (v.startsWith("${")) {
                continue;
            // Or this parameter is commented out in Eclipse
            } else if (k.startsWith("#")) {
                continue;
            }
            
            // We want it!
            argsMap.put(k, v);
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
            Conf handle = confHandles.get(confName);
            Class<?> confClass = handle.getClass();
            assert(confClass != null);
            Field f = null;
            String f_name = m.group(2).toLowerCase();
            try {
                f = confClass.getField(f_name);
            } catch (Exception ex) {
                if (debug.get()) LOG.warn("Invalid configuration property '" + k + "'. Ignoring...");
                continue;
            }
            ConfigProperty cp = handle.getConfigProperties().get(f);
            assert(cp != null) : "Missing ConfigProperty for " + f;
            Class<?> f_class = f.getType();
            Object value = null;
            if (debug.get()) LOG.debug(String.format("Casting value '%s' for key '%s' to proper type [class=%s]",
                                       v, k, f_class));

            try {
                if (f_class.equals(int.class) || f_class.equals(Integer.class)) {
                    value = Integer.parseInt(v);
                } else if (f_class.equals(long.class) || f_class.equals(Long.class)) {
                    value = Long.parseLong(v);
                } else if (f_class.equals(double.class) || f_class.equals(Double.class)) {
                    value = Double.parseDouble(v);
                } else if (f_class.equals(boolean.class) || f_class.equals(Boolean.class)) {
                    value = Boolean.parseBoolean(v.toLowerCase());
                } else if (f_class.equals(String.class)) {
                    value = v;
                } else {
                    LOG.warn(String.format("Unexpected value type '%s' for property '%s'", f_class.getSimpleName(), f_name));
                    continue;
                }
            } catch (Exception ex) {
                LOG.error(String.format("Invalid value '%s' for configuration parameter '%s'", v, k), ex);
                continue;
            }
            if (debug.get()) LOG.debug(String.format("CAST %s => %s", k, value));
           
            this.set(handle, f, value);
            this.markAsExternal(handle, f_name);
        } // FOR
    }
    
    public Map<String, String> getParametersLoadedFromArgs() {
        Map<String, String> m = new HashMap<String, String>();
        for (Conf confHandle : this.externalParams.keySet()) {
            for (String f_name : this.externalParams.get(confHandle)) {
                Object val = confHandle.getValue(f_name);
                if (val != null) {
                    String key = String.format("%s.%s", confHandle.prefix, f_name);
                    if (trace.get()) LOG.trace(String.format("LOADED %s => %s", key, val.toString()));
                    m.put(key, val.toString());
                }
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
        Set<String> params = this.externalParams.get(confHandle);
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
        
        boolean changed = false;
        if (f != null && f.exists()) {
            conf.loadFromFile(f);
            changed = true;
        }
        if (args != null) {
            conf.loadFromArgs(args);
            changed = true;
        }
        if (changed) conf.populateDependencies();
        
        return (conf);
    }
    
    public synchronized static HStoreConf init(File f) {
        return HStoreConf.init(f, null);
    }
    
    public synchronized static HStoreConf initArgumentsParser(ArgumentsParser args) {
        if (conf != null) throw new RuntimeException("Trying to initialize HStoreConf more than once");
        conf = new HStoreConf(args);
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
    
    /**
     * Returns true if the given string is a valid HStoreConf parameter
     * @param name
     * @return
     */
    public boolean hasParameter(String name) {
        Matcher m = REGEX_PARSE.matcher(name);
        if (m.find()) {
            Conf c = this.confHandles.get(m.group(1));
            assert(c != null) : "Unexpected null Conf for '" + m.group(1) + "'";
            return (c.hasParameter(m.group(2)));
        }
        return (false);
    }
    
    private static HStoreConf confHelper;
    public static boolean isConfParameter(String name) {
        if (confHelper == null) {
            synchronized (HStoreConf.class) {
                if (confHelper == null) {
                    confHelper = new HStoreConf();
                }
            } // SYNCH
        }
        return confHelper.hasParameter(name);
    }

}
