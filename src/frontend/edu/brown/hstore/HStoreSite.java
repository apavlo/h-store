/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;
import org.apache.log4j.Logger;
import org.voltdb.AriesLog;
import org.voltdb.AriesLogNative;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.MemoryStats;
import org.voltdb.AntiCacheMemoryStats;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureProfiler;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSource;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SysProcSelector;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.AdHocPlannedStmt;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWorkThread;
import org.voltdb.exceptions.ClientConnectionLostException;
import org.voltdb.exceptions.EvictedTupleAccessException;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.VoltNetwork;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.EstTimeUpdater;
import org.voltdb.utils.Pair;
import org.voltdb.utils.SystemStatsCollector;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.ClientInterface.ClientInputHandler;
import edu.brown.hstore.HStoreThreadManager.ThreadGroupType;
import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.ClientResponseCallback;
import edu.brown.hstore.callbacks.LocalFinishCallback;
import edu.brown.hstore.callbacks.LocalInitQueueCallback;
import edu.brown.hstore.callbacks.PartitionCountingCallback;
import edu.brown.hstore.callbacks.RedirectCallback;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.remote.RemoteEstimator;
import edu.brown.hstore.estimators.remote.RemoteEstimatorState;
import edu.brown.hstore.internal.SetDistributedTxnMessage;
import edu.brown.hstore.stats.AntiCacheManagerProfilerStats;
import edu.brown.hstore.stats.BatchPlannerProfilerStats;
import edu.brown.hstore.stats.MarkovEstimatorProfilerStats;
import edu.brown.hstore.stats.PartitionExecutorProfilerStats;
import edu.brown.hstore.stats.SiteProfilerStats;
import edu.brown.hstore.stats.SpecExecProfilerStats;
import edu.brown.hstore.stats.TransactionCounterStats;
import edu.brown.hstore.stats.TransactionProfilerStats;
import edu.brown.hstore.stats.TransactionQueueManagerProfilerStats;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.DependencyTracker;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.hstore.util.MapReduceHelperThread;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.hstore.util.TransactionProfilerDumper;
import edu.brown.interfaces.Configurable;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.logging.RingBufferAppender;
import edu.brown.markov.EstimationThresholds;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.profilers.HStoreSiteProfiler;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.workload.Workload;

/**
 * THE ALL POWERFUL H-STORE SITE!
 * This is the central hub for a site and all of its partitions
 * All incoming transactions come into this and all transactions leave through this
 * @author pavlo
 */
public class HStoreSite implements VoltProcedureListener.Handler, Shutdownable, Configurable, Runnable {
    public static final Logger LOG = Logger.getLogger(HStoreSite.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // INSTANCE MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The H-Store Configuration Object
     */
    private final HStoreConf hstore_conf;

    /** Catalog Stuff **/
    private long instanceId;
    private final CatalogContext catalogContext;
    private final Host catalog_host;
    private final Site catalog_site;
    private final int site_id;
    private final String site_name;
    
    /**
     * This buffer pool is used to serialize ClientResponses to send back
     * to clients.
     */
    private final DBBPool buffer_pool = new DBBPool(false, false);
    
    /**
     * Incoming request deserializer
     */
    private final ThreadLocal<FastDeserializer> incomingDeserializers = new ThreadLocal<FastDeserializer>() {
        @Override
        protected FastDeserializer initialValue() {
            return (new FastDeserializer(new byte[0]));
        }
    };
    
    /**
     * Outgoing response serializers
     */
    private final ThreadLocal<FastSerializer> outgoingSerializers = new ThreadLocal<FastSerializer>() {
        @Override
        protected FastSerializer initialValue() {
            return (new FastSerializer(HStoreSite.this.buffer_pool));
        }
    };
    
    /**
     * This is the object that we use to generate unqiue txn ids used by our
     * H-Store specific code. There can either be a single manager for the entire site,
     * or we can use one per partition. 
     * @see HStoreConf.site.txn_partition_id_managers
     */
    private final TransactionIdManager txnIdManagers[];

    /**
     * The TransactionInitializer is used to figure out what txns will do
     *  before we start executing them
     */
    private final TransactionInitializer txnInitializer;
    
    /**
     * This class determines what partitions transactions/queries will
     * need to execute on based on their input parameters.
     */
    private final PartitionEstimator p_estimator;
    private final AbstractHasher hasher;
    
    /**
     * Keep track of which txns that we have in-flight right now
     */
    private final Map<Long, AbstractTransaction> inflight_txns = 
                        new ConcurrentHashMap<Long, AbstractTransaction>();
    
    /**
     * Queues for transactions that are ready to be cleaned up and deleted
     * There is one queue for each Status type
     */
    private final Map<Status, Queue<Long>> deletable_txns = new HashMap<Status, Queue<Long>>();
    
    /**
     * The list of the last txn ids that were successfully deleted
     * This is primarily used for debugging
     */
    private final CircularFifoBuffer<String> deletable_last = new CircularFifoBuffer<String>(10);
    
    /**
     * This TransactionEstimator is a stand-in for transactions that need to access
     * this partition but who are running at some other node in the cluster.
     */
    private final RemoteEstimator remoteTxnEstimator;
    
    // ----------------------------------------------------------------------------
    // STATS STUFF
    // ----------------------------------------------------------------------------
    
    private final StatsAgent statsAgent = new StatsAgent();
    private TransactionProfilerStats txnProfilerStats;
    private MemoryStats memoryStats;
    private AntiCacheMemoryStats anticacheMemoryStats;
    
    // ----------------------------------------------------------------------------
    // NETWORKING STUFF
    // ----------------------------------------------------------------------------
    
    /**
     * This thread is responsible for listening for incoming txn requests from 
     * clients. It will then forward the request to HStoreSite.procedureInvocation()
     */
//    private VoltProcedureListener voltListeners[];
//    private final NIOEventLoop procEventLoops[];
    
    private final VoltNetwork voltNetwork;
    private ClientInterface clientInterface;
    
    // ----------------------------------------------------------------------------
    // TRANSACTION COORDINATOR/PROCESSING THREADS
    // ----------------------------------------------------------------------------
    
    /**
     * This manager is used to pin threads to specific CPU cores
     */
    private final HStoreThreadManager threadManager;
    
    /**
     * PartitionExecutors
     * These are the single-threaded execution engines that have exclusive
     * access to a partition. Any transaction that needs to access data at a partition
     * will have to first get queued up by one of these executors.
     */
    private final PartitionExecutor executors[];
    private final Thread executor_threads[];
    
    /**
     * DependencyTrackers
     * One per partition.
     */
    private final DependencyTracker depTrackers[];
    
    /**
     * The queue manager is responsible for deciding what distributed transaction
     * is allowed to acquire the locks for each partition. It can also requeue
     * restart transactions. 
     */
    private final TransactionQueueManager txnQueueManager;
    
    /**
     * The HStoreCoordinator is responsible for communicating with other HStoreSites
     * in the cluster to execute distributed transactions.
     * NOTE: We will bind this variable after construction so that we can inject some
     * testing code as needed.
     */
    private HStoreCoordinator hstore_coordinator;

    /**
     * TransactionPreProcessor Threads
     */
    private List<TransactionPreProcessor> preProcessors = null;
    private BlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> preProcessorQueue = null;
    
    /**
     * TransactionPostProcessor Thread
     * These threads allow a PartitionExecutor to send back ClientResponses back to
     * the clients without blocking
     */
    private List<TransactionPostProcessor> postProcessors = null;
    private BlockingQueue<Object[]> postProcessorQueue = null;
    
    /**
     * Transaction Handle Cleaner
     */
    private final List<TransactionCleaner> txnCleaners = new ArrayList<TransactionCleaner>();
    
    /**
     * MapReduceHelperThread
     */
    private boolean mr_helper_started = false;
    private final MapReduceHelperThread mr_helper;
    
    /**
     * Transaction Command Logger (WAL)
     */
    private final CommandLogWriter commandLogger;

    /**
     * AdHoc: This thread waits for AdHoc queries. 
     */
    private boolean adhoc_helper_started = false;
    private final AsyncCompilerWorkThread asyncCompilerWorkThread;
    
    /**
     * Anti-Cache Abstraction Layer
     */
    private final AntiCacheManager anticacheManager;
    
    /**
     * This catches any exceptions that are thrown in the various
     * threads spawned by this HStoreSite
     */
    private final EventObservableExceptionHandler exceptionHandler = new EventObservableExceptionHandler();
    
    // ----------------------------------------------------------------------------
    // INTERNAL STATE OBSERVABLES
    // ----------------------------------------------------------------------------
    
    /**
     * EventObservable for when the HStoreSite is finished initializing
     * and is now ready to execute transactions.
     */
    private boolean ready = false;
    private final EventObservable<HStoreSite> ready_observable = new EventObservable<HStoreSite>();
    
    /**
     * EventObservable for when we receive the first non-sysproc stored procedure
     * Other components of the system can attach to the EventObservable to be told when this occurs 
     */
    private boolean startWorkload = false;
    private final EventObservable<HStoreSite> startWorkload_observable = 
                        new EventObservable<HStoreSite>();
    
    /**
     * EventObservable for when the HStoreSite has been told that it needs to shutdown.
     */
    private Shutdownable.ShutdownState shutdown_state = ShutdownState.INITIALIZED;
    private final EventObservable<Object> prepare_observable = new EventObservable<Object>();
    private final EventObservable<Object> shutdown_observable = new EventObservable<Object>();
    
    // ----------------------------------------------------------------------------
    // PARTITION SPECIFIC MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Collection of local partitions managed at this HStoreSite
     */
    private final PartitionSet local_partitions = new PartitionSet();
    
    /**
     * PartitionId -> Internal Offset
     * This is so that we don't have to keep long arrays of local partition information
     */
    private final int local_partition_offsets[];
    
    // ----------------------------------------------------------------------------
    // TRANSACTION ESTIMATION
    // ----------------------------------------------------------------------------

    /**
     * Estimation Thresholds
     */
    private EstimationThresholds thresholds = new EstimationThresholds(); // default values
    
    // ----------------------------------------------------------------------------
    // STATUS + PROFILING MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * Status Monitor
     */
    private final HStoreSiteStatus status_monitor;
    
    /**
     * Profiler
     */
    private HStoreSiteProfiler profiler = new HStoreSiteProfiler();

    /**
     * Transaction Profiler Dumper!
     */
    private TransactionProfilerDumper txn_profiler_dumper;
    
    // ----------------------------------------------------------------------------
    // CACHED STRINGS
    // ----------------------------------------------------------------------------
    
    private final String REJECTION_MESSAGE;    
    
    // ----------------------------------------------------------------------------    
    // ARIES
    // ----------------------------------------------------------------------------

    private AriesLog m_ariesLog = null;
        
    private String m_ariesLogFileName = null;    
    //XXX Must match with AriesLogProxy
    private final String m_ariesDefaultLogFileName = "aries.log";
    
    @SuppressWarnings("unused")
    private VoltLogger m_recoveryLog = null;    
    
    public AriesLog getAriesLogger() {
        return m_ariesLog;
    }

    public String getAriesLogFileName() {
        return m_ariesLogFileName;
    }

    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param coordinators
     * @param p_estimator
     */
    protected HStoreSite(int site_id, CatalogContext catalogContext, HStoreConf hstore_conf) {
        assert(hstore_conf != null);
        assert(catalogContext != null);
        this.hstore_conf = hstore_conf;
        this.catalogContext = catalogContext;
        
        this.catalog_site = this.catalogContext.getSiteById(site_id);
        if (this.catalog_site == null) throw new RuntimeException("Invalid site #" + site_id);
        
        this.catalog_host = this.catalog_site.getHost(); 
        this.site_id = this.catalog_site.getId();
        this.site_name = HStoreThreadManager.getThreadName(this.site_id, null);
        
        final int num_partitions = this.catalogContext.numberOfPartitions;
        this.local_partitions.addAll(CatalogUtil.getLocalPartitionIds(catalog_site));
        int num_local_partitions = this.local_partitions.size();
        
         for (Status s : Status.values()) {
             this.deletable_txns.put(s, new ConcurrentLinkedQueue<Long>());
         } // FOR
        
        this.executors = new PartitionExecutor[num_partitions];
        this.executor_threads = new Thread[num_partitions];
        this.depTrackers = new DependencyTracker[num_partitions];
        
        // Get the hasher we will use for this HStoreSite
        this.hasher = ClassUtil.newInstance(hstore_conf.global.hasher_class,
                                             new Object[]{ this.catalogContext, num_partitions },
                                             new Class<?>[]{ CatalogContext.class, int.class });
        this.p_estimator = new PartitionEstimator(this.catalogContext, this.hasher);
        this.remoteTxnEstimator = new RemoteEstimator(this.p_estimator);
        
        // ARIES 
        if(hstore_conf.site.aries){
            // Don't use both recovery modes
            assert(hstore_conf.site.snapshot == false);

            LOG.warn("Starting ARIES recovery at site");           

            String siteName = HStoreThreadManager.formatSiteName(this.getSiteId());
            String ariesSiteDirPath = hstore_conf.site.aries_dir + File.separatorChar + siteName + File.separatorChar;
           
            this.m_ariesLogFileName =  ariesSiteDirPath + m_ariesDefaultLogFileName ; 
            int numPartitionsPerSite =   this.catalog_site.getPartitions().size();
            int numSites = this.catalogContext.numberOfSites;

            LOG.warn("ARIES : Log Native creation :: numSites : "+numSites+" numPartitionsPerSite : "+numPartitionsPerSite);           
            this.m_ariesLog = new AriesLogNative(numSites, numPartitionsPerSite, this.m_ariesLogFileName);
            this.m_recoveryLog = new VoltLogger("RECOVERY");
        }
                        
        // **IMPORTANT**
        // Always clear out the CatalogUtil and BatchPlanner before we start our new HStoreSite
        // TODO: Move this cache information into CatalogContext
        CatalogUtil.clearCache(this.catalogContext.database);
        BatchPlanner.clear(this.catalogContext.numberOfPartitions);
        TransactionCounter.resetAll(this.catalogContext);

        // Only preload stuff if we were asked to
        if (hstore_conf.site.preload) {
            if (debug.val) LOG.debug("Preloading cached objects");
            try {
                // Don't forget our CatalogUtil friend!
                CatalogUtil.preload(this.catalogContext.database);
                
                // Load up everything the QueryPlanUtil
                PlanNodeUtil.preload(this.catalogContext.database);
                
                // Then load up everything in the PartitionEstimator
                this.p_estimator.preload();
            } catch (Exception ex) {
                throw new RuntimeException("Failed to prepare HStoreSite", ex);
            }
        }
        
        // Offset Hack
        this.local_partition_offsets = new int[num_partitions];
        Arrays.fill(this.local_partition_offsets, HStoreConstants.NULL_PARTITION_ID);
        int offset = 0;
        for (int partition : this.local_partitions) {
            this.local_partition_offsets[partition] = offset++;
        } // FOR
        
        // -------------------------------
        // THREADS
        // -------------------------------
        
        EventObserver<Pair<Thread, Throwable>> observer = new EventObserver<Pair<Thread, Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> arg) {
                Thread thread = arg.getFirst();
                Throwable error = arg.getSecond();
                String threadName = "<unknown>";
                if (thread != null) threadName = thread.getName(); 
                LOG.fatal(String.format("Thread %s had a fatal error: %s",
                          threadName, (error != null ? error.getMessage() : null)));
                error.printStackTrace();
                hstore_coordinator.shutdownClusterBlocking(error);
            }
        };
        this.exceptionHandler.addObserver(observer);
        Thread.setDefaultUncaughtExceptionHandler(this.exceptionHandler);
        
        // HStoreSite Thread Manager (this always get invoked first)
        this.threadManager = new HStoreThreadManager(this);
        
        // Distributed Transaction Queue Manager
        this.txnQueueManager = new TransactionQueueManager(this);
        
        // One Transaction Cleaner for every eight partitions
        int numCleaners = (int)Math.ceil(num_local_partitions / 8.0);
        for (int i = 0; i < numCleaners; i++) {
            this.txnCleaners.add(new TransactionCleaner(this));
        } // FOR
        
        // MapReduce Transaction helper thread
        if (catalogContext.getMapReduceProcedures().isEmpty() == false) { 
            this.mr_helper = new MapReduceHelperThread(this);
        } else {
            this.mr_helper = null;
        }
        
        // Separate TransactionIdManager per partition
        if (hstore_conf.site.txn_partition_id_managers) {
            this.txnIdManagers = new TransactionIdManager[num_partitions];
            for (int partition : this.local_partitions) {
                this.txnIdManagers[partition] = new TransactionIdManager(partition);
            } // FOR
        }
        // Single TransactionIdManager for the entire site
        else {
            this.txnIdManagers = new TransactionIdManager[] {
                new TransactionIdManager(this.site_id)
            };
        }
        
        // Command Logger
        if (hstore_conf.site.commandlog_enable) {
            // It would be nice if we could come up with a unique name for this
            // invocation of the system (like the cluster instanceId). But for now
            // we'll just write out to our directory...
            
            java.util.Date date = new java.util.Date();
            Timestamp current = new Timestamp(date.getTime());
            String nonce = Long.toString(current.getTime());            
            
            File logFile = new File(hstore_conf.site.commandlog_dir +
                                    File.separator +
                                    this.getSiteName().toLowerCase() +
                                    "_" + nonce +
                                    CommandLogWriter.LOG_OUTPUT_EXT);                      
                     
            this.commandLogger = new CommandLogWriter(this, logFile);
        } else {
            this.commandLogger = null;
        }

        // AdHoc Support
        if (hstore_conf.site.exec_adhoc_sql) {
            this.asyncCompilerWorkThread = new AsyncCompilerWorkThread(this, this.site_id);
        } else {
            this.asyncCompilerWorkThread = null;
        }
        
        // The AntiCacheManager will allow us to do special things down in the EE
        // for evicted tuples
        if (hstore_conf.site.anticache_enable) {
            this.anticacheManager = new AntiCacheManager(this);
        } else {
            this.anticacheManager = null;
        }
        
        // -------------------------------
        // NETWORK SETUP
        // -------------------------------
        
        this.voltNetwork = new VoltNetwork(this);
        this.clientInterface = new ClientInterface(this, this.catalog_site.getProc_port());
        
        // -------------------------------
        // TRANSACTION ESTIMATION
        // -------------------------------
        
        // Transaction Properties Initializer
        this.txnInitializer = new TransactionInitializer(this);
        
        // CACHED MESSAGES
        this.REJECTION_MESSAGE = "Transaction was rejected by " + this.getSiteName();
        
        // -------------------------------
        // STATS SETUP
        // -------------------------------
        
        this.initTxnProcessors();
        this.initStatSources();
        
        // Profiling
        if (hstore_conf.site.profiling) {
            this.profiler = new HStoreSiteProfiler();
            if (hstore_conf.site.status_exec_info) {
                this.profiler.network_idle.resetOnEventObservable(this.startWorkload_observable);
            }
        } else {
            this.profiler = null;
        }
        
        this.status_monitor = new HStoreSiteStatus(this, hstore_conf);
        
        LoggerUtil.refreshLogging(hstore_conf.global.log_refresh);
    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION STUFF
    // ----------------------------------------------------------------------------

    /**
     * Initializes all the pieces that we need to start this HStore site up
     * This should only be called by our run() method
     */
    protected HStoreSite init() {
        if (debug.val)
            LOG.debug("Initializing HStoreSite " + this.getSiteName());
        this.hstore_coordinator = this.initHStoreCoordinator();
        
        // First we need to tell the HStoreCoordinator to start-up and initialize its connections
        if (debug.val)
            LOG.debug("Starting HStoreCoordinator for " + this.getSiteName());
        this.hstore_coordinator.start();

        ThreadGroup auxGroup = this.threadManager.getThreadGroup(ThreadGroupType.AUXILIARY);
        
        // Start TransactionQueueManager
        Thread t = new Thread(auxGroup, this.txnQueueManager);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        t.start();
        
        // Start VoltNetwork
        t = new Thread(this.voltNetwork);
        t.setName(HStoreThreadManager.getThreadName(this, HStoreConstants.THREAD_NAME_VOLTNETWORK));
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        t.start();
        
        // Start CommandLogWriter
        t = new Thread(auxGroup, this.commandLogger);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        t.start();
        
        // Start AntiCacheManager Queue Processor
        if (this.anticacheManager != null && this.anticacheManager.getEvictableTables().isEmpty() == false) {
            t = new Thread(auxGroup, this.anticacheManager);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(this.exceptionHandler);
            t.start();
        }
        
        // TransactionPreProcessors
        if (this.preProcessors != null) {
            for (TransactionPreProcessor tpp : this.preProcessors) {
                t = new Thread(this.threadManager.getThreadGroup(ThreadGroupType.PROCESSING), tpp); 
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(this.exceptionHandler);
                t.start();    
            } // FOR
        }
        // TransactionPostProcessors
        if (this.postProcessors != null) {
            for (TransactionPostProcessor tpp : this.postProcessors) {
                t = new Thread(this.threadManager.getThreadGroup(ThreadGroupType.PROCESSING), tpp);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(this.exceptionHandler);
                t.start();    
            } // FOR
        }
        
        // Then we need to start all of the PartitionExecutor in threads
        if (debug.val)
            LOG.debug(String.format("Starting PartitionExecutor threads for %s partitions on %s",
                      this.local_partitions.size(), this.getSiteName()));
        for (int partition : this.local_partitions.values()) {
            PartitionExecutor executor = this.getPartitionExecutor(partition);
            // executor.initHStoreSite(this);
            
            t = new Thread(this.threadManager.getThreadGroup(ThreadGroupType.EXECUTION), executor);
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY); // Probably does nothing...
            t.setUncaughtExceptionHandler(this.exceptionHandler);
            this.executor_threads[partition] = t;
            t.start();
        } // FOR
        
        // Start Transaction Cleaners
        int i = 0;
        for (TransactionCleaner cleaner : this.txnCleaners) {
            String name = String.format("%s-%02d", HStoreThreadManager.getThreadName(this, HStoreConstants.THREAD_NAME_TXNCLEANER), i);
            t = new Thread(this.threadManager.getThreadGroup(ThreadGroupType.CLEANER), cleaner);
            t.setName(name);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(this.exceptionHandler);
            t.start();
            i += 1;
        } // FOR
        
        this.initPeriodicWorks();
        
        // Transaction Profile CSV Dumper
        if (hstore_conf.site.txn_profiling && hstore_conf.site.txn_profiling_dump) {
            File csvFile = new File(hstore_conf.global.log_dir +
                                    File.separator +
                                    this.getSiteName().toLowerCase() +
                                    "-profiler.csv");
            this.txn_profiler_dumper = new TransactionProfilerDumper(csvFile);
            LOG.info(String.format("Transaction profile data will be written to '%s'", csvFile));
        }
        
        // Add in our shutdown hook
        // Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
        
        return (this);
    }
    
    private void initTxnProcessors() {
        if (hstore_conf.site.exec_preprocessing_threads == false &&
            hstore_conf.site.exec_postprocessing_threads == false) {
            return;
        }
        
        // Transaction Pre/Post Processing Threads
        // We need at least one core per partition and one core for the VoltProcedureListener
        // Everything else we can give to the pre/post processing guys
        final int num_local_partitions = this.local_partitions.size();
        int num_available_cores = this.threadManager.getNumCores() - (num_local_partitions + 1);

        // If there are no available cores left, then we won't create any extra processors
        if (num_available_cores <= 0) {
            LOG.warn("Insufficient number of cores on " + catalog_host.getIpaddr() + ". " +
                     "Disabling transaction pre/post processing threads");
            hstore_conf.site.exec_preprocessing_threads = false;
            hstore_conf.site.exec_postprocessing_threads = false;
            return;
        }

        int num_preProcessors = 0;
        int num_postProcessors = 0;
        
        // Both Types of Processors
        if (hstore_conf.site.exec_preprocessing_threads && hstore_conf.site.exec_postprocessing_threads) {
            int split = (int)Math.ceil(num_available_cores / 2d);
            num_preProcessors = split;
        }
        // TransactionPreProcessor Only
        else if (hstore_conf.site.exec_preprocessing_threads) {
            num_preProcessors = num_available_cores;
        }
        
        // We only need one TransactionPostProcessor per HStoreSite
        if (hstore_conf.site.exec_postprocessing_threads) {
            num_postProcessors = 1;
        }
        
        // Overrides
        if (hstore_conf.site.exec_preprocessing_threads_count >= 0) {
            num_preProcessors = hstore_conf.site.exec_preprocessing_threads_count;
        }
        
        // Initialize TransactionPreProcessors
        if (num_preProcessors > 0) {
            if (debug.val)
                LOG.debug(String.format("Starting %d %s threads",
                          num_preProcessors, TransactionPreProcessor.class.getSimpleName()));
            this.preProcessors = new ArrayList<TransactionPreProcessor>();
            this.preProcessorQueue = new LinkedBlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>>();
            for (int i = 0; i < num_preProcessors; i++) {
                TransactionPreProcessor t = new TransactionPreProcessor(this, this.preProcessorQueue);
                this.preProcessors.add(t);
            } // FOR
        }
        // Initialize TransactionPostProcessors
        if (num_postProcessors > 0) {
            if (debug.val)
                LOG.debug(String.format("Starting %d %s threads",
                          num_postProcessors, TransactionPostProcessor.class.getSimpleName()));
            this.postProcessors = new ArrayList<TransactionPostProcessor>();
            this.postProcessorQueue = new LinkedBlockingQueue<Object[]>();
            for (int i = 0; i < num_postProcessors; i++) {
                TransactionPostProcessor t = new TransactionPostProcessor(this, this.postProcessorQueue);
                this.postProcessors.add(t);
            } // FOR
        }
    }
    
    /**
     * Initial internal stats sources
     */
    private void initStatSources() {
        StatsSource statsSource = null;

        // TXN PROFILERS
        this.txnProfilerStats = new TransactionProfilerStats(this.catalogContext);
        this.statsAgent.registerStatsSource(SysProcSelector.TXNPROFILER, 0, this.txnProfilerStats);
        
        // MEMORY
        this.memoryStats = new MemoryStats();
        this.statsAgent.registerStatsSource(SysProcSelector.MEMORY, 0, this.memoryStats);
        
        // ANTICACHE MEMORY
        this.anticacheMemoryStats = new AntiCacheMemoryStats();
        this.statsAgent.registerStatsSource(SysProcSelector.MULTITIER_ANTICACHE, 0, this.anticacheMemoryStats);

        // TXN COUNTERS
        statsSource = new TransactionCounterStats(this.catalogContext);
        this.statsAgent.registerStatsSource(SysProcSelector.TXNCOUNTER, 0, statsSource);

        // EXECUTOR PROFILERS
        statsSource = new PartitionExecutorProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.EXECPROFILER, 0, statsSource);
        
        // QUEUE PROFILER
        statsSource = new TransactionQueueManagerProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.QUEUEPROFILER, 0, statsSource);
        
        // ANTI-CACHE PROFILER
        statsSource = new AntiCacheManagerProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.ANTICACHE, 0, statsSource);
        
        // MARKOV ESTIMATOR PROFILER
        statsSource = new MarkovEstimatorProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.MARKOVPROFILER, 0, statsSource);
        
        // SPECEXEC PROFILER
        statsSource = new SpecExecProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.SPECEXECPROFILER, 0, statsSource);
        
        // CLIENT INTERFACE PROFILER
        statsSource = new SiteProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.SITEPROFILER, 0, statsSource);
        
        // BATCH PLANNER PROFILER
        statsSource = new BatchPlannerProfilerStats(this, this.catalogContext);
        this.statsAgent.registerStatsSource(SysProcSelector.PLANNERPROFILER, 0, statsSource);
        
    }
    
    // -------------------------------
    // SNAPSHOTTING SETUP
    // -------------------------------
    
    /**
     * Returns the directory where snapshot files are stored
     * @return
     */
    public File getSnapshotDir() {
        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(this.hstore_conf.site.snapshot_dir);

        synchronized (HStoreSite.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNC

        File dbDirPath = new File(base_dir);

        if (this.hstore_conf.site.snapshot_reset) {
            LOG.warn(String.format("Deleting snapshot directory '%s'", dbDirPath));
            FileUtil.deleteDirectory(dbDirPath);
        }
        FileUtil.makeDirIfNotExists(dbDirPath);

        return (dbDirPath);
    }
    
    /**
     * Thread that is periodically executed to take snapshots
     */
    @SuppressWarnings("unused")
    private final ExceptionHandlingRunnable snapshotter = new ExceptionHandlingRunnable() {
        @Override
        public void runImpl() {
            synchronized(HStoreSite.this) {
                try {
                    // take snapshot
                    takeSnapshot();                    
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        }
    };

    /**
     * Take snapshots
     */
    private void takeSnapshot(){
        // Do this only on site lowest id
        Host catalog_host = this.getHost();
        Integer lowest_site_id = Integer.MAX_VALUE, s_id;

        for (Site st : CatalogUtil.getAllSites(catalog_host)) {
            s_id = st.getId();
            lowest_site_id = Math.min(s_id, lowest_site_id);
        }

        int m_siteId = this.getSiteId();
        
        if (m_siteId == lowest_site_id) {
            if (debug.val) LOG.warn("Taking snapshot at site "+m_siteId);
            try {
                File snapshotDir = this.getSnapshotDir();
                String path = snapshotDir.getAbsolutePath();

                java.util.Date date = new java.util.Date();
                Timestamp current = new Timestamp(date.getTime());
                String nonce = Long.toString(current.getTime());

                CatalogContext cc = this.getCatalogContext();
                String procName = VoltSystemProcedure.procCallName(SnapshotSave.class);
                Procedure catalog_proc = cc.procedures.getIgnoreCase(procName);

                ParameterSet params = new ParameterSet();
                params.setParameters(
                        path,  // snapshot dir
                        nonce, // nonce - timestamp
                        1      // block
                        );

                int base_partition = Collections.min(this.local_partitions);

                RpcCallback<ClientResponseImpl> callback = new RpcCallback<ClientResponseImpl>() {
                    @Override
                    public void run(ClientResponseImpl parameter) {
                        // Do nothing!
                    }
                };

                LocalTransaction ts = this.txnInitializer.createLocalTransaction(
                        null, 
                        EstTime.currentTimeMillis(), 
                        99999999, 
                        base_partition, 
                        catalog_proc, 
                        params, 
                        callback
                        );

                LOG.warn("Queuing snapshot transaction : base partition : "+base_partition+" path :"+ path + " nonce :"+ nonce);

                // Queue @SnapshotSave transaction
                this.transactionQueue(ts);

            } catch (Exception ex) {
                ex.printStackTrace();
                LOG.fatal("SnapshotSave exception: " + ex.getMessage());
                this.hstore_coordinator.shutdown();
            }
        }        
        
    }
    
    
    /**
     * Schedule all the periodic works
     */
    private void initPeriodicWorks() {
        
        // Make sure that we always initialize the periodic thread so that
        // we can ensure that it only shows up on the cores that we want it to.
        this.threadManager.initPerioidicThread();
        if (debug.val) LOG.debug("init periodic thread");
        
        // Periodic Work Processor
        this.threadManager.schedulePeriodicWork(new ExceptionHandlingRunnable() {
            @Override
            public void runImpl() {
                try {
                    HStoreSite.this.processPeriodicWork();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        }, 0, hstore_conf.site.exec_periodic_interval, TimeUnit.MILLISECONDS);
        if (debug.val) LOG.debug("exec periodic interval");
        
        // Heartbeats
        this.threadManager.schedulePeriodicWork(new ExceptionHandlingRunnable() {
            @Override
            public void runImpl() {
                try {
                    if (HStoreSite.this.hstore_coordinator != null) {
                        HStoreSite.this.hstore_coordinator.sendHeartbeat();
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        }, hstore_conf.site.network_heartbeats_interval,
           hstore_conf.site.network_heartbeats_interval, TimeUnit.MILLISECONDS);
        if (debug.val) LOG.debug("heartbeat");
        
        // HStoreStatus
        if (hstore_conf.site.status_enable) {
            this.threadManager.schedulePeriodicWork(
                this.status_monitor,
                hstore_conf.site.status_interval,
                hstore_conf.site.status_interval,
                TimeUnit.MILLISECONDS);
        }
        if (debug.val) LOG.info("exec status enable");
        
        // AntiCache Memory Monitor
        if (debug.val) LOG.debug("about to starting memory monitor thread");
        if (this.anticacheManager != null) {
            if (debug.val) LOG.debug("acm not null");
            if (this.anticacheManager.getEvictableTables().isEmpty() == false) {
                if (debug.val) LOG.debug("get evictables true");
                this.threadManager.schedulePeriodicWork(
                        this.anticacheManager.getMemoryMonitorThread(),
                        hstore_conf.site.anticache_check_interval,
                        hstore_conf.site.anticache_check_interval,
                        TimeUnit.MILLISECONDS);
            } else {
                LOG.warn("There are no tables marked as evictable. Disabling anti-cache monitoring");
            }
        }
        
        // small stats samples
        this.threadManager.schedulePeriodicWork(new ExceptionHandlingRunnable() {
            @Override
            public void runImpl() {
                SystemStatsCollector.asyncSampleSystemNow(false, false);
            }
        }, 0, 5, TimeUnit.SECONDS);

        // medium stats samples
        this.threadManager.schedulePeriodicWork(new ExceptionHandlingRunnable() {
            @Override
            public void runImpl() {
                SystemStatsCollector.asyncSampleSystemNow(true, false);
            }
        }, 0, 1, TimeUnit.MINUTES);

        // large stats samples
        this.threadManager.schedulePeriodicWork(new ExceptionHandlingRunnable() {
            @Override
            public void runImpl() {
                SystemStatsCollector.asyncSampleSystemNow(true, true);
            }
        }, 0, 6, TimeUnit.MINUTES);
        
        // Take Snapshots
        /* Disable for now
        if (this.hstore_conf.site.snapshot) {
                this.threadManager.schedulePeriodicWork(
                        this.snapshotter,
                        hstore_conf.site.snapshot_interval,
                        hstore_conf.site.snapshot_interval,
                        TimeUnit.MILLISECONDS);
        }
        */
        
    }
        
    // ----------------------------------------------------------------------------
    // INTERFACE METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void updateConf(HStoreConf hstore_conf, String[] changed) {
        if (hstore_conf.site.profiling && this.profiler == null) {
            this.profiler = new HStoreSiteProfiler();
        }
        
        // Push the updates to all of our PartitionExecutors
        for (PartitionExecutor executor : this.executors) {
            if (executor == null) continue;
            executor.updateConf(hstore_conf, null);
        } // FOR
        
        // Update all our other boys
        this.clientInterface.updateConf(hstore_conf, null);
        this.txnQueueManager.updateConf(hstore_conf, null);
    }
    
    // ----------------------------------------------------------------------------
    // ADDITIONAL INITIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    public void addPartitionExecutor(int partition, PartitionExecutor executor) {
        assert(this.shutdown_state != ShutdownState.STARTED);
        assert(executor != null);
        this.executors[partition] = executor;
        this.depTrackers[partition] = new DependencyTracker(executor);
        this.executors[partition].initHStoreSite(this);
    }
    
    /**
     * Return a new HStoreCoordinator for this HStoreSite. Note that this
     * should only be called by HStoreSite.init(), otherwise the 
     * internal state for this HStoreSite will be incorrect. If you want
     * the HStoreCoordinator at runtime, use HStoreSite.getHStoreCoordinator()
     * @return
     */
    protected HStoreCoordinator initHStoreCoordinator() {
        assert(this.shutdown_state != ShutdownState.STARTED);
        return new HStoreCoordinator(this);        
    }
    
    protected void setTransactionIdManagerTimeDelta(long delta) {
        for (TransactionIdManager t : this.txnIdManagers) {
            if (t != null) t.setTimeDelta(delta);
        } // FOR
    }
    
    protected void setThresholds(EstimationThresholds thresholds) {
        this.thresholds = thresholds;
        if (debug.val) LOG.debug("Set new EstimationThresholds: " + thresholds);
    }
    
    // ----------------------------------------------------------------------------
    // CATALOG METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the CatalogContext handle used for this HStoreSite instance
     * @return
     */
    public CatalogContext getCatalogContext() {
        return (this.catalogContext);
    }
    /**
     * Return the Site catalog object for this HStoreSite
     */
    public Site getSite() {
        return (this.catalog_site);
    }
    public int getSiteId() {
        return (this.site_id);
    }
    public String getSiteName() {
        return (this.site_name);
    }
    
    public Host getHost() {
        return (this.catalog_host);
    }
    public int getHostId() {
        return (this.catalog_host.getId());
    }
    
    /**
     * Return the list of partition ids managed by this HStoreSite
     * TODO: Moved to CatalogContext 
     */
    public PartitionSet getLocalPartitionIds() {
        return (this.local_partitions);
    }
    /**
     * Returns true if the given partition id is managed by this HStoreSite
     * @param partition
     * @return
     */
    public boolean isLocalPartition(int partition) {
        assert(partition >= 0);
        assert(partition < this.local_partition_offsets.length) :
            String.format("Invalid partition %d - %s", partition, this.catalogContext.getAllPartitionIds());
        return (this.local_partition_offsets[partition] != -1);
    }
    /**
     * Returns true if the given PartitionSite contains partitions that are
     * all managed by this HStoreSite.
     * @param partitions
     * @return
     */
    public boolean allLocalPartitions(PartitionSet partitions) {
        for (int p : partitions.values()) {
            if (this.local_partition_offsets[p] == -1) {
                return (false);
            }
        } // FOR
        return (true);
    }
    
    // ----------------------------------------------------------------------------
    // THREAD UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    protected final Thread.UncaughtExceptionHandler getExceptionHandler() {
        return (this.exceptionHandler);
    }
    
    /**
     * Start the MapReduceHelper Thread
     */
    private void startMapReduceHelper() {
        synchronized (this.mr_helper) {
            if (this.mr_helper_started) return;
            if (debug.val)
                LOG.debug("Starting " + this.mr_helper.getClass().getSimpleName());
            
            Thread t = new Thread(this.mr_helper);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(this.exceptionHandler);
            t.start();
            this.mr_helper_started = true;
        } // SYNCH
    }
    
    /**
     * Start threads for processing AdHoc queries 
     */
    private void startAdHocHelper() {
        synchronized (this.asyncCompilerWorkThread) {
            if (this.adhoc_helper_started) return;
        
            if (debug.val)
                LOG.debug("Starting " + this.asyncCompilerWorkThread.getClass().getSimpleName());
            this.asyncCompilerWorkThread.start();
            this.adhoc_helper_started = true;
        } // SYNCH
    }
    
    /**
     * Get the MapReduce Helper thread 
     */
    public MapReduceHelperThread getMapReduceHelper() {
        return (this.mr_helper);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    @Override
    public long getInstanceId() {
        return (this.instanceId);
    }
    protected void setInstanceId(long instanceId) {
        if (debug.val) LOG.debug("Setting Cluster InstanceId: " + instanceId);
        this.instanceId = instanceId;
    }
    
    /**
     * Return the HStoreCoordinator instance for this site.
     * <B>Note:</b> The init() method for this site must be called before this can be called. 
     * @return
     */
    public HStoreCoordinator getCoordinator() {
        return (this.hstore_coordinator);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public TransactionQueueManager getTransactionQueueManager() {
        return (this.txnQueueManager);
    }
    public AntiCacheManager getAntiCacheManager() {
        return (this.anticacheManager);
    }
    public ClientInterface getClientInterface() {
        return (this.clientInterface);
    }
    public StatsAgent getStatsAgent() {
        return (this.statsAgent);
    }
    public VoltNetwork getVoltNetwork() {
        return (this.voltNetwork);
    }
    public EstimationThresholds getThresholds() {
        return thresholds;
    }
    public HStoreSiteProfiler getProfiler() {
        return (this.profiler);
    }
    public DBBPool getBufferPool() {
        return (this.buffer_pool);
    }
    public CommandLogWriter getCommandLogWriter() {
        return (this.commandLogger);
    }
    protected final Map<Long, AbstractTransaction> getInflightTxns() {
        return (this.inflight_txns);
    }
    protected final Map<Status, Queue<Long>> getDeletableQueues() {
        return (this.deletable_txns);
    }
    protected final String getRejectionMessage() {
        return (this.REJECTION_MESSAGE);
    }
    
    /**
     * Convenience method to dump out status of this HStoreSite
     * @return
     */
    public String statusSnapshot() {
        return new HStoreSiteStatus(this, hstore_conf).snapshot(true, true, false);
    }
    
    public HStoreThreadManager getThreadManager() {
        return (this.threadManager);
    }
    public PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    public AbstractHasher getHasher() {
        return (this.hasher);
    }
    public TransactionInitializer getTransactionInitializer() {
        return (this.txnInitializer);
    }
    public PartitionExecutor getPartitionExecutor(int partition) {
        PartitionExecutor es = this.executors[partition]; 
        assert(es != null) : 
            String.format("Unexpected null PartitionExecutor for partition #%d on %s",
                          partition, this.getSiteName());
        return (es);
    }
    public DependencyTracker getDependencyTracker(int partition) {
        return (this.depTrackers[partition]);
    }
    
    public MemoryStats getMemoryStatsSource() {
        return (this.memoryStats);
    }
    
    public AntiCacheMemoryStats getAntiCacheMemoryStatsSource() {
        return (this.anticacheMemoryStats);
    }

    public Collection<TransactionPreProcessor> getTransactionPreProcessors() {
        return (this.preProcessors);
    }
    public boolean hasTransactionPreProcessors() {
        return (this.preProcessors != null && this.preProcessors.isEmpty() == false);
    }
    public Collection<TransactionPostProcessor> getTransactionPostProcessors() {
        return (this.postProcessors);
    }
    public boolean hasTransactionPostProcessors() {
        return (this.postProcessors != null && this.postProcessors.isEmpty() == false);
    }
    
    /**
     * Get the TransactionIdManager for the given partition
     * If there are not separate managers per partition, we will just
     * return the global one for this HStoreSite 
     * @param partition
     * @return
     */
    public TransactionIdManager getTransactionIdManager(int partition) {
        if (this.txnIdManagers.length == 1) {
            return (this.txnIdManagers[0]);
        } else {
            return (this.txnIdManagers[partition]);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractTransaction> T getTransaction(Long txn_id) {
        assert(txn_id != null) : "Null txnId";
        return ((T)this.inflight_txns.get(txn_id));
    }

    // ----------------------------------------------------------------------------
    // LOCAL PARTITION OFFSETS
    // ----------------------------------------------------------------------------
    
    /**
     * For the given partition id, return its offset in the list of 
     * all the local partition ids managed by this HStoreSite.
     * This will fail if the given partition is not local to this HStoreSite.
     * @param partition
     * @return
     */
    @Deprecated
    public int getLocalPartitionOffset(int partition) {
        assert(partition < this.local_partition_offsets.length) :
            String.format("Unable to get offset of local partition %d %s [hashCode=%d]",
                          partition, Arrays.toString(this.local_partition_offsets), this.hashCode());
        return this.local_partition_offsets[partition];
    }
    
    // ----------------------------------------------------------------------------
    // EVENT OBSERVABLES
    // ----------------------------------------------------------------------------

    /**
     * Get the Observable handle for this HStoreSite that can alert others when the party is
     * getting started
     */
    public EventObservable<HStoreSite> getReadyObservable() {
        return (this.ready_observable);
    }
    /**
     * Get the Observable handle for this HStore for when the first non-sysproc
     * transaction request arrives and we are technically beginning the workload
     * portion of a benchmark run.
     */
    public EventObservable<HStoreSite> getStartWorkloadObservable() {
        return (this.startWorkload_observable);
    }
    
    private synchronized void notifyStartWorkload() {
        if (this.startWorkload == false) {
            this.startWorkload = true;
            this.startWorkload_observable.notifyObservers(this);
        }
    }
    
    /**
     * Get the EventObservable handle for this HStoreSite that can alert 
     * others when we have gotten a message to prepare to shutdown
     * @return
     */
    public EventObservable<Object> getPrepareShutdownObservable() {
        return (this.prepare_observable);
    }
    
    /**
     * Get the EventObservable handle for this HStoreSite that can alert 
     * others when the party is ending
     * @return
     */
    public EventObservable<Object> getShutdownObservable() {
        return (this.shutdown_observable);
    }
    

    
    /**
     * Launch all of the threads needed by this HStoreSite. This is a blocking call
     */
    @Override
    public void run() {
        if (this.ready) {
            throw new RuntimeException("Trying to start " + this.getSiteName() + " more than once");
        }
        
        this.init();
        
        // ARIES
        if (this.hstore_conf.site.aries && this.hstore_conf.site.aries_forward_only == false) {
            doPhysicalRecovery();
            waitForAriesLogInit();
        }
        
        // LOGICAL
        if (this.hstore_conf.site.snapshot){
            doLogicalRecovery();
        }
        
        try {
            this.clientInterface.startAcceptingConnections();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        this.shutdown_state = ShutdownState.STARTED;
//        if (hstore_conf.site.network_profiling) {
//            this.profiler.network_idle_time.start();
//        }
        this.ready = true;
        this.ready_observable.notifyObservers(this);

        // IMPORTANT: This message must always be printed in order for the BenchmarkController
        //            to know that we're ready! That's why we have to use System.out instead of LOG
        String msg = String.format("%s : Site=%s / Address=%s:%d / Partitions=%s",
                                   HStoreConstants.SITE_READY_MSG,
                                   this.getSiteName(),
                                   this.catalog_site.getHost().getIpaddr(),
                                   CollectionUtil.first(CatalogUtil.getExecutionSitePorts(this.catalog_site)),
                                   this.local_partitions);
        System.out.println(msg);
        System.out.flush();
        
        // We will join on our HStoreCoordinator thread. When that goes
        // down then we know that the whole party is over
        try {
            this.hstore_coordinator.getListenerThread().join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            RingBufferAppender appender = RingBufferAppender.getRingBufferAppender(LOG);
            if (appender != null) {
                int width = 100;
                System.err.println(StringUtil.header(appender.getClass().getSimpleName(), "=", width));
                for (String log : appender.getLogMessages()) {
                    System.err.println(log.trim());
                }
                System.err.println(StringUtil.repeat("=", width));
                System.err.flush();
            }
        }
    }
    
    /**
     * Returns true if this HStoreSite is fully initialized and running
     * This will be set to false if the system is shutting down
     */
    public boolean isRunning() {
        return (this.ready);
    }   
        
    // ARIES
    public void doPhysicalRecovery() {
        while (!m_ariesLog.isReadyForReplay()) {
            try {
                // don't sleep for too long as recovery numbers might get biased
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }        

        LOG.info("ARIES : ariesLog is ready for replay at site :"+this.site_id);

        if (!m_ariesLog.isRecoveryCompleted()) {
            int m_siteId = this.getSiteId();
            CatalogMap<Partition> partitionMap = this.catalog_site.getPartitions();

            for (Partition pt : partitionMap ) {
                PartitionExecutor pe =  getPartitionExecutor(pt.getId());
                assert (pe != null);

                ExecutionEngine ee = pe.getExecutionEngine();
                assert (ee != null);

                int m_partitionId = pe.getPartitionId();

                LOG.info("ARIES : start recovery at partition  :"+m_partitionId+" on site :"+m_siteId);
                
                if (!m_ariesLog.isRecoveryCompletedForSite(m_partitionId)) {
                    ee.doAriesRecoveryPhase(m_ariesLog.getPointerToReplayLog(), m_ariesLog.getReplayLogSize(), m_ariesLog.getTxnIdToBeginReplay());
                    m_ariesLog.setRecoveryCompleted(m_partitionId);                
                }
            }
        }

        LOG.info("ARIES : recovery completed at site :"+this.site_id);
    }
    
    private void waitForAriesLogInit() {
        // wait for the main thread to complete Aries recovery
        // and initialize the log
        //LOG.warn("ARIES : wait for log to be inititalized at site :"+this.site_id);
        while (!m_ariesLog.isInitialized) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //LOG.warn("ARIES : log is inititalized at site :"+this.site_id);
    }        
    
    // LOGICAL
    public void doLogicalRecovery() {        
        LOG.warn("Logical : recovery at site with min id :" + this.site_id);
                
        //XXX Load snapshot using @SnapshotRestore
        //XXX Load command log and redo all entires
     
        LOG.warn("Logical : recovery completed on site with min id :" + this.site_id);
    }

        
    // ----------------------------------------------------------------------------
    // SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    @Override
    public void prepareShutdown(boolean error) {
        this.shutdown_state = ShutdownState.PREPARE_SHUTDOWN;

        if (ProcedureProfiler.workloadTrace instanceof Workload) {
            try {
                ((Workload)ProcedureProfiler.workloadTrace).flush();
            } catch (Throwable ex) {
                LOG.error("Failed to flush workload trace", ex);
            }
        }
        
        if (this.hstore_coordinator != null)
            this.hstore_coordinator.prepareShutdown(false);
        
        try {
            this.txnQueueManager.prepareShutdown(error);
        } catch (Throwable ex) {
            LOG.error("Unexpected error when preparing " +
                     this.txnQueueManager.getClass().getSimpleName() + " for shutdown", ex);
        }
        this.clientInterface.prepareShutdown(error);
        
        if (this.preProcessors != null) {
            for (TransactionPreProcessor tpp : this.preProcessors) {
                tpp.prepareShutdown(false);
            } // FOR
        }
        if (this.postProcessors != null) {
            for (TransactionPostProcessor tpp : this.postProcessors) {
                tpp.prepareShutdown(false);
            } // FOR
        }
        
        if (this.mr_helper != null) {
            this.mr_helper.prepareShutdown(error);
        }
        if (this.commandLogger != null) {
            this.commandLogger.prepareShutdown(error);
        }
        if (this.anticacheManager != null) {
            this.anticacheManager.prepareShutdown(error);
        }
        for (TransactionCleaner t : this.txnCleaners) {
            t.prepareShutdown(error);
        } // FOR

        if (this.adhoc_helper_started) {
            if (this.asyncCompilerWorkThread != null)
                this.asyncCompilerWorkThread.prepareShutdown(error);
        }
        
        for (int p : this.local_partitions.values()) {
            if (this.executors[p] != null) 
                this.executors[p].prepareShutdown(error);
        } // FOR
        
        // Tell anybody that wants to know that we're going down
        if (trace.val) LOG.trace(String.format("Notifying %d observers that we're preparing shutting down",
                         this.prepare_observable.countObservers()));
        this.prepare_observable.notifyObservers(error);
        
        // *********************************** DEBUG ***********************************
        
        Logger root = Logger.getRootLogger();
//        if (error && RingBufferAppender.getRingBufferAppender(LOG) != null) {
//            root.info("Flushing RingBufferAppender logs");
//            for (Appender appender : CollectionUtil.iterable(root.getAllAppenders(), Appender.class)) {
//                LOG.addAppender(appender);    
//            } // FOR
//        }
        if (debug.val) root.debug("Preparing to shutdown. Flushing all logs");
        LoggerUtil.flushAllLogs();
        
        if (this.deletable_last.isEmpty() == false) {
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (String txn : this.deletable_last) {
                sb.append(String.format(" [%02d] %s\n", i++, txn));
                // sb.append(String.format(" [%02d]\n%s\n", i++, StringUtil.prefix(txn, "  | ")));
            }
            LOG.info("Last Deleted Transactions:\n" + sb + "\n\n");
        }
        
//        sb = new StringBuilder();
//        i = 0;
//        for (Long txn : this.deletable_txns[Status.OK.ordinal()]) {
//            sb.append(String.format(" [%02d] %s\n", i++, this.inflight_txns.get(txn).debug()));
//        }
//        LOG.info("Waiting to be Deleted Transactions:\n" + sb);
    }
    
    /**
     * Perform shutdown operations for this HStoreSiteNode
     */
    @Override
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
//            if (debug.val)
                LOG.warn("Already told to shutdown... Ignoring");
            return;
        }
        if (this.shutdown_state != ShutdownState.PREPARE_SHUTDOWN) this.prepareShutdown(false);
        this.shutdown_state = ShutdownState.SHUTDOWN;
        if (debug.val) LOG.debug("Shutting down everything at " + this.getSiteName());

        // Stop the monitor thread
        if (this.status_monitor != null) this.status_monitor.shutdown();
        
        // Kill the queue manager
        this.txnQueueManager.shutdown();
        
        if (this.mr_helper_started && this.mr_helper != null) {
            this.mr_helper.shutdown();
        }
        if (this.commandLogger != null) {
            this.commandLogger.shutdown();
        }
        if (this.anticacheManager != null) {
            this.anticacheManager.shutdown();
        }
        for (TransactionCleaner t : this.txnCleaners) {
            t.shutdown();
        } // FOR
      
        // this.threadManager.getPeriodicWorkExecutor().shutdown();
        
        // Stop AdHoc threads
        if (this.adhoc_helper_started) {
            if (this.asyncCompilerWorkThread != null)
                this.asyncCompilerWorkThread.shutdown();
        }

        if (this.preProcessors != null) {
            for (TransactionPreProcessor tpp : this.preProcessors) {
                tpp.shutdown();
            } // FOR
        }
        if (this.postProcessors != null) {
            for (TransactionPostProcessor tpp : this.postProcessors) {
                tpp.shutdown();
            } // FOR
        }
        
        // Tell anybody that wants to know that we're going down
        if (trace.val) LOG.trace("Notifying " + this.shutdown_observable.countObservers() + " observers that we're shutting down");
        this.shutdown_observable.notifyObservers();
        
        // Tell our local boys to go down too
        for (int p : this.local_partitions.values()) {
            if (this.executors[p] != null) this.executors[p].shutdown();
        } // FOR
        if (this.hstore_coordinator != null) {
            this.hstore_coordinator.shutdown();
        }
        
        if (this.txn_profiler_dumper != null) {
            try {
                this.txn_profiler_dumper.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        if (this.voltNetwork != null) {
            try {
                this.voltNetwork.shutdown();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            this.clientInterface.shutdown();
        }
        
        LOG.info(String.format("Completed shutdown process at %s [instanceId=%d]",
                               this.getSiteName(), this.instanceId));
    }
    
    /**
     * Returns true if HStoreSite is in the process of shutting down
     * @return
     */
    @Override
    public boolean isShuttingDown() {
        return (this.shutdown_state == ShutdownState.SHUTDOWN);
    }
    
    // ----------------------------------------------------------------------------
    // INCOMING INVOCATION HANDLER METHODS
    // ----------------------------------------------------------------------------
    
    protected void invocationQueue(ByteBuffer buffer, ClientInputHandler handler, Connection c) {
        int messageSize = buffer.capacity();
        RpcCallback<ClientResponseImpl> callback = new ClientResponseCallback(this.clientInterface, c, messageSize);
        this.clientInterface.increaseBackpressure(messageSize);
        
        if (this.preProcessorQueue != null) {
            this.preProcessorQueue.add(Pair.of(buffer, callback));
        } else {
            this.invocationProcess(buffer, callback);
        }
    }
    
    /**
     * This is legacy method needed for using Evan's VoltProcedureListener.
     */
    @Override
    @Deprecated
    public void invocationQueue(ByteBuffer buffer, final RpcCallback<byte[]> clientCallback) {
        // XXX: This is a big hack. We should just deal with the ClientResponseImpl directly
        RpcCallback<ClientResponseImpl> wrapperCallback = new RpcCallback<ClientResponseImpl>() {
            @Override
            public void run(ClientResponseImpl parameter) {
                if (trace.val) LOG.trace("Serializing ClientResponse to byte array:\n" + parameter);
                
                FastSerializer fs = new FastSerializer();
                try {
                    parameter.writeExternal(fs);
                    clientCallback.run(fs.getBBContainer().b.array());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    fs.clear();
                }
            }
        };
        
        if (this.preProcessorQueue != null) {
            this.preProcessorQueue.add(Pair.of(buffer, wrapperCallback));
        } else {
            this.invocationProcess(buffer, wrapperCallback);
        }
    }
    
    /**
     * This is the main method that takes in a ByteBuffer request from the client and queues
     * it up for execution. The clientCallback expects to get back a ClientResponse generated
     * after the txn is executed. 
     * @param buffer
     * @param clientCallback
     */
    public void invocationProcess(ByteBuffer buffer, RpcCallback<ClientResponseImpl> clientCallback) {
//        if (hstore_conf.site.network_profiling || hstore_conf.site.txn_profiling) {
//            long timestamp = ProfileMeasurement.getTime();
//            if (hstore_conf.site.network_profiling) {
//                ProfileMeasurement.swap(timestamp, this.profiler.network_idle_time, this.profiler.network_processing_time);
//            }
//        }
        
        long timestamp = -1;
        if (hstore_conf.global.nanosecond_latencies) {
            timestamp = System.nanoTime();
        } else {
            timestamp = System.currentTimeMillis();
            EstTimeUpdater.update(timestamp);
        }

        // Extract the stuff we need to figure out whether this guy belongs at our site
        // We don't need to create a StoredProcedureInvocation anymore in order to
        // extract out the data that we need in this request
        final FastDeserializer incomingDeserializer = this.incomingDeserializers.get();
        incomingDeserializer.setBuffer(buffer);
        final long client_handle = StoredProcedureInvocation.getClientHandle(buffer);
        final int procId = StoredProcedureInvocation.getProcedureId(buffer);
        int base_partition = StoredProcedureInvocation.getBasePartition(buffer);
        if (debug.val)
            LOG.debug(String.format("Raw Request: clientHandle=%d / basePartition=%d / procId=%d / procName=%s",
                      client_handle, base_partition, 
                      procId, StoredProcedureInvocation.getProcedureName(incomingDeserializer)));
        
        // Optimization: We can get the Procedure catalog handle from its procId
        Procedure catalog_proc = catalogContext.getProcedureById(procId);
     
        // Otherwise, we have to get the procedure name and do a look up with that.
        if (catalog_proc == null) {
            String procName = StoredProcedureInvocation.getProcedureName(incomingDeserializer);
            catalog_proc = this.catalogContext.procedures.getIgnoreCase(procName);
            if (catalog_proc == null) {
                String msg = "Unknown procedure '" + procName + "'";
                this.responseError(client_handle,
                                   Status.ABORT_UNEXPECTED,
                                   msg,
                                   clientCallback,
                                   timestamp);
                return;
            }
        }
        boolean sysproc = catalog_proc.getSystemproc();
        
        // -------------------------------
        // PARAMETERSET INITIALIZATION
        // -------------------------------
        
        // Extract just the ParameterSet from the StoredProcedureInvocation
        // We will deserialize the rest of it later
        ParameterSet procParams = new ParameterSet();
        try {
            StoredProcedureInvocation.seekToParameterSet(buffer);
            incomingDeserializer.setBuffer(buffer);
            procParams.readExternal(incomingDeserializer);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } 
        assert(procParams != null) :
            "The parameters object is null for new txn from client #" + client_handle;
        if (debug.val)
            LOG.debug(String.format("Received new stored procedure invocation request for %s [handle=%d]",
                      catalog_proc.getName(), client_handle));
        
        // System Procedure Check
        // If this method returns true, then we want to halt processing the
        // request any further and immediately return
        if (sysproc && this.processSysProc(client_handle, catalog_proc, procParams, clientCallback)) {
            return;
        }
        
        // If this is the first non-sysproc transaction that we've seen, then
        // we will notify anybody that is waiting for this event. This is used to clear
        // out any counters or profiling information that got recorded when we were loading data
        if (this.startWorkload == false && sysproc == false) {
            this.notifyStartWorkload();
        }
        
        // -------------------------------
        // BASE PARTITION
        // -------------------------------

        // The base partition is where this txn's Java stored procedure will run on
        if (base_partition == HStoreConstants.NULL_PARTITION_ID) {
            base_partition = this.txnInitializer.calculateBasePartition(client_handle,
                                                                        catalog_proc,
                                                                        procParams,
                                                                        base_partition);
        }
        
        // Profiling Updates
        if (hstore_conf.site.txn_counters) TransactionCounter.RECEIVED.inc(catalog_proc);
        if (hstore_conf.site.profiling && base_partition != HStoreConstants.NULL_PARTITION_ID) {
            synchronized (profiler.network_incoming_partitions) {
                profiler.network_incoming_partitions.put(base_partition);
            } // SYNCH
        }
        
        // -------------------------------
        // REDIRECT TXN TO PROPER BASE PARTITION
        // -------------------------------
        if (this.isLocalPartition(base_partition) == false) {
            // If the base_partition isn't local, then we need to ship it off to
            // the right HStoreSite
            this.transactionRedirect(catalog_proc, buffer, base_partition, clientCallback);
            return;
        }
        
        // 2012-12-24 - We always want the network threads to do the initialization
        if (trace.val)
            LOG.trace("Initializing transaction request using network processing thread");
        LocalTransaction ts = this.txnInitializer.createLocalTransaction(
                                        buffer,
                                        timestamp,
                                        client_handle,
                                        base_partition,
                                        catalog_proc,
                                        procParams,
                                        clientCallback);
        this.transactionQueue(ts);
        if (trace.val)
            LOG.trace(String.format("Finished initial processing of new txn."));
//        if (hstore_conf.site.network_profiling) {
//            ProfileMeasurement.swap(this.profiler.network_processing_time, this.profiler.network_idle_time);
//        }
    }
    
    
    /**
     * Special handling for certain incoming sysproc requests. These are just for
     * specialized sysprocs where we need to do some pre-processing that is separate
     * from how the regular sysproc txns are executed.
     * @param catalog_proc
     * @param clientCallback
     * @param request
     * @return True if this request was handled and the caller does not need to do anything further
     */
    private boolean processSysProc(long client_handle,
                                   Procedure catalog_proc,
                                   ParameterSet params,
                                   RpcCallback<ClientResponseImpl> clientCallback) {
        
        // -------------------------------
        // SHUTDOWN
        // TODO: Execute as a regular sysproc transaction
        // -------------------------------
        if (catalog_proc.getName().equalsIgnoreCase("@Shutdown")) {
            ClientResponseImpl cresponse = new ClientResponseImpl(
                    -1,
                    client_handle,
                    -1,
                    Status.OK,
                    HStoreConstants.EMPTY_RESULT,
                    "");
            this.responseSend(cresponse, clientCallback, EstTime.currentTimeMillis(), 0);

            // Non-blocking....
            Exception error = new Exception("Shutdown command received at " + this.getSiteName());
            this.hstore_coordinator.shutdownCluster(error);
            return (true);
        }
        
        // -------------------------------
        // QUIESCE
        // -------------------------------
//        else if (catalog_proc.getName().equals("@Quiesce")) {
//            // Tell the queue manager ahead of time to wipe out everything!
//            this.txnQueueManager.clearQueues();
//            return (false);
//        }
        
        // -------------------------------
        // EXECUTOR STATUS
        // -------------------------------
        else if (catalog_proc.getName().equalsIgnoreCase("@ExecutorStatus")) {
            if (this.status_monitor != null) {
                this.status_monitor.printStatus();
                RingBufferAppender appender = RingBufferAppender.getRingBufferAppender(LOG);
                if (appender != null) appender.dump(System.err);
            }
            ClientResponseImpl cresponse = new ClientResponseImpl(
                    -1,
                    client_handle,
                    -1,
                    Status.OK,
                    HStoreConstants.EMPTY_RESULT,
                    "");
            this.responseSend(cresponse, clientCallback, EstTime.currentTimeMillis(), 0);
            return (true);
        }
        
        // -------------------------------
        // ADHOC
        // -------------------------------
        else if (catalog_proc.getName().equalsIgnoreCase("@AdHoc")) {
            String msg = null;
            
            // Is this feature disabled?
            if (hstore_conf.site.exec_adhoc_sql == false) {
                msg = "AdHoc queries are disabled";
            }
            // Check that variable 'request' in this func. is same as 
            // 'task' in ClientInterface.handleRead()
            else if (params.size() != 1) {
                msg = "AdHoc system procedure requires exactly one parameter, " +
                      "the SQL statement to execute.";
            }
            
            if (msg != null) {
                this.responseError(client_handle,
                                   Status.ABORT_GRACEFUL,
                                   msg,
                                   clientCallback,
                                   EstTime.currentTimeMillis());
                return (true);
            }
            
            // Check if we need to start our threads now
            if (this.adhoc_helper_started == false) {
                this.startAdHocHelper();
            }
            
            // Create a LocalTransaction handle that will carry into the
            // the adhoc compiler. Since we don't know what this thing will do, we have
            // to assume that it needs to touch all partitions.
            int idx = (int)(Math.abs(client_handle) % this.local_partitions.size());
            int base_partition = this.local_partitions.values()[idx];
            
            LocalTransaction ts = this.txnInitializer.createLocalTransaction(null,
                                                                             EstTime.currentTimeMillis(),
                                                                             client_handle,
                                                                             base_partition,
                                                                             catalog_proc,
                                                                             params,
                                                                             clientCallback);
            String sql = (String)params.toArray()[0];
            this.asyncCompilerWorkThread.planSQL(ts, sql);
            return (true);
        }
        
        return (false);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION OPERATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Queue a new transaction for initialization and execution.
     * If it is a single-partition txn, then it will be queued at its base 
     * partition's PartitionExecutor queue. If it is distributed transaction,
     * then it will need to first acquire the locks for all of the partitions
     * that it wants to access.
     * @param ts
     */
    public void transactionQueue(LocalTransaction ts) {
        assert(ts.isInitialized()) : "Uninitialized transaction handle [" + ts + "]";
        
        // Make sure that we start the MapReduceHelperThread
        if (this.mr_helper_started == false && ts.isMapReduce()) {
            assert(this.mr_helper != null);
            this.startMapReduceHelper();
        }
                
        if (debug.val)
            LOG.debug(String.format("%s - Dispatching %s transaction to execute at partition %d [handle=%d]",
                      ts, (ts.isPredictSinglePartition() ? "single-partition" : "distributed"), 
                      ts.getBasePartition(), ts.getClientHandle()));
        
        if (ts.isPredictSinglePartition()) {
            this.transactionInit(ts);
        }
        else {
            LocalInitQueueCallback initCallback = (LocalInitQueueCallback)ts.getInitCallback();
            this.hstore_coordinator.transactionInit(ts, initCallback);
        }
    }
    
    /**
     * Queue the given transaction to be initialized in the local TransactionQueueManager.
     * This is a non-blocking call.
     * @param ts
     */
    public void transactionInit(AbstractTransaction ts) {
        assert(ts.isInitialized()) : "Uninitialized transaction handle [" + ts + "]";
        this.txnQueueManager.queueTransactionInit(ts);
    }
    
    /**
     * Pass a message that sets the current distributed txn at the target partition
     * @param ts
     * @param partition
     */
    public void transactionSetPartitionLock(AbstractTransaction ts, int partition) {
        assert(ts.isInitialized()) : "Uninitialized transaction handle [" + ts + "]";
        assert(this.isLocalPartition(partition)) :
            String.format("Trying to queue %s for %s at non-local partition %d",
                          SetDistributedTxnMessage.class.getSimpleName(), ts, partition);
        this.executors[partition].queueSetPartitionLock(ts);
    }

    /**
     * Queue the transaction to start executing on its base partition.
     * This function can block a transaction executing on that partition
     * <B>IMPORTANT:</B> The transaction could be deleted after calling this if it is rejected
     * @param ts
     */
    public void transactionStart(LocalTransaction ts) {
        if (debug.val)
            LOG.debug(String.format("Starting %s %s on partition %d%s",
                      (ts.isPredictSinglePartition() ? "single-partition" : "distributed"),
                      ts, ts.getBasePartition(),
                      (ts.isPredictSinglePartition() ? "" : " [partitions=" + ts.getPredictTouchedPartitions() + "]")));
        assert(ts.getPredictTouchedPartitions().isEmpty() == false) :
            "No predicted partitions for " + ts + "\n" + ts.debug();
        assert(this.executors[ts.getBasePartition()] != null) :
            "Unable to start " + ts + " - No PartitionExecutor exists for partition #" + ts.getBasePartition() + " at HStoreSite " + this.site_id;
        
        if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startQueueExec();
        final boolean success = this.executors[ts.getBasePartition()].queueStartTransaction(ts);
        
        if (success == false) {
            // Depending on what we need to do for this type txn, we will send
            // either an ABORT_THROTTLED or an ABORT_REJECT in our response
            // An ABORT_THROTTLED means that the client will back-off of a bit
            // before sending another txn request, where as an ABORT_REJECT means
            // that it will just try immediately
            Status status = Status.ABORT_REJECT;
            if (debug.val)
                LOG.debug(String.format("%s - Hit with a %s response from partition %d " +
                          "[queueSize=%d]",
                          ts, status, ts.getBasePartition(),
                          this.executors[ts.getBasePartition()].getDebugContext().getWorkQueueSize()));
            boolean singlePartitioned = ts.isPredictSinglePartition();
            if (singlePartitioned == false) {
                LocalFinishCallback finish_callback = ts.getFinishCallback();
                finish_callback.init(ts, status);
                this.hstore_coordinator.transactionFinish(ts, status, finish_callback);
            }
            // We will want to delete this transaction after we reject it if it is a single-partition txn
            // Otherwise we will let the normal distributed transaction process clean things up
            this.transactionReject(ts, status);
            if (singlePartitioned) this.queueDeleteTransaction(ts.getTransactionId(), status);
        }        
    }
    
    /**
     * Execute a WorkFragment on a particular PartitionExecutor
     * @param request
     * @param clientCallback
     */
    public void transactionWork(AbstractTransaction ts, WorkFragment fragment) {
        if (debug.val)
            LOG.debug(String.format("%s - Queuing %s on partition %d [prefetch=%s]",
                      ts, fragment.getClass().getSimpleName(),
                      fragment.getPartitionId(), fragment.getPrefetch()));
        assert(this.isLocalPartition(fragment.getPartitionId())) :
            "Trying to queue work for " + ts + " at non-local partition " + fragment.getPartitionId();
        
        if (hstore_conf.site.specexec_enable && ts instanceof RemoteTransaction && fragment.hasFutureStatements()) {
            QueryEstimate query_estimate = fragment.getFutureStatements();
            RemoteTransaction remote_ts = (RemoteTransaction)ts;
            RemoteEstimatorState t_state = (RemoteEstimatorState)remote_ts.getEstimatorState();
            if (t_state == null) {
                t_state = this.remoteTxnEstimator.startTransaction(ts.getTransactionId(),
                                                                   ts.getBasePartition(),
                                                                   ts.getProcedure(),
                                                                   null);
                remote_ts.setEstimatorState(t_state);
            }
            if (debug.val)
                LOG.debug(String.format("%s - Updating %s with %d future statement hints for partition %d",
                          ts, t_state.getClass().getSimpleName(),
                          fragment.getFutureStatements().getStmtIdsCount(),
                          fragment.getPartitionId()));
            
            this.remoteTxnEstimator.processQueryEstimate(t_state, query_estimate, fragment.getPartitionId());
        }
        this.executors[fragment.getPartitionId()].queueWork(ts, fragment);
    }

    /**
     * This method is the first part of two phase commit for a transaction.
     * If speculative execution is enabled, then we'll notify each the PartitionExecutors
     * for the listed partitions that it is done. This will cause all the 
     * that are blocked on this transaction to be released immediately and queued 
     * If the second PartitionSet in the arguments is not null, it will be updated with
     * the partitionIds that we called PREPARE on for this transaction 
     * @param ts The transaction handle that we want to prepare.
     * @param partitions The set of partitions to notify that this txn is ready to commit.
     * @param callback The txn's prepare callback for this invocation.
     */
    public void transactionPrepare(AbstractTransaction ts,
                                   PartitionSet partitions,
                                   PartitionCountingCallback<? extends AbstractTransaction> callback) {
        if (debug.val)
            LOG.debug(String.format("2PC:PREPARE %s [partitions=%s]", ts, partitions));
        
        assert(callback.isInitialized());
        for (int partition : this.local_partitions.values()) {
            if (partitions.contains(partition) == false) continue;
            
            // If this txn is already prepared at this partition, then we 
            // can skip processing it at the PartitionExecutor and update
            // the callback right here
            if (ts.isMarkedPrepared(partition)) {
                callback.run(partition);
            }
            else {
                // TODO: If this txn is read-only, then we should invoke finish right here
                // Because this txn didn't change anything at this partition, we should
                // release all of its locks and immediately allow the partition to execute
                // transactions without speculative execution. We sort of already do that
                // because we will allow spec exec read-only txns to commit immediately 
                // but it would reduce the number of messages that the base partition needs
                // to wait for when it does the 2PC:FINISH
                // Berstein's book says that most systems don't actually do this because a txn may 
                // need to execute triggers... but since we don't have any triggers we can do it!
                // More Info: https://github.com/apavlo/h-store/issues/31
                // If speculative execution is enabled, then we'll turn it on at the PartitionExecutor
                // for this partition
                this.executors[partition].queuePrepare(ts, callback);
            }
        } // FOR
    }
    
    /**
     * This method is used to finish a distributed transaction.
     * The PartitionExecutor will either commit or abort the transaction at the specified partitions
     * This is a non-blocking call that doesn't wait to know that the txn was finished successfully at 
     * each PartitionExecutor.
     * @param txn_id
     * @param status
     * @param partitions
     */
    public void transactionFinish(Long txn_id, Status status, PartitionSet partitions) {
        if (debug.val)
            LOG.debug(String.format("2PC:FINISH Txn #%d [status=%s, partitions=%s]",
                      txn_id, status, partitions));
        
        // If we don't have a AbstractTransaction handle, then we know that we never did anything
        // for this transaction and we can just ignore this finish request.
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        if (ts == null) {
            if (debug.val)
                LOG.warn(String.format("No transaction information exists for #%d." +
                           "Ignoring finish request", txn_id));
            return;
        }
        
        // Set the status in case something goes awry and we just want
        // to check whether this transaction is suppose to be aborted.
        // XXX: Why is this needed?
        ts.setStatus(status);
        
        // We only need to do this for distributed transactions, because all single-partition
        // transactions will commit/abort immediately
        if (ts.isPredictSinglePartition() == false) {
//            PartitionCountingCallback<AbstractTransaction> callback = null;
            for (int partition : this.local_partitions.values()) {
                if (partitions.contains(partition) == false) continue;
                
                // 2013-01-11
                // We can check to see whether the txn was ever released at the partition.
                // If it wasn't then we know that we don't need to queue a finish message
                // This is to allow the PartitionExecutor to spend more time processing other
                // more useful stuff.
//                if (ts.isMarkedReleased(partition)) {
                    if (trace.val)
                        LOG.trace(String.format("%s - Queuing transaction to get finished on partition %d",
                                  ts, partition));
                    try {
                        this.executors[partition].queueFinish(ts, status);
                    } catch (Throwable ex) {
                        LOG.error(String.format("Unexpected error when trying to finish %s\nHashCode: %d / Status: %s / Partitions: %s",
                                  ts, ts.hashCode(), status, partitions));
                        throw new RuntimeException(ex);
                    }
//                }
//                else {
//                    if (callback == null) callback = ts.getFinishCallback();
//                    if (trace.val)
//                        LOG.trace(String.format("%s - Decrementing %s directly for partition %d",
//                                  ts, callback.getClass().getSimpleName(), partition));
//                    callback.run(partition);
//                }
            } // FOR
        }
    }

    // ----------------------------------------------------------------------------
    // FAILED TRANSACTIONS (REQUEUE / REJECT / RESTART)
    // ----------------------------------------------------------------------------
    
    /**
     * Send the transaction request to another node for execution. We will create
     * a TransactionRedirectCallback that will automatically send the ClientResponse
     * generated from the remote node for this txn back to the client 
     * @param catalog_proc
     * @param serializedRequest
     * @param base_partition
     * @param clientCallback
     */
    public void transactionRedirect(Procedure catalog_proc,
                                    ByteBuffer serializedRequest,
                                    int base_partition,
                                    RpcCallback<ClientResponseImpl> clientCallback) {
        if (debug.val)
            LOG.debug(String.format("Forwarding %s request to partition %d [clientHandle=%d]",
                     catalog_proc.getName(), base_partition,
                     StoredProcedureInvocation.getClientHandle(serializedRequest)));
        
        // Make a wrapper for the original callback so that when the result comes back frm the remote partition
        // we will just forward it back to the client. How sweet is that??
        RedirectCallback callback = null;
        try {
            callback = new RedirectCallback(this);
            // callback = (RedirectCallback)objectPools.CALLBACKS_TXN_REDIRECT_REQUEST.borrowObject();
            callback.init(clientCallback);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get TransactionRedirectCallback", ex);
        }
        
        // Mark this request as having been redirected
        // XXX: This sucks because we have to copy the bytes, which will then
        // get copied again when we have to serialize it out to a ByteString
        serializedRequest.rewind();
        ByteBuffer copy = ByteBuffer.allocate(serializedRequest.capacity());
        copy.put(serializedRequest);
        StoredProcedureInvocation.setBasePartition(base_partition, copy);
        
        this.hstore_coordinator.transactionRedirect(copy.array(),
                                                    callback,
                                                    base_partition);
        if (hstore_conf.site.txn_counters) TransactionCounter.REDIRECTED.inc(catalog_proc);
    }
    
    /**
     * A non-blocking method to requeue an aborted transaction using the
     * TransactionQueueManager. This allows a PartitionExecutor to tell us that
     * they can't execute some transaction and we'll let the queue manager's 
     * thread take care of it for us.
     * This will eventually call HStoreSite.transactionRestart()
     * @param ts
     * @param status
     */
    public void transactionRequeue(LocalTransaction ts, Status status) {
        assert(ts != null);
        assert(status != Status.OK) :
            "Unexpected requeue status " + status + " for " + ts;
        ts.setStatus(status);
        this.txnQueueManager.restartTransaction(ts, status);
    }
    
    /**
     * Rejects a transaction and returns an empty result back to the client
     * @param ts
     */
    public void transactionReject(LocalTransaction ts, Status status) {
        assert(ts != null) : "Null LocalTransaction handle [status=" + status + "]";
        assert(ts.isInitialized()) : "Uninitialized transaction: " + ts;
        if (debug.val)
            LOG.debug(String.format("%s - Rejecting transaction with status %s [clientHandle=%d]",
                      ts, status, ts.getClientHandle()));
        
        String msg = this.REJECTION_MESSAGE; //  + " - [0]";
        ts.setStatus(status);
        ClientResponseImpl cresponse = new ClientResponseImpl();
        cresponse.init(ts, status, HStoreConstants.EMPTY_RESULT, msg);
        this.responseSend(ts, cresponse);

        if (hstore_conf.site.txn_counters) {
            if (status == Status.ABORT_REJECT) {
                TransactionCounter.REJECTED.inc(ts.getProcedure());
            } else {
                assert(false) : "Unexpected rejection status for " + ts + ": " + status;
            }
        }
    }

    /**
     * Restart the given transaction with a brand new transaction handle.
     * This method will perform the following operations:
     *  (1) Restart the transaction as new multi-partitioned transaction
     *  (2) Mark the original transaction as aborted so that is rolled back
     *  
     * <B>IMPORTANT:</B> If the return status of the transaction is ABORT_REJECT, then
     *                   you will probably need to delete the transaction handle.
     * <B>IMPORTANT:</B> This is a blocking call and should not be invoked by the PartitionExecutor
     *                    
     * @param status Final status of this transaction
     * @param ts
     * @return Returns the final status of this transaction
     */
    public Status transactionRestart(LocalTransaction orig_ts, Status status) {
    //LOG.info(String.format("transaction %d was requested for a restarted", orig_ts.getTransactionId()));
        assert(orig_ts != null) : "Null LocalTransaction handle [status=" + status + "]";
        assert(orig_ts.isInitialized()) : "Uninitialized transaction??";
        if (debug.val)
            LOG.debug(String.format("%s got hit with a %s! " +
                      "Going to clean-up our mess and re-execute [restarts=%d]",
                      orig_ts , status, orig_ts.getRestartCounter()));
        int base_partition = orig_ts.getBasePartition();
        SerializableException orig_error = orig_ts.getPendingError();

        //LOG.info("In transactionRestart()"); 
                
        // If this txn has been restarted too many times, then we'll just give up
        // and reject it outright
        int restart_limit = (orig_ts.isSysProc() ? hstore_conf.site.txn_restart_limit_sysproc :
                                                   hstore_conf.site.txn_restart_limit);
        if (orig_ts.getRestartCounter() > restart_limit) {
            String msg = String.format("%s has been restarted %d times! Rejecting...",
                                       orig_ts, orig_ts.getRestartCounter());
            if (debug.val) LOG.warn(msg);
            if (orig_ts.isSysProc()) {
                throw new RuntimeException(msg);
            } else {
                this.transactionReject(orig_ts, Status.ABORT_REJECT);
                return (Status.ABORT_REJECT);
            }
        }
        
        // -------------------------------
        // REDIRECTION
        // -------------------------------
        if (hstore_conf.site.exec_db2_redirects && 
                 status != Status.ABORT_RESTART &&
                 status != Status.ABORT_SPECULATIVE &&
                 status != Status.ABORT_EVICTEDACCESS) {
            // Figure out whether this transaction should be redirected based on what partitions it
            // tried to touch before it was aborted
            FastIntHistogram touched = orig_ts.getTouchedPartitions();
            
            // XXX: We should probably decrement the base partition by one 
            //      so that we only consider where they actually executed queries
            if (debug.val)
                LOG.debug(String.format("Touched partitions for mispredicted %s\n%s",
                          orig_ts, touched));
            int redirect_partition = HStoreConstants.NULL_PARTITION_ID;
            if (touched.getValueCount() == 1) {
                redirect_partition = touched.getMaxValue();
            }
            // If the original base partition is in our most touched set, then
            // we'll prefer to use that
            else if (touched.getValueCount() > 0) {
                Collection<Integer> most_touched = touched.getMaxCountValues();
                assert(most_touched != null) :
                    "Failed to get most touched partition for " + orig_ts + "\n" + touched;
                if (debug.val)
                    LOG.debug(String.format("Most touched partitions for mispredicted %s: %s",
                              orig_ts, most_touched));
                if (most_touched.contains(base_partition)) {
                    redirect_partition = base_partition;
                } else {
                    redirect_partition = CollectionUtil.random(most_touched);
                }
            }
            else {
                redirect_partition = base_partition;
            }
            assert(redirect_partition != HStoreConstants.NULL_PARTITION_ID) :
                "Redirect partition is null!\n" + orig_ts.debug();
            if (debug.val) {
                LOG.debug("Redirect Partition: " + redirect_partition + " -> " + (this.isLocalPartition(redirect_partition) == false));
                LOG.debug("Local Partitions: " + this.local_partitions);
            }
            
            // If the txn wants to execute on another node, then we'll send them off *only* if this txn wasn't
            // already redirected at least once. If this txn was already redirected, then it's going to just
            // execute on the same partition, but this time as a multi-partition txn that locks all partitions.
            // That's what you get for messing up!!
            if (this.isLocalPartition(redirect_partition) == false && orig_ts.getRestartCounter() == 0) {
                if (debug.val)
                    LOG.debug(String.format("%s - Redirecting to partition %d because of misprediction",
                              orig_ts, redirect_partition));
                
                Procedure catalog_proc = orig_ts.getProcedure();
                StoredProcedureInvocation spi = new StoredProcedureInvocation(orig_ts.getClientHandle(),
                                                                              catalog_proc.getId(),
                                                                              catalog_proc.getName(),
                                                                              orig_ts.getProcedureParameters().toArray());
                spi.setBasePartition(redirect_partition);
                spi.setRestartCounter(orig_ts.getRestartCounter()+1);
                
                FastSerializer out = this.outgoingSerializers.get();
                try {
                    out.writeObject(spi);
                } catch (IOException ex) {
                    String msg = "Failed to serialize StoredProcedureInvocation to redirect txn";
                    throw new ServerFaultException(msg, ex, orig_ts.getTransactionId());
                }
                
                RedirectCallback callback;
                try {
                    // callback = (RedirectCallback)objectPools.CALLBACKS_TXN_REDIRECT_REQUEST.borrowObject();
                    callback = new RedirectCallback(this);
                    callback.init(orig_ts.getClientCallback());
                } catch (Exception ex) {
                    String msg = "Failed to get TransactionRedirectCallback";
                    throw new ServerFaultException(msg, ex, orig_ts.getTransactionId());   
                }
                this.hstore_coordinator.transactionRedirect(out.getBytes(),
                                                            callback,
                                                            redirect_partition);
                out.clear();
                if (hstore_conf.site.txn_counters) TransactionCounter.REDIRECTED.inc(orig_ts.getProcedure());
                return (Status.ABORT_RESTART);
                
            // Allow local redirect
            } else if (orig_ts.getRestartCounter() <= 1) {
                if (redirect_partition != base_partition &&
                    this.isLocalPartition(redirect_partition)) {
                    if (debug.val)
                        LOG.debug(String.format("%s - Redirecting to local partition %d [restartCtr=%d]%s",
                                  orig_ts, redirect_partition, orig_ts.getRestartCounter(),
                                  (trace.val ? "\n"+touched : "")));
                    base_partition = redirect_partition;
                }
            } else {
                if (debug.val)
                    LOG.debug(String.format("%s - Mispredicted txn has already been aborted once before. " +
                              "Restarting as all-partition txn [restartCtr=%d, redirectPartition=%d]\n%s",
                              orig_ts, orig_ts.getRestartCounter(), redirect_partition, touched));
                touched.put(this.local_partitions);
            }
        }

        // -------------------------------
        // LOCAL RE-EXECUTION
        // -------------------------------
        
        // Figure out what partitions they tried to touch so that we can make sure to lock
        // those when the txn is restarted
        boolean malloc = false;
        PartitionSet predict_touchedPartitions = null;
        if (status == Status.ABORT_RESTART ||
            status == Status.ABORT_EVICTEDACCESS ||
            status == Status.ABORT_SPECULATIVE) {
            
            predict_touchedPartitions = new PartitionSet(orig_ts.getPredictTouchedPartitions());
            malloc = true;
        }
        else if (orig_ts.getRestartCounter() <= 2) { // FIXME
            // HACK: Ignore ConcurrentModificationException
            // This can occur if we are trying to requeue the transactions but there are still
            // pieces of it floating around at this site that modify the TouchedPartitions histogram
            predict_touchedPartitions = new PartitionSet();
            malloc = true;
            Collection<Integer> orig_touchedPartitions = orig_ts.getTouchedPartitions().values();
            while (true) {
                try {
                    predict_touchedPartitions.addAll(orig_touchedPartitions);
                } catch (ConcurrentModificationException ex) {
                    continue;
                }
                break;
            } // WHILE
        } else {
            if (debug.val)
                LOG.warn(String.format("Restarting %s as a dtxn using all partitions\n%s", orig_ts, orig_ts.debug()));
            predict_touchedPartitions = this.catalogContext.getAllPartitionIds();
        }
        
        // -------------------------------
        // MISPREDICTION
        // -------------------------------
        if (status == Status.ABORT_MISPREDICT && orig_error instanceof MispredictionException) {
            MispredictionException ex = (MispredictionException)orig_error;
            Collection<Integer> partitions = ex.getPartitions().values();
            assert(partitions.isEmpty() == false) :
                "Unexpected empty MispredictionException PartitionSet for " + orig_ts;

            if (predict_touchedPartitions.containsAll(partitions) == false) {
                if (malloc == false) {
                    // XXX: Since the MispredictionException isn't re-used, we can 
                    //      probably reuse the PartitionSet 
                    predict_touchedPartitions = new PartitionSet(predict_touchedPartitions);
                    malloc = true;
                }
                predict_touchedPartitions.addAll(partitions);
            }
            if (trace.val)
                LOG.trace(orig_ts + " Mispredicted Partitions: " + partitions);
        }
        
        if (predict_touchedPartitions.contains(base_partition) == false) {
            if (malloc == false) {
                predict_touchedPartitions = new PartitionSet(predict_touchedPartitions);
                malloc = true;
            }
            predict_touchedPartitions.add(base_partition);
        }
        if (predict_touchedPartitions.isEmpty()) {
            if (debug.val)
                LOG.warn(String.format("Restarting %s as a dtxn using all partitions\n%s",
                         orig_ts, orig_ts.debug()));
            predict_touchedPartitions = this.catalogContext.getAllPartitionIds();
        }
        
        // -------------------------------
        // NEW TXN INITIALIZATION
        // -------------------------------
        boolean predict_readOnly = orig_ts.getProcedure().getReadonly(); // FIXME
        boolean predict_abortable = true; // FIXME
        
        LocalTransaction new_ts = this.txnInitializer.createLocalTransaction(
                orig_ts,
                base_partition,
                predict_touchedPartitions,
                predict_readOnly,
                predict_abortable);
        assert(new_ts != null);

        // -------------------------------
        // ANTI-CACHING REQUEUE
        // -------------------------------
        if (status == Status.ABORT_EVICTEDACCESS && orig_error instanceof EvictedTupleAccessException) {
            if (this.anticacheManager == null) {
                String message = "Got eviction notice but anti-caching is not enabled";
                LOG.warn(message); 
                throw new ServerFaultException(message, orig_error, orig_ts.getTransactionId());
            }
            
            EvictedTupleAccessException error = (EvictedTupleAccessException)orig_error;
            int block_ids[] = error.getBlockIds();
            int tuple_offsets[] = error.getTupleOffsets();

            Table evicted_table = error.getTable(this.catalogContext.database);
            new_ts.setPendingError(error, false);

            if (debug.val)
                LOG.debug(String.format("Added aborted txn to %s queue. Unevicting %d blocks from %s (%d).",
                          AntiCacheManager.class.getSimpleName(), block_ids.length, evicted_table.getName(), evicted_table.getRelativeIndex()));
            
            if (orig_ts.getBasePartition() != error.getPartitionId() && !this.isLocalPartition(error.getPartitionId())) {
                new_ts.setOldTransactionId(orig_ts.getTransactionId());
            }
            this.anticacheManager.queue(new_ts, error.getPartitionId(), evicted_table, block_ids, tuple_offsets);
            
            
        }
            
        // -------------------------------
        // REGULAR TXN REQUEUE
        // -------------------------------
        else {
            if (debug.val) {
                LOG.debug(String.format("Re-executing %s as new %s-partition %s on partition %d " +
                          "[restarts=%d, partitions=%s]%s",
                          orig_ts,
                          (predict_touchedPartitions.size() == 1 ? "single" : "multi"),
                          new_ts,
                          base_partition,
                          new_ts.getRestartCounter(),
                          predict_touchedPartitions,
                          (trace.val ? "\n"+orig_ts.debug() : "")));
                if (trace.val && status == Status.ABORT_MISPREDICT)
                    LOG.trace(String.format("%s Mispredicted partitions: %s",
                              new_ts, orig_ts.getTouchedPartitions().values()));
            }
            
            this.transactionQueue(new_ts);    
        }
        
        return (Status.ABORT_RESTART);
    }

    // ----------------------------------------------------------------------------
    // CLIENT RESPONSE PROCESSING METHODS
    // ----------------------------------------------------------------------------

    /**
     * Send back the given ClientResponse to the actual client waiting for it
     * At this point the transaction should been properly committed or aborted at
     * the PartitionExecutor, including if it was mispredicted.
     * This method may not actually send the ClientResponse right away if command-logging
     * is enabled. Instead it will be queued up and held until we know that the txn's information
     * was successfully flushed to disk.
     * 
     * <B>Note:</B> The ClientResponse's status cannot be ABORT_MISPREDICT or ABORT_EVICTEDACCESS.
     * @param ts
     * @param cresponse
     */
    public void responseSend(LocalTransaction ts, ClientResponseImpl cresponse) {
        Status status = cresponse.getStatus();
        assert(cresponse != null) :
            "Missing ClientResponse for " + ts;
        assert(cresponse.getClientHandle() != -1) :
            "The client handle for " + ts + " was not set properly";
        assert(status != Status.ABORT_MISPREDICT && status != Status.ABORT_EVICTEDACCESS) :
            "Trying to send back a client response for " + ts + " but the status is " + status;
        
        if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startPostClient();
        boolean sendResponse = true;
        
        // We have to send this txn to the CommandLog if all of the following are true:
        //  (1) We have a CommandLogWriter
        //  (2) The txn completed successfully
        //  (3) It is not a sysproc
        LOG.trace("Command logger :"+this.commandLogger);
        LOG.trace("Status :"+status);
        LOG.trace("Is SysProc :"+ts.isSysProc());
        
        if (this.commandLogger != null && status == Status.OK && ts.isSysProc() == false) {
            sendResponse = this.commandLogger.appendToLog(ts, cresponse);
        }

        if (sendResponse) {
            // NO GROUP COMMIT -- SEND OUT AND COMPLETE
            // NO COMMAND LOGGING OR TXN ABORTED -- SEND OUT AND COMPLETE
            if (hstore_conf.site.exec_postprocessing_threads) {
                if (trace.val)
                    LOG.trace(String.format("%s - Sending ClientResponse to post-processing thread [status=%s]",
                              ts, cresponse.getStatus()));
                this.responseQueue(ts, cresponse);
            } else {
                this.responseSend(cresponse,
                                  ts.getClientCallback(),
                                  ts.getInitiateTime(),
                                  ts.getRestartCounter());
            }
        } else if (debug.val) { 
            LOG.debug(String.format("%s - Holding the ClientResponse until logged to disk", ts));
        }
        if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.stopPostClient();
    }

    /**
     * Instead of having the PartitionExecutor send the ClientResponse directly back
     * to the client, this method will queue it up at one of the TransactionPostProcessors.
     * @param ts
     * @param cresponse
     */
    private void responseQueue(LocalTransaction ts, ClientResponseImpl cresponse) {
        assert(hstore_conf.site.exec_postprocessing_threads);
        if (debug.val)
            LOG.debug(String.format("Adding ClientResponse for %s from partition %d " +
                      "to processing queue [status=%s, size=%d]",
                      ts, ts.getBasePartition(), cresponse.getStatus(), this.postProcessorQueue.size()));
        this.postProcessorQueue.add(new Object[]{
                                            cresponse,
                                            ts.getClientCallback(),
                                            ts.getInitiateTime(),
                                            ts.getRestartCounter()
        });
    }

    /**
     * Use the TransactionPostProcessors to dispatch the ClientResponse back over the network
     * @param cresponse
     * @param clientCallback
     * @param initiateTime
     * @param restartCounter
     */
    public void responseQueue(ClientResponseImpl cresponse,
                              RpcCallback<ClientResponseImpl> clientCallback,
                              long initiateTime,
                              int restartCounter) {
        this.postProcessorQueue.add(new Object[]{
                                            cresponse,
                                            clientCallback,
                                            initiateTime,
                                            restartCounter
        });
    }

    /**
     * Convenience method for sending an error ClientResponse back to the client
     * @param client_handle
     * @param status
     * @param message
     * @param clientCallback
     * @param initiateTime
     */
    public void responseError(long client_handle,
                              Status status,
                              String message,
                              RpcCallback<ClientResponseImpl> clientCallback,
                              long initiateTime) {
        ClientResponseImpl cresponse = new ClientResponseImpl(
                                            -1,
                                            client_handle,
                                            -1,
                                            status,
                                            HStoreConstants.EMPTY_RESULT,
                                            message);
        this.responseSend(cresponse, clientCallback, initiateTime, 0);
    }
    
    /**
     * This is the only place that we will invoke the original Client callback
     * and send back the results. This should not be called directly by anything
     * but the HStoreSite or the CommandLogWriter
     * @param ts
     * @param cresponse
     * @param logTxn
     */
    public void responseSend(ClientResponseImpl cresponse,
                             RpcCallback<ClientResponseImpl> clientCallback,
                             long initiateTime,
                             int restartCounter) {
        Status status = cresponse.getStatus();
 
        // If the txn committed/aborted, then we can send the response directly back to the
        // client here. Note that we don't even need to call HStoreSite.finishTransaction()
        // since that doesn't do anything that we haven't already done!
        if (debug.val) {
            String extra = "";
            if (status == Status.ABORT_UNEXPECTED && cresponse.getException() != null) {
                extra = "\n" + StringUtil.join("\n", cresponse.getException().getStackTrace());
            }
            if (trace.val && status == Status.OK && cresponse.getResults().length > 0) {
                extra += "\n" + cresponse.getResults()[0];
            }
            LOG.debug(String.format("Txn %s - Sending back ClientResponse [handle=%d, status=%s]%s",
                      (cresponse.getTransactionId() == -1 ? "<NONE>" : "#"+cresponse.getTransactionId()),
                      cresponse.getClientHandle(), status, extra));
        }
        
        long now = -1;
        if (hstore_conf.global.nanosecond_latencies) {
            now = System.nanoTime();
        } else {
            now = System.currentTimeMillis();
            EstTimeUpdater.update(now);
        }
        cresponse.setClusterRoundtrip((int)(now - initiateTime));
        cresponse.setRestartCounter(restartCounter);
        try {
            clientCallback.run(cresponse);
        } catch (ClientConnectionLostException ex) {
            // There is nothing else we can really do here. We'll clean up
            // the transaction just as normal and report the error
            // in our logs if they have debugging turned on
            if (trace.val)
                LOG.warn("Failed to send back ClientResponse for txn #" + cresponse.getTransactionId(), ex);
        }
    }
    
    // ----------------------------------------------------------------------------
    // DELETE TRANSACTION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Queue a completed txn for final cleanup and bookkeeping. This will be deleted
     * by the HStoreSite's periodic work thread. It is ok to queue up the same txn twice
     * <B>Note:</B> If you call this, you can never access anything in this txn again.
     * @param txn_id
     * @param status The final status for the txn
     */
    public void queueDeleteTransaction(Long txn_id, Status status) {
        assert(txn_id != null) : "Unexpected null transaction id";
        if (debug.val)
            LOG.debug(String.format("Queueing txn #%d for deletion [status=%s]", txn_id, status));
        
        // Queue it up for deletion! There is no return for the txn from this!
        try {
            this.deletable_txns.get(status).offer(txn_id);
        } catch (NullPointerException ex) {
            LOG.warn("STATUS = " + status);
            LOG.warn("TXN_ID = " + txn_id);
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Clean-up all of the state information about a RemoteTransaction that is finished
     * <B>NOTE:</B> You should not be calling this directly. Use queueDeleteTransaction() instead!
     * @param ts
     * @param status
     */
    protected void deleteRemoteTransaction(RemoteTransaction ts, Status status) {
        // Nothing else to do for RemoteTransactions other than to just
        // return the object back into the pool
        final Long txn_id = ts.getTransactionId();
        AbstractTransaction rm = this.inflight_txns.remove(txn_id);
        if (debug.val) LOG.debug(String.format("Deleted %s [%s / inflightRemoval:%s]", ts, status, (rm != null)));
        
        EstimatorState t_state = ts.getEstimatorState(); 
        if (t_state != null) {
            this.remoteTxnEstimator.destroyEstimatorState(t_state);
        }
        
        if (debug.val) {
            LOG.warn(String.format("%s - Finished with %s [hashCode=%d]",
                     ts, ts.getClass().getSimpleName(), ts.hashCode()));
            this.deletable_last.add(String.format("%s :: %s", ts, status));
        }
        return;
    }

    /**
     * Clean-up all of the state information about a LocalTransaction that is finished
     * <B>NOTE:</B> You should not be calling this directly. Use queueDeleteTransaction() instead!
     * @param ts
     * @param status
     */
    protected void deleteLocalTransaction(LocalTransaction ts, final Status status) {
        final Long txn_id = ts.getTransactionId();
        final int base_partition = ts.getBasePartition();
        final Procedure catalog_proc = ts.getProcedure();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        if (debug.val) {
            LOG.debug(String.format("About to delete %s [%s]", ts, status));
            if (trace.val) LOG.trace(ts + " - State before delete:\n" + ts.debug());
        }
        
        assert(ts.checkDeletableFlag()) :
            String.format("Trying to delete %s before it was marked as ready!", ts);
        
        // Clean-up any extra information that we may have for the txn
        TransactionEstimator t_estimator = null;
        EstimatorState t_state = ts.getEstimatorState(); 
        if (t_state != null) {
            t_estimator = this.executors[base_partition].getTransactionEstimator();
            assert(t_estimator != null);
        }
        if (ts.hasDependencyTracker()) {
            // HACK: Check whether there were unnecessary prefetch queries
            if (hstore_conf.site.txn_profiling && ts.profiler != null) {
                Integer cnt = this.depTrackers[base_partition].getDebugContext().getUnusedPrefetchResultCount(ts);
                if (cnt != null) ts.profiler.addPrefetchUnusedQuery(cnt.intValue());
            }
            this.depTrackers[base_partition].removeTransaction(ts);
        }
        
        // Update Transaction profiler
        // XXX: Should we include totals for mispredicted txns?
        if (hstore_conf.site.txn_profiling &&
                ts.profiler != null &&
                ts.profiler.isDisabled() == false &&
                status != Status.ABORT_MISPREDICT) {
            ts.profiler.stopTransaction();
            if (this.txnProfilerStats != null) {
                this.txnProfilerStats.addTxnProfile(ts.getProcedure(), ts.profiler);
            }
            if (this.status_monitor != null) {
                this.status_monitor.addTxnProfile(ts.getProcedure(), ts.profiler);
            }
        }
        
        try {
            switch (status) {
                case OK:
                    if (t_estimator != null) {
                        if (trace.val)
                            LOG.trace(String.format("Telling the %s to COMMIT %s",
                                      t_estimator.getClass().getSimpleName(), ts));
                        t_estimator.commit(t_state);
                    }
                    // We always need to keep track of how many txns we process 
                    // in order to check whether we are hung or not
                    if (hstore_conf.site.txn_counters || hstore_conf.site.status_kill_if_hung) {
                        TransactionCounter.COMPLETED.inc(catalog_proc);
                    }
                    break;
                case ABORT_USER:
                    if (t_estimator != null) {
                        if (trace.val) LOG.trace("Telling the TransactionEstimator to ABORT " + ts);
                        t_estimator.abort(t_state, status);
                    }
                    if (hstore_conf.site.txn_counters)
                        TransactionCounter.ABORTED.inc(catalog_proc);
                    break;
                case ABORT_MISPREDICT:
                case ABORT_RESTART:
                case ABORT_EVICTEDACCESS:
                case ABORT_SPECULATIVE:
                    if (t_estimator != null) {
                        if (trace.val) LOG.trace("Telling the TransactionEstimator to IGNORE " + ts);
                        t_estimator.abort(t_state, status);
                    }
                    if (hstore_conf.site.txn_counters) {
                        if (status == Status.ABORT_EVICTEDACCESS) {
                //if(ts.getRestartCounter()==0){
                                TransactionCounter.EVICTEDACCESS.inc(catalog_proc);
                //}
                        }
                        else if (status == Status.ABORT_SPECULATIVE) {
                            TransactionCounter.ABORT_SPECULATIVE.inc(catalog_proc);
                        }
                        else if (status == Status.ABORT_MISPREDICT) {
                            TransactionCounter.MISPREDICTED.inc(catalog_proc);
                        }
                        // Don't count restarted txns more than once
                        else if (ts.getRestartCounter() == 0) {
                            TransactionCounter.RESTARTED.inc(catalog_proc);
                        }
                    }
                    break;
                case ABORT_REJECT:
                    if (hstore_conf.site.txn_counters)
                        TransactionCounter.REJECTED.inc(catalog_proc);
                    break;
                case ABORT_UNEXPECTED:
                    if (hstore_conf.site.txn_counters)
                        TransactionCounter.ABORT_UNEXPECTED.inc(catalog_proc);
                    break;
                case ABORT_GRACEFUL:
                    if (hstore_conf.site.txn_counters)
                        TransactionCounter.ABORT_GRACEFUL.inc(catalog_proc);
                    break;
                default:
                    LOG.warn(String.format("Unexpected status %s for %s", status, ts));
            } // SWITCH
        } catch (Throwable ex) {
            LOG.error(String.format("Unexpected error when cleaning up %s transaction %s", status, ts), ex);
            // Pass...
        } finally {
            if (t_state != null && t_estimator != null) {
                assert(txn_id == t_state.getTransactionId()) :
                    String.format("Unexpected mismatch txnId in %s [%d != %d]",
                                  t_state.getClass().getSimpleName(),
                                  txn_id, t_state.getTransactionId());
                t_estimator.destroyEstimatorState(t_state);
            }
        }
        
        if (hstore_conf.site.txn_profiling && hstore_conf.site.txn_profiling_dump &&
            this.txn_profiler_dumper != null && ts.profiler != null) { //  && ts.profiler.isDisabled() == false) {
            this.txn_profiler_dumper.writeRow(ts);
        }
        
        // Update additional transaction profiling counters
        if (hstore_conf.site.txn_counters) {
            // Speculative Execution Counters
            if (ts.isSpeculative() && status != Status.ABORT_SPECULATIVE) {
                TransactionCounter.SPECULATIVE.inc(catalog_proc);
                switch (ts.getSpeculationType()) {
                    case SP1_IDLE:
                        TransactionCounter.SPECULATIVE_SP1_IDLE.inc(catalog_proc);
                        break;
                    case SP1_LOCAL:
                        TransactionCounter.SPECULATIVE_SP1_LOCAL.inc(catalog_proc);
                        break;
                    case SP2_LOCAL:
                        TransactionCounter.SPECULATIVE_SP2.inc(catalog_proc);
                        break;
                    case SP3_REMOTE_BEFORE:
                        TransactionCounter.SPECULATIVE_SP3_BEFORE.inc(catalog_proc);
                        break;
                    case SP3_REMOTE_AFTER:
                        TransactionCounter.SPECULATIVE_SP3_AFTER.inc(catalog_proc);
                        break;
                    case SP4_LOCAL:
                        TransactionCounter.SPECULATIVE_SP4_LOCAL.inc(catalog_proc);
                        break;
                    case SP4_REMOTE:
                        TransactionCounter.SPECULATIVE_SP4_REMOTE.inc(catalog_proc);
                        break;
                    default:
                        throw new RuntimeException("Unexpected " + ts.getSpeculationType());
                } // SWITCH
            }
            
            if (ts.isSysProc()) {
                TransactionCounter.SYSPROCS.inc(catalog_proc);
            } else if (status != Status.ABORT_MISPREDICT &&
                       status != Status.ABORT_REJECT &&
                       status != Status.ABORT_EVICTEDACCESS &&
                       status != Status.ABORT_SPECULATIVE) {
                (singlePartitioned ? TransactionCounter.SINGLE_PARTITION : TransactionCounter.MULTI_PARTITION).inc(catalog_proc);
                
                // Check for the number of multi-site txns
                if (singlePartitioned == false) {
                    int baseSite = catalogContext.getSiteIdForPartitionId(base_partition);
                    for (int partition : ts.getPredictTouchedPartitions().values()) {
                        int site = catalogContext.getSiteIdForPartitionId(partition);
                        if (site != baseSite) {
                            TransactionCounter.MULTI_SITE.inc(catalog_proc);
                            break;
                        }
                    } // FOR
                }
                
                // Only count no-undo buffers for completed transactions
                if (ts.isExecNoUndoBuffer(base_partition)) TransactionCounter.NO_UNDO.inc(catalog_proc);
            }
        }
        
        // SANITY CHECK
        if (hstore_conf.site.exec_validate_work) {
            for (int p : this.local_partitions.values()) {
                assert(ts.equals(this.executors[p].getDebugContext().getCurrentDtxn()) == false) :
                    String.format("About to finish %s but it is still the current DTXN at partition %d", ts, p);
            } // FOR
        }

        AbstractTransaction rm = this.inflight_txns.remove(txn_id);
        assert(rm == null || rm == ts) : String.format("%s != %s", ts, rm);
        if (trace.val)
            LOG.trace(String.format("Deleted %s [%s / inflightRemoval:%s]", ts, status, (rm != null)));
        
        assert(ts.isInitialized()) : "Trying to return uninitialized txn #" + txn_id;
        if (debug.val) {
            LOG.warn(String.format("%s - Finished with %s [hashCode=%d]",
                     ts, ts.getClass().getSimpleName(), ts.hashCode()));
            this.deletable_last.add(String.format("%s :: %s [SPECULATIVE=%s]",
                                    ts, status, ts.isSpeculative()));
        }
    }

    // ----------------------------------------------------------------------------
    // UTILITY WORK
    // ----------------------------------------------------------------------------
    
    /**
     * Added for @AdHoc processes, periodically checks for AdHoc queries waiting to be compiled.
     * 
     */
    private void processPeriodicWork() {
        // if (trace.val) LOG.trace("Checking for PeriodicWork...");

        // We want to do this here just so that the time is always moving forward.
        EstTimeUpdater.update(System.currentTimeMillis());
        
        if (this.clientInterface != null) {
            this.clientInterface.checkForDeadConnections(EstTime.currentTimeMillis());
        }
        
        // poll planner queue
        if (this.asyncCompilerWorkThread != null) {
            this.checkForFinishedCompilerWork();
            this.asyncCompilerWorkThread.verifyEverthingIsKosher();
        }
        
        // Don't delete anything if we're shutting down
        // This is so that we can see the state of things right before we stopped
        if (this.isShuttingDown()) {
            if (trace.val) LOG.warn(this.getSiteName() + " is shutting down. Suspending transaction handle cleanup");
            return;
        }
        
        return;
    }

    /**
     * Added for @AdHoc processes
     * 
     */
    private void checkForFinishedCompilerWork() {
        if (trace.val) LOG.trace("Checking for finished compiled work.");
        AsyncCompilerResult result = null;
 
        while ((result = this.asyncCompilerWorkThread.getPlannedStmt()) != null) {
            if (trace.val) LOG.trace("AsyncCompilerResult\n" + result);
            
            // ----------------------------------
            // BUSTED!
            // ----------------------------------
            if (result.errorMsg != null) {
                if (debug.val)
                    LOG.error(String.format("Unexpected %s Error for clientHandle #%d: %s",
                              this.asyncCompilerWorkThread.getClass().getSimpleName(),
                              result.clientHandle, result.errorMsg));
                
                ClientResponseImpl errorResponse =
                        new ClientResponseImpl(-1,
                                               result.clientHandle,
                                               this.local_partitions.get(),
                                               Status.ABORT_UNEXPECTED,
                                               HStoreConstants.EMPTY_RESULT,
                                               result.errorMsg);
                this.responseSend(result.ts, errorResponse);
                
                // We can just delete the LocalTransaction handle directly
                result.ts.getInitCallback().cancel();
                boolean deletable = result.ts.isDeletable();
                if (deletable == false) {
                    LOG.warn(result.ts + " is not deletable?\n" + result.ts.debug());
                }
                assert(deletable);
                this.deleteLocalTransaction(result.ts, Status.ABORT_UNEXPECTED);
            }
            // ----------------------------------
            // AdHocPlannedStmt
            // ----------------------------------
            else if (result instanceof AdHocPlannedStmt) {
                AdHocPlannedStmt plannedStmt = (AdHocPlannedStmt) result;

                // Modify the StoredProcedureInvocation
                ParameterSet params = result.ts.getProcedureParameters();
                assert(params != null) : "Unexpected null ParameterSet";
                params.setParameters(
                    plannedStmt.aggregatorFragment,
                    plannedStmt.collectorFragment,
                    plannedStmt.sql,
                    plannedStmt.isReplicatedTableDML ? 1 : 0
                );

                // initiate the transaction
                int base_partition = result.ts.getBasePartition();
                Long txn_id = this.txnInitializer.registerTransaction(result.ts, base_partition);
                result.ts.setTransactionId(txn_id);
                
                if (debug.val) LOG.debug("Queuing AdHoc transaction: " + result.ts);
                this.transactionQueue(result.ts);
                
            }
            // ----------------------------------
            // Unexpected
            // ----------------------------------
            else {
                throw new RuntimeException(
                        "Should not be able to get here (HStoreSite.checkForFinishedCompilerWork())");
            }
        } // WHILE
    }
        
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    public class Debug implements DebugContext {
        /**
         * Get the total number of transactions inflight for all partitions 
         */
        public int getInflightTxnCount() {
            return (inflight_txns.size());
        }
        public int getDeletableTxnCount() {
            int total = 0;
            for (Queue<Long> q : deletable_txns.values()) {
                total += q.size();
            }
            return (total);
        }
        public Collection<String> getLastDeletedTxns() {
            return (deletable_last);
        }
        public void resetStartWorkload() {
            synchronized (HStoreSite.this) {
                HStoreSite.this.startWorkload = false;
            } // SYNCH
        }
        
        /**
         * Get the collection of inflight Transaction state handles
         * THIS SHOULD ONLY BE USED FOR TESTING!
         * @return
         */
        public Collection<AbstractTransaction> getInflightTransactions() {
            return (inflight_txns.values());
        }
        
        public int getQueuedResponseCount() {
            return (postProcessorQueue.size());
        }
        
        public HStoreSiteProfiler getProfiler() {
            return (profiler);
        }
    }
    
    private HStoreSite.Debug cachedDebugContext;
    public HStoreSite.Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new HStoreSite.Debug(); 
        }
        return this.cachedDebugContext;
    }

}
