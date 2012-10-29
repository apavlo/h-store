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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.MemoryStats;
import org.voltdb.ParameterSet;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSource;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SysProcSelector;
import org.voltdb.TransactionIdManager;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
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
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.EstTimeUpdater;
import org.voltdb.utils.Pair;
import org.voltdb.utils.SystemStatsCollector;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.ClientInterface.ClientInputHandler;
import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.ClientResponseCallback;
import edu.brown.hstore.callbacks.TransactionCleanupCallback;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionRedirectCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.remote.RemoteEstimator;
import edu.brown.hstore.estimators.remote.RemoteEstimatorState;
import edu.brown.hstore.stats.SiteProfilerStats;
import edu.brown.hstore.stats.MarkovEstimatorProfilerStats;
import edu.brown.hstore.stats.PartitionExecutorProfilerStats;
import edu.brown.hstore.stats.PoolCounterStats;
import edu.brown.hstore.stats.SpecExecProfilerStats;
import edu.brown.hstore.stats.TransactionCounterStats;
import edu.brown.hstore.stats.TransactionProfilerStats;
import edu.brown.hstore.stats.TransactionQueueManagerProfilerStats;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.hstore.util.MapReduceHelperThread;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.hstore.wal.CommandLogWriter;
import edu.brown.interfaces.Configurable;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Loggable;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.logging.RingBufferAppender;
import edu.brown.markov.EstimationThresholds;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.profilers.HStoreSiteProfiler;
import edu.brown.profilers.PartitionExecutorProfiler;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * THE ALL POWERFUL H-STORE SITE!
 * This is the central hub for a site and all of its partitions
 * All incoming transactions come into this and all transactions leave through this
 * @author pavlo
 */
public class HStoreSite implements VoltProcedureListener.Handler, Shutdownable, Loggable, Configurable, Runnable {
    public static final Logger LOG = Logger.getLogger(HStoreSite.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private static boolean d;
    private static boolean t;
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
        d = debug.get();
        t = trace.get();
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
    private final IdentityHashMap<Thread, FastDeserializer> incomingDeserializers =
                        new IdentityHashMap<Thread, FastDeserializer>();
    
    /**
     * Outgoing response serializers
     */
    private final IdentityHashMap<Thread, FastSerializer> outgoingSerializers = 
                        new IdentityHashMap<Thread, FastSerializer>();
    
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
     * 
     */
    private final Queue<Long> deletable_txns[];
    
    private final List<Long> deletable_txns_requeue = new ArrayList<Long>();
    
    /**
     * Reusable Object Pools
     */
    private final HStoreObjectPools objectPools;
    
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
    
    // ----------------------------------------------------------------------------
    // NETWORKING STUFF
    // ----------------------------------------------------------------------------
    
    /**
     * This thread is responsible for listening for incoming txn requests from 
     * clients. It will then forward the request to HStoreSite.procedureInvocation()
     */
//    private VoltProcedureListener voltListeners[];
//    private final NIOEventLoop procEventLoops[];
    
    private VoltNetwork voltNetwork;
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
    private final List<TransactionPreProcessor> preProcessors;
    private final BlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> preProcessorQueue;
    
    /**
     * TransactionPostProcessor Thread
     * These threads allow a PartitionExecutor to send back ClientResponses back to
     * the clients without blocking
     */
    private final List<TransactionPostProcessor> postProcessors;
    private final BlockingQueue<Pair<LocalTransaction, ClientResponseImpl>> postProcessorQueue;
    
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
    private final AsyncCompilerWorkThread asyncCompilerWork_thread;
    
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
    private final EventObservable<Object> shutdown_observable = new EventObservable<Object>();
    
    // ----------------------------------------------------------------------------
    // PARTITION SPECIFIC MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Collection of local partitions managed at this HStoreSite
     */
    private final PartitionSet local_partitions = new PartitionSet();
    
    /**
     * Integer list of all local partitions managed at this HStoreSite
     */
    protected final Integer local_partitions_arr[];
    
    /**
     * PartitionId -> Internal Offset
     * This is so that we don't have to keep long arrays of local partition information
     */
    private final int local_partition_offsets[];
    
    /**
     * For a given offset from LOCAL_PARTITION_OFFSETS, this array
     * will contain the partition id
     */
    private final int local_partition_reverse[];
    
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
    private HStoreSiteStatus status_monitor = null;
    
    private HStoreSiteProfiler profiler = new HStoreSiteProfiler();
    
    // ----------------------------------------------------------------------------
    // CACHED STRINGS
    // ----------------------------------------------------------------------------
    
    private final String REJECTION_MESSAGE;
    
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
        
         this.deletable_txns = new Queue[Status.values().length];
         for (Status s : Status.values()) {
             this.deletable_txns[s.ordinal()] = new ConcurrentLinkedQueue<Long>();
         }
//        this.deletable_txns = new Queue[num_local_partitions];
//        for (int i = 0; i < this.deletable_txns.length; i++) {
//            this.deletable_txns[i] = new ConcurrentLinkedQueue<Pair<Long, Status>>();
//        }
        
        // **IMPORTANT**
        // We have to setup the partition offsets before we do anything else here
        this.local_partitions_arr = new Integer[num_local_partitions];
        this.executors = new PartitionExecutor[num_partitions];
        this.executor_threads = new Thread[num_partitions];
        
        // Get the hasher we will use for this HStoreSite
        this.hasher = ClassUtil.newInstance(hstore_conf.global.hasherClass,
                                             new Object[]{ this.catalogContext.database, num_partitions },
                                             new Class<?>[]{ Database.class, int.class });
        this.p_estimator = new PartitionEstimator(this.catalogContext, this.hasher);
        this.remoteTxnEstimator = new RemoteEstimator(this.p_estimator);

        // **IMPORTANT**
        // Always clear out the CatalogUtil and BatchPlanner before we start our new HStoreSite
        // TODO: Move this cache information into CatalogContext
        CatalogUtil.clearCache(this.catalogContext.database);
        BatchPlanner.clear(this.catalogContext.numberOfPartitions);

        // Only preload stuff if we were asked to
        if (hstore_conf.site.preload) {
            if (d) LOG.debug("Preloading cached objects");
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
        this.local_partition_reverse = new int[num_local_partitions];
        int offset = 0;
        for (int partition : this.local_partitions) {
            this.local_partition_offsets[partition] = offset;
            this.local_partition_reverse[offset] = partition; 
            this.local_partitions_arr[offset] = partition;
            offset++;
        } // FOR
        
        // Object Pools
        this.objectPools = new HStoreObjectPools(this);
        
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
            File logFile = new File(hstore_conf.site.commandlog_dir +
                                    File.separator +
                                    this.getSiteName().toLowerCase() + ".log");
            this.commandLogger = new CommandLogWriter(this, logFile);
        } else {
            this.commandLogger = null;
        }

        // AdHoc Support
        if (hstore_conf.site.exec_adhoc_sql) {
            this.asyncCompilerWork_thread = new AsyncCompilerWorkThread(this, this.site_id);
        } else {
            this.asyncCompilerWork_thread = null;
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
        
        this.voltNetwork = new VoltNetwork();
        this.clientInterface = ClientInterface.create(this,
                                                      this.voltNetwork,
                                                      this.catalogContext,
                                                      this.getSiteId(),
                                                      this.getSiteId(),
                                                      catalog_site.getProc_port(),
                                                      null);
        
        
        // -------------------------------
        // TRANSACTION PROCESSING THREADS
        // -------------------------------

        List<TransactionPreProcessor> _preProcessors = null;
        List<TransactionPostProcessor> _postProcessors = null;
        BlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> _preQueue = null;
        BlockingQueue<Pair<LocalTransaction, ClientResponseImpl>> _postQueue = null;
        
        if (hstore_conf.site.exec_preprocessing_threads || hstore_conf.site.exec_postprocessing_threads) {
            // Transaction Pre/Post Processing Threads
            // We need at least one core per partition and one core for the VoltProcedureListener
            // Everything else we can give to the pre/post processing guys
            int num_available_cores = threadManager.getNumCores() - (num_local_partitions + 1);

            // If there are no available cores left, then we won't create any extra processors
            if (num_available_cores <= 0) {
                LOG.warn("Insufficient number of cores on " + catalog_host.getIpaddr() + ". " +
                         "Disabling transaction pre/post processing threads");
                hstore_conf.site.exec_preprocessing_threads = false;
                hstore_conf.site.exec_postprocessing_threads = false;
            } else {
                int num_preProcessors = 0;
                int num_postProcessors = 0;
                
                // Both Types of Processors
                if (hstore_conf.site.exec_preprocessing_threads && hstore_conf.site.exec_postprocessing_threads) {
                    int split = (int)Math.ceil(num_available_cores / 2d);
                    num_preProcessors = split;
                    num_postProcessors = split;
                }
                // TransactionPreProcessor Only
                else if (hstore_conf.site.exec_preprocessing_threads) {
                    num_preProcessors = num_available_cores;
                }
                // TransactionPostProcessor Only
                else {
                    num_postProcessors = num_available_cores;
                }
                
                // Overrides
                if (hstore_conf.site.exec_preprocessing_threads_count >= 0) {
                    num_preProcessors = hstore_conf.site.exec_preprocessing_threads_count;
                }
                if (hstore_conf.site.exec_postprocessing_threads_count >= 0) {
                    num_postProcessors = hstore_conf.site.exec_postprocessing_threads_count;
                }
                
                // Initialize TransactionPreProcessors
                if (num_preProcessors > 0) {
                    if (d) LOG.debug(String.format("Starting %d %s threads",
                                     num_preProcessors,
                                     TransactionPreProcessor.class.getSimpleName()));
                    _preProcessors = new ArrayList<TransactionPreProcessor>();
                    _preQueue = new LinkedBlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>>();
                    for (int i = 0; i < num_preProcessors; i++) {
                        TransactionPreProcessor t = new TransactionPreProcessor(this, _preQueue);
                        _preProcessors.add(t);
                    } // FOR
                }
                // Initialize TransactionPostProcessors
                if (num_postProcessors > 0) {
                    if (d) LOG.debug(String.format("Starting %d %s threads",
                                     num_postProcessors,
                                     TransactionPostProcessor.class.getSimpleName()));
                    _postProcessors = new ArrayList<TransactionPostProcessor>();
                    _postQueue = new LinkedBlockingQueue<Pair<LocalTransaction, ClientResponseImpl>>();
                    for (int i = 0; i < num_postProcessors; i++) {
                        TransactionPostProcessor t = new TransactionPostProcessor(this, _postQueue);
                        _postProcessors.add(t);
                    } // FOR
                }
            }
        }
        this.preProcessors = _preProcessors;
        this.preProcessorQueue = _preQueue;
        this.postProcessors = _postProcessors;
        this.postProcessorQueue = _postQueue;
        
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
        
        this.initStatSources();
        
        // Profiling
        if (hstore_conf.site.profiling) {
            this.profiler = new HStoreSiteProfiler();
            if (hstore_conf.site.status_show_executor_info) {
                this.profiler.network_idle.resetOnEventObservable(this.startWorkload_observable);
            }
        } else {
            this.profiler = null;
        }
        
        LoggerUtil.refreshLogging(hstore_conf.global.log_refresh);
    }
    
    // ----------------------------------------------------------------------------
    // INTERFACE METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    @Override
    public void updateConf(HStoreConf hstore_conf) {
        if (hstore_conf.site.profiling && this.profiler == null) {
            this.profiler = new HStoreSiteProfiler();
        }
        
        // Push the updates to all of our PartitionExecutors
        for (PartitionExecutor executor : this.executors) {
            if (executor == null) continue;
            executor.updateConf(hstore_conf);
        } // FOR
        
        // Update all our other boys
        this.objectPools.updateConf(hstore_conf);
        this.txnQueueManager.updateConf(hstore_conf);
    }
    
    // ----------------------------------------------------------------------------
    // ADDITIONAL INITIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    public void addPartitionExecutor(int partition, PartitionExecutor executor) {
        assert(this.shutdown_state != ShutdownState.STARTED);
        assert(executor != null);
        this.executors[partition] = executor;
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
        if (d) LOG.debug("Set new EstimationThresholds: " + thresholds);
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
     * Return an immutable array of the local partition ids managed by this HStoreSite
     * Use this array is prefable to the PartitionSet if you must iterate of over them.
     * This avoids having to create a new Iterator instance each time.
     * TODO: Moved to CatalogContext
     */
    public Integer[] getLocalPartitionIdArray() {
        return (this.local_partitions_arr);
    }
    /**
     * Returns true if the given partition id is managed by this HStoreSite
     * @param partition
     * @return
     */
    public boolean isLocalPartition(int partition) {
        return (this.local_partition_offsets[partition] != -1);
    }
    /**
     * Returns true if the given PartitoinSet contains partitions that are all
     * is managed by this HStoreSite
     * @param partitions
     * @return
     */
    public boolean isLocalPartitions(PartitionSet partitions) {
        for (Integer p : partitions) {
            if (this.local_partition_offsets[p.intValue()] == -1) {
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
        assert(this.mr_helper_started == false);
        if (d) LOG.debug("Starting " + this.mr_helper.getClass().getSimpleName());
        Thread t = new Thread(this.mr_helper);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        t.start();
        this.mr_helper_started = true;
    }
    
    /**
     * Start threads for processing AdHoc queries 
     */
    private void startAdHocHelper() {
        assert(this.adhoc_helper_started == false);
        if (d) LOG.debug("Starting " + this.asyncCompilerWork_thread.getClass().getSimpleName());
        this.asyncCompilerWork_thread.start();
        this.adhoc_helper_started = true;
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
        if (d) LOG.debug("Setting Cluster InstanceId: " + instanceId);
        this.instanceId = instanceId;
    }
    
    public HStoreCoordinator getCoordinator() {
        return (this.hstore_coordinator);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public HStoreObjectPools getObjectPools() {
        return (this.objectPools);
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
    protected final String getRejectionMessage() {
        return (this.REJECTION_MESSAGE);
    }
    
    /**
     * Convenience method to dump out status of this HStoreSite
     * @return
     */
    public String statusSnapshot() {
        return new HStoreSiteStatus(this, hstore_conf).snapshot(true, true, false, false);
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
    
    public Collection<TransactionPreProcessor> getTransactionPreProcessors() {
        return (this.preProcessors);
    }
    public Collection<TransactionPostProcessor> getTransactionPostProcessors() {
        return (this.postProcessors);
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
        return ((T)this.inflight_txns.get(txn_id));
    }

    
    /**
     * Return a thread-safe FastDeserializer
     * @return
     */
    private FastDeserializer getIncomingDeserializer() {
        Thread t = Thread.currentThread();
        FastDeserializer fds = this.incomingDeserializers.get(t);
        if (fds == null) {
            fds = new FastDeserializer(new byte[0]);
            this.incomingDeserializers.put(t, fds);
        }
        assert(fds != null);
        return (fds);
    }
    
    /**
     * Return a thread-safe FastSerializer
     * @return
     */
    private FastSerializer getOutgoingSerializer() {
        Thread t = Thread.currentThread();
        FastSerializer fs = this.outgoingSerializers.get(t);
        if (fs == null) {
            fs = new FastSerializer(this.buffer_pool);
            this.outgoingSerializers.put(t, fs);
        }
        assert(fs != null);
        return (fs);
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
    public int getLocalPartitionOffset(int partition) {
        assert(partition < this.local_partition_offsets.length) :
            String.format("Unable to get offset of local partition %d %s [hashCode=%d]",
                          partition, Arrays.toString(this.local_partition_offsets), this.hashCode());
        return this.local_partition_offsets[partition];
    }
    
    /**
     * For the given local partition offset generated by getLocalPartitionOffset(),
     * return its corresponding partition id
     * @param offset
     * @return
     * @see HStoreSite.getLocalPartitionOffset
     */
    public int getLocalPartitionFromOffset(int offset) {
        return this.local_partition_reverse[offset];
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
     * Get the Oberservable handle for this HStoreSite that can alert others when the party is ending
     * @return
     */
    public EventObservable<Object> getShutdownObservable() {
        return (this.shutdown_observable);
    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION STUFF
    // ----------------------------------------------------------------------------

    /**
     * Initializes all the pieces that we need to start this HStore site up
     */
    protected HStoreSite init() {
        if (d) LOG.debug("Initializing HStoreSite " + this.getSiteName());

        this.hstore_coordinator = this.initHStoreCoordinator();
        
        // First we need to tell the HStoreCoordinator to start-up and initialize its connections
        if (d) LOG.debug("Starting HStoreCoordinator for " + this.getSiteName());
        this.hstore_coordinator.start();

        // Start TransactionQueueManager
        Thread t = new Thread(this.txnQueueManager);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        t.start();
        
        // Start VoltNetwork
        t = new Thread(this.voltNetwork);
        t.setName(HStoreThreadManager.getThreadName(this, "voltnetwork"));
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(this.exceptionHandler);
        t.start();
        
        // Initialize Status Monitor
        if (hstore_conf.site.status_enable) {
            assert(hstore_conf.site.status_interval >= 0);
            this.status_monitor = new HStoreSiteStatus(this, hstore_conf);
        }
        
        // TransactionPreProcessors
        if (this.preProcessors != null) {
            for (TransactionPreProcessor tpp : this.preProcessors) {
                t = new Thread(tpp);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(this.exceptionHandler);
                t.start();    
            } // FOR
        }
        // TransactionPostProcessors
        if (this.postProcessors != null) {
            for (TransactionPostProcessor tpp : this.postProcessors) {
                t = new Thread(tpp);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(this.exceptionHandler);
                t.start();    
            } // FOR
        }
        
        // Then we need to start all of the PartitionExecutor in threads
        if (d) LOG.debug(String.format("Starting PartitionExecutor threads for %s partitions on %s",
                         this.local_partitions_arr.length, this.getSiteName()));
        for (int partition : this.local_partitions_arr) {
            PartitionExecutor executor = this.getPartitionExecutor(partition);
            executor.initHStoreSite(this);
            
            t = new Thread(executor);
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY); // Probably does nothing...
            t.setUncaughtExceptionHandler(this.exceptionHandler);
            this.executor_threads[partition] = t;
            t.start();
        } // FOR
        
        this.schedulePeriodicWorks();
        
        // Add in our shutdown hook
        // Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
        
        return (this);
    }
    
    
    /**
     * Initial internal stats sources
     */
    private void initStatSources() {
        StatsSource statsSource = null;
        
        // MEMORY
        statsSource = new MemoryStats();
        this.statsAgent.registerStatsSource(SysProcSelector.MEMORY, 0, statsSource);
        
        // TXN COUNTERS
        statsSource = new TransactionCounterStats(this.catalogContext);
        this.statsAgent.registerStatsSource(SysProcSelector.TXNCOUNTER, 0, statsSource);

        // TXN PROFILERS
        this.txnProfilerStats = new TransactionProfilerStats(this.catalogContext);
        this.statsAgent.registerStatsSource(SysProcSelector.TXNPROFILER, 0, this.txnProfilerStats);

        // EXECUTOR PROFILERS
        statsSource = new PartitionExecutorProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.EXECPROFILER, 0, statsSource);
        
        // QUEUE PROFILER
        statsSource = new TransactionQueueManagerProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.QUEUEPROFILER, 0, statsSource);
        
        // MARKOV ESTIMATOR PROFILER
        statsSource = new MarkovEstimatorProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.MARKOVPROFILER, 0, statsSource);
        
        // SPECEXEC PROFILER
        statsSource = new SpecExecProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.SPECEXECPROFILER, 0, statsSource);
        
        // CLIENT INTERFACE PROFILER
        statsSource = new SiteProfilerStats(this);
        this.statsAgent.registerStatsSource(SysProcSelector.SITEPROFILER, 0, statsSource);
        
        // OBJECT POOL COUNTERS
        statsSource = new PoolCounterStats(this.objectPools);
        this.statsAgent.registerStatsSource(SysProcSelector.POOL, 0, statsSource);
    }
    
    /**
     * Schedule all the periodic works
     */
    private void schedulePeriodicWorks() {
        this.threadManager.schedulePeriodicWork(new ExceptionHandlingRunnable() {
            @Override
            public void runImpl() {
                try {
                    HStoreSite.this.processPeriodicWork();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
        
        // HStoreStatus
        if (this.status_monitor != null) {
            this.threadManager.schedulePeriodicWork(
                this.status_monitor,
                hstore_conf.site.status_interval,
                hstore_conf.site.status_interval,
                TimeUnit.MILLISECONDS);
        }
        
        // AntiCache Memory Monitor
        if (this.anticacheManager != null) {
            if (this.anticacheManager.getEvictableTables().isEmpty() == false) {
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
        this.threadManager.schedulePeriodicWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, false);
            }
        }, 0, 1, TimeUnit.MINUTES);

        // large stats samples
        this.threadManager.schedulePeriodicWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, true);
            }
        }, 0, 6, TimeUnit.MINUTES);
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
                                   Arrays.toString(this.local_partitions_arr));
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
    
    // ----------------------------------------------------------------------------
    // SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    @Override
    public void prepareShutdown(boolean error) {
        this.shutdown_state = ShutdownState.PREPARE_SHUTDOWN;

        if (error && RingBufferAppender.getRingBufferAppender(LOG) != null) {
            Logger root = Logger.getRootLogger();
            for (Appender appender : CollectionUtil.iterable(root.getAllAppenders(), Appender.class)) {
                LOG.addAppender(appender);    
            } // FOR
        }
        
        if (this.hstore_coordinator != null)
            this.hstore_coordinator.prepareShutdown(false);
        
        this.txnQueueManager.prepareShutdown(error);
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
        
        if (this.adhoc_helper_started) {
            if (this.asyncCompilerWork_thread != null)
                this.asyncCompilerWork_thread.prepareShutdown(error);
        }
        
        for (int p : this.local_partitions_arr) {
            if (this.executors[p] != null) 
                this.executors[p].prepareShutdown(error);
        } // FOR
        
    }
    
    /**
     * Perform shutdown operations for this HStoreSiteNode
     */
    @Override
    public synchronized void shutdown(){
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
//            if (d)
                LOG.warn("Already told to shutdown... Ignoring");
            return;
        }
        if (this.shutdown_state != ShutdownState.PREPARE_SHUTDOWN) this.prepareShutdown(false);
        this.shutdown_state = ShutdownState.SHUTDOWN;
        if (d) LOG.debug("Shutting down everything at " + this.getSiteName());

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
      
        // this.threadManager.getPeriodicWorkExecutor().shutdown();
        
        // Stop AdHoc threads
        if (this.adhoc_helper_started) {
            if (this.asyncCompilerWork_thread != null)
                this.asyncCompilerWork_thread.shutdown();
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
        if (t) LOG.trace("Notifying " + this.shutdown_observable.countObservers() + " observers that we're shutting down");
        this.shutdown_observable.notifyObservers();
        
        // Tell our local boys to go down too
        for (int p : this.local_partitions_arr) {
            this.executors[p].shutdown();
        } // FOR
        
        if (this.voltNetwork != null) {
            try {
                this.voltNetwork.shutdown();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            this.clientInterface.shutdown();
        }
        
        this.hstore_coordinator.shutdown();
        
        LOG.info(String.format("Completed shutdown process at %s [hashCode=%d]",
                               this.getSiteName(), this.hashCode()));
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
    
    @Override
    public void invocationQueue(ByteBuffer buffer, final RpcCallback<byte[]> clientCallback) {
        // XXX: This is a big hack. We should just deal with the ClientResponseImpl directly
        RpcCallback<ClientResponseImpl> wrapperCallback = new RpcCallback<ClientResponseImpl>() {
            @Override
            public void run(ClientResponseImpl parameter) {
                if (trace.get()) LOG.trace("Serializing ClientResponse to byte array:\n" + parameter);
                
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
        long timestamp = EstTime.currentTimeMillis();

        // Extract the stuff we need to figure out whether this guy belongs at our site
        // We don't need to create a StoredProcedureInvocation anymore in order to
        // extract out the data that we need in this request
        final FastDeserializer incomingDeserializer = this.getIncomingDeserializer();
        incomingDeserializer.setBuffer(buffer);
        final long client_handle = StoredProcedureInvocation.getClientHandle(buffer);
        final int procId = StoredProcedureInvocation.getProcedureId(buffer);
        int base_partition = StoredProcedureInvocation.getBasePartition(buffer);
        if (t) LOG.trace(String.format("Raw Request: clientHandle=%d / procId=%d / basePartition=%d",
                         client_handle, procId, base_partition));
        
        // Optimization: We can get the Procedure catalog handle from its procId
        Procedure catalog_proc = catalogContext.getProcedureById(procId);
        String procName = null;
     
        // Otherwise, we have to get the procedure name and do a look up with that.
        if (catalog_proc == null) {
            procName = StoredProcedureInvocation.getProcedureName(incomingDeserializer);
            this.catalogContext.database.getProcedures().get(procName);
            if (catalog_proc == null) {
                catalog_proc = this.catalogContext.database.getProcedures().getIgnoreCase(procName);
            }
            
            // TODO: This should be an error message back to the client, not an exception
            if (catalog_proc == null) {
                String msg = "Unknown procedure '" + procName + "'";
                LOG.error(msg);
                this.responseError(client_handle,
                                   Status.ABORT_UNEXPECTED,
                                   msg,
                                   clientCallback,
                                   timestamp);
                return;
            }
        } else {
            procName = catalog_proc.getName();
        }
        boolean sysproc = catalog_proc.getSystemproc();
        
        // -------------------------------
        // PARAMETERSET INITIALIZATION
        // -------------------------------
        
        // Initialize the ParameterSet
        ParameterSet procParams = null;
        try {
//            procParams = objectPools.PARAMETERSETS.borrowObject();
            procParams = new ParameterSet();
            StoredProcedureInvocation.seekToParameterSet(buffer);
            incomingDeserializer.setBuffer(buffer);
            procParams.readExternal(incomingDeserializer);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } 
        assert(procParams != null) :
            "The parameters object is null for new txn from client #" + client_handle;
        if (d) LOG.debug(String.format("Received new stored procedure invocation request for %s " +
                         "[handle=%d]",
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
        
        // Profiling Updates
        if (hstore_conf.site.txn_counters) TransactionCounter.RECEIVED.inc(procName);
        if (hstore_conf.site.profiling && base_partition != -1) {
            synchronized (profiler.network_incoming_partitions) {
                profiler.network_incoming_partitions.put(base_partition);
            } // SYNCH
        }
        
        base_partition = this.txnInitializer.calculateBasePartition(client_handle,
                                                                    catalog_proc,
                                                                    procParams,
                                                                    base_partition);
        
        // -------------------------------
        // REDIRECT TXN TO PROPER BASE PARTITION
        // -------------------------------
        if (this.isLocalPartition(base_partition) == false) {
            // If the base_partition isn't local, then we need to ship it off to
            // the right HStoreSite
            this.transactionRedirect(catalog_proc, buffer, base_partition, clientCallback);
            return;
        }
        
        PartitionExecutor executor = this.executors[base_partition];
        boolean success = true;
        
        // We will initialize the transaction right here if the configuration parameter
        // is set to true. 
        // TODO: We need to measure whether it is faster to do it this way (with and without
        // the models) or whether it is faster to queue things up in the PartitionExecutor
        // and let it be responsible for sorting things out
        if (hstore_conf.site.network_txn_initialization) {
            if (t) LOG.trace("Initializing transaction request using network processing thread");
            LocalTransaction ts = this.txnInitializer.createLocalTransaction(
                                            buffer,
                                            timestamp,
                                            client_handle,
                                            base_partition,
                                            catalog_proc,
                                            procParams,
                                            clientCallback);
            this.transactionQueue(ts);
        }
        // We should queue the txn at the proper partition
        // The PartitionExecutor thread will be responsible for creating
        // the LocalTransaction handle and figuring out whatever else we need to
        // about this txn...
        else {
            success = executor.queueNewTransaction(buffer,
                                                   timestamp,
                                                   catalog_proc,
                                                   procParams,
                                                   clientCallback);
            if (success == false) {
                Status status = Status.ABORT_REJECT;
//                if (catalog_proc.getName().equalsIgnoreCase("@Quiesce"))
                if (d) LOG.debug(String.format("Hit with a %s response from partition %d " +
                                 "[queueSize=%d]",
                                 status, base_partition, executor.getDebugContext().getWorkQueueSize()));
                this.responseError(client_handle,
                                   status,
                                   REJECTION_MESSAGE + " - [1]",
                                   clientCallback,
                                   timestamp);
            }
        }

        if (hstore_conf.site.txn_counters && success == false) {
            TransactionCounter.REJECTED.inc(catalog_proc);
        }
        
        if (t) LOG.trace(String.format("Finished initial processing of new txn. [success=%s]", success));
        EstTimeUpdater.update(System.currentTimeMillis());
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
        if (catalog_proc.getName().equals("@Shutdown")) {
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
        else if (catalog_proc.getName().equals("@Quiesce")) {
            // Tell the queue manager ahead of time to wipe out everything!
            this.txnQueueManager.clearQueues();
            return (false);
        }
        
        // -------------------------------
        // ADHOC
        // -------------------------------
        else if (catalog_proc.getName().equals("@AdHoc")) {
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
            int idx = (int)(Math.abs(client_handle) % this.local_partitions_arr.length);
            int base_partition = this.local_partitions_arr[idx].intValue();
            
            LocalTransaction ts = this.txnInitializer.createLocalTransaction(null,
                                                                             EstTime.currentTimeMillis(),
                                                                             client_handle,
                                                                             base_partition,
                                                                             catalog_proc,
                                                                             params,
                                                                             clientCallback);
            String sql = (String)params.toArray()[0];
            this.asyncCompilerWork_thread.planSQL(ts, sql);
            return (true);
        }
        
        return (false);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION OPERATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Queue a new transaction for execution. If it is a single-partition txn, then it will
     * be queued at its base partition's PartitionExecutor queue. If it is distributed transaction,
     * then it will need to first acquire the locks for all of the partitions that it wants to
     * access.
     * <B>Note:</B> This method should only be used for distributed transactions.
     * Single-partition txns should be queued up directly within their base PartitionExecutor.
     * @param ts
     */
    public void transactionQueue(LocalTransaction ts) {
        assert(ts.isInitialized()) : 
            "Unexpected uninitialized LocalTranaction for " + ts;
        
        // Make sure that we start the MapReduceHelperThread
        if (ts.isMapReduce() && this.mr_helper_started == false) {
            assert(this.mr_helper != null);
            this.startMapReduceHelper();
        }
                
        if (t) LOG.trace(ts + " - Dispatching new transaction invocation");
        
        // -------------------------------
        // SINGLE-PARTITION or NON-BLOCKING MAPREDUCE TRANSACTION
        // -------------------------------
        if (ts.isPredictSinglePartition() || (ts.isMapReduce() && hstore_conf.site.mr_map_blocking == false)) {
            if (d) LOG.debug(String.format("%s - Fast path single-partition execution at partition %d " +
                             "[handle=%d]",
                             ts, ts.getBasePartition(), ts.getClientHandle()));
            this.transactionStart(ts, ts.getBasePartition());
        }
        // -------------------------------    
        // DISTRIBUTED TRANSACTION
        // -------------------------------
        else {
            final int base_partition = ts.getBasePartition();
            final Long txn_id = ts.getTransactionId();
            if (d) LOG.debug(String.format("%s - Queuing distributed transaction to execute at " +
                             "partition %d [handle=%d]",
                             ts, base_partition, ts.getClientHandle()));
            
            // Check whether our transaction can't run right now because its id is less than
            // the last seen txnid from the remote partitions that it wants to touch
            for (Integer partition : ts.getPredictTouchedPartitions()) {
                Long last_txn_id = this.txnQueueManager.getLastLockTransaction(partition.intValue()); 
                if (txn_id.compareTo(last_txn_id) < 0) {
                    // If we catch it here, then we can just block ourselves until
                    // we generate a txn_id with a greater value and then re-add ourselves
                    if (d) {
                        LOG.warn(String.format("%s - Unable to queue transaction because the last txn " +
                                 "id at partition %d is %d. Restarting...",
                                 ts, partition, last_txn_id));
                        LOG.warn(String.format("LastTxnId:#%s / NewTxnId:#%s",
                                 TransactionIdManager.toString(last_txn_id),
                                 TransactionIdManager.toString(txn_id)));
                    }
                    if (hstore_conf.site.txn_counters && ts.getRestartCounter() == 1) {
                        TransactionCounter.BLOCKED_LOCAL.inc(ts.getProcedure());
                    }
                    this.txnQueueManager.blockTransaction(ts, partition.intValue(), last_txn_id);
                    return;
                }
            } // FOR
            
            // This callback prevents us from making additional requests to the Dtxn.Coordinator until
            // we get hear back about our our initialization request
            if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startInitDtxn();
            this.txnQueueManager.initTransaction(ts);
        }
    }
    
    /**
     * Add the given transaction id to this site's queue manager with all of the partitions that
     * it needs to lock. This is only for distributed transactions.
     * The callback will be invoked once the transaction has acquired all of the locks for the
     * partitions provided, or aborted if the transaction is unable to lock those partitions.
     * @param txn_id
     * @param partitions The list of partitions that this transaction needs to access
     * @param callback
     */
    public void transactionInit(Long txn_id,
                                int procedureId,
                                PartitionSet partitions,
                                int base_partition,
                                RpcCallback<TransactionInitResponse> callback) {
        Procedure catalog_proc = catalogContext.getProcedureById(procedureId);
        
        // We should always force a txn from a remote partition into the queue manager
        this.txnQueueManager.lockQueueInsert(txn_id, partitions, base_partition, procedureId, callback, catalog_proc.getSystemproc());
    }

    /**
     * Queue the transaction to start executing on its base partition.
     * This function can block a transaction executing on that partition
     * <B>IMPORTANT:</B> The transaction could be deleted after calling this if it is rejected
     * @param ts, base_partition
     */
    public void transactionStart(LocalTransaction ts, int base_partition) {
        final Long txn_id = ts.getTransactionId();
        final Procedure catalog_proc = ts.getProcedure();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        
        if (d) LOG.debug(String.format("Starting %s %s on partition %d%s",
                         (singlePartitioned ? "single-partition" : "distributed"),
                         ts, base_partition,
                         (singlePartitioned ? "" : " [partitions=" + ts.getPredictTouchedPartitions() + "]")));
        assert(ts.getPredictTouchedPartitions().isEmpty() == false) :
            "No predicted partitions for " + ts + "\n" + ts.debug();
        
        assert(this.executors[base_partition] != null) :
            "Unable to start " + ts + " - No PartitionExecutor exists for partition #" + base_partition + " at HStoreSite " + this.site_id;
        
        if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startQueue();
        final boolean success = this.executors[base_partition].queueNewTransaction(ts);
        
        if (hstore_conf.site.txn_counters && success) {
            assert(catalog_proc != null) :
                String.format("Null Procedure for txn #%d [hashCode=%d]", txn_id, ts.hashCode());
            TransactionCounter.EXECUTED.inc(catalog_proc);
        }
        
        if (success == false) {
            // Depending on what we need to do for this type txn, we will send
            // either an ABORT_THROTTLED or an ABORT_REJECT in our response
            // An ABORT_THROTTLED means that the client will back-off of a bit
            // before sending another txn request, where as an ABORT_REJECT means
            // that it will just try immediately
            Status status = Status.ABORT_REJECT;
            if (d) LOG.debug(String.format("%s - Hit with a %s response from partition %d " +
                             "[queueSize=%d]",
                             ts, status, base_partition,
                             this.executors[base_partition].getDebugContext().getWorkQueueSize()));
            if (singlePartitioned == false) {
                TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(status);
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
        if (d) LOG.debug(String.format("%s - Queuing %s on partition %d " +
                         "[prefetch=%s]",
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
                this.remoteTxnEstimator.processQueryEstimate(t_state, query_estimate, fragment.getPartitionId());
            }
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
     * @param txn_id
     * @param partitions
     * @param updated
     */
    public void transactionPrepare(Long txn_id, PartitionSet partitions) {
        if (d) LOG.debug(String.format("2PC:PREPARE Txn #%d [partitions=%s]", txn_id, partitions));
        
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        if (ts instanceof LocalTransaction) {
            ((LocalTransaction)ts).getOrInitTransactionPrepareCallback();
        }
        
        for (int p : this.local_partitions.values()) {
            if (partitions.contains(p) == false) continue;

            // We could have been asked to participate in a distributed transaction but
            // they never actually sent us anything, so we should just tell the queue manager
            // that the txn is done. There is nothing that we need to do at the PartitionExecutors
            if (ts == null) {
                if (t) LOG.trace(String.format("Telling queue manager that txn #%d is finished at partition %d", txn_id, p));
                this.txnQueueManager.lockQueueFinished(txn_id, Status.OK, p);
            }
            // Otherwise we are going queue a PrepareTxnMessage at each of the partitions that 
            // this txn is using and let them deal with it.
            else {
                if (hstore_conf.site.exec_profiling && p != ts.getBasePartition() && ts.needsFinish(p)) {
                    PartitionExecutorProfiler pep = this.executors[p].getProfiler();
                    assert(pep != null);
                    pep.idle_2pc_remote_time.start();
                }
                
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
                this.executors[p].queuePrepare(ts);
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
        if (d) LOG.debug(String.format("2PC:FINISH Txn #%d [status=%s, partitions=%s]",
                         txn_id, status, partitions));
        boolean commit = (status == Status.OK);
        
        // If we don't have a AbstractTransaction handle, then we know that we never did anything
        // for this transaction and we can just ignore this finish request. We do have to tell
        // the TransactionQueue manager that we're done though
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        TransactionFinishCallback finish_callback = null;
        TransactionCleanupCallback cleanup_callback = null;
        if (ts != null) {
            ts.setStatus(status);
            
            if (ts instanceof RemoteTransaction || ts instanceof MapReduceTransaction) {
                if (t) LOG.trace(ts + " - Initializing the TransactionCleanupCallback");
                // TODO(xin): We should not be invoking this callback at the basePartition's site
                if ( !(ts instanceof MapReduceTransaction && this.isLocalPartition(ts.getBasePartition()))) {
                    cleanup_callback = ts.getCleanupCallback();
                    assert(cleanup_callback != null);
                    cleanup_callback.init(ts, status, partitions);
                }
            } else {
                finish_callback = ((LocalTransaction)ts).getTransactionFinishCallback();
                assert(finish_callback != null);
            }
        }
        
        for (Integer p : partitions) {
            if (this.isLocalPartition(p.intValue()) == false) {
                if (t) LOG.trace(String.format("#%d - Skipping finish at partition %d", txn_id, p));
                continue;
            }
            if (t) LOG.trace(String.format("#%d - Invoking finish at partition %d", txn_id, p));
            
            // We only need to tell the queue stuff that the transaction is finished
            // if it's not a commit because there won't be a 2PC:PREPARE message
            if (commit == false) this.txnQueueManager.lockQueueFinished(txn_id, status, p.intValue());

            // Then actually commit the transaction in the execution engine
            // We only need to do this for distributed transactions, because all single-partition
            // transactions will commit/abort immediately
            if (ts != null && ts.isPredictSinglePartition() == false && ts.needsFinish(p.intValue())) {
                if (t) LOG.trace(String.format("%s - Calling finishTransaction on partition %d", ts, p));
                try {
                    this.executors[p.intValue()].queueFinish(ts, status);
                } catch (Throwable ex) {
                    LOG.error(String.format("Unexpected error when trying to finish %s\nHashCode: %d / Status: %s / Partitions: %s",
                              ts, ts.hashCode(), status, partitions));
                    throw new RuntimeException(ex);
                }
            }
            // If this is a LocalTransaction, then we want to just decrement their TransactionFinishCallback counter
            else if (finish_callback != null) {
                if (t) LOG.trace(String.format("%s - Notifying %s that the txn is finished at partition %d",
                                 ts, finish_callback.getClass().getSimpleName(), p));
                finish_callback.decrementCounter(1);
            }
            // If we didn't queue the transaction to be finished at this partition, then we need to make sure
            // that we mark the transaction as finished for this callback
            else if (cleanup_callback != null) {
                if (t) LOG.trace(String.format("%s - Notifying %s that the txn is finished at partition %d",
                                 ts, cleanup_callback.getClass().getSimpleName(), p));
                cleanup_callback.run(p);
            }
        } // FOR            
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
        if (d) LOG.debug(String.format("Forwarding %s request to partition %d", catalog_proc.getName(), base_partition));
        
        // Make a wrapper for the original callback so that when the result comes back frm the remote partition
        // we will just forward it back to the client. How sweet is that??
        TransactionRedirectCallback callback = null;
        try {
            callback = (TransactionRedirectCallback)objectPools.CALLBACKS_TXN_REDIRECT_REQUEST.borrowObject();
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
        assert(ts.isInitialized());
        if (d) LOG.debug(String.format("%s - Rejecting transaction with status %s [clientHandle=%d]",
                         ts, status, ts.getClientHandle()));
        
        String msg = this.REJECTION_MESSAGE + " - [0]";
        if (ts.getProcedure().getSystemproc()) {
            try {
                throw new Exception(msg);
            } catch (Exception ex) {
                StringWriter writer = new StringWriter();
                ex.printStackTrace(new PrintWriter(writer));
                msg = writer.toString();
                if (d) LOG.warn(String.format("%s - Rejecting transaction with status %s [clientHandle=%d]",
                                ts, status, ts.getClientHandle()), ex);
            }
        }
        
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
        assert(orig_ts != null) : "Null LocalTransaction handle [status=" + status + "]";
        assert(orig_ts.isInitialized()) : "Uninitialized transaction??";
        if (d) LOG.debug(String.format("%s got hit with a %s! Going to clean-up our mess and re-execute " +
                         "[restarts=%d]",
                         orig_ts , status, orig_ts.getRestartCounter()));
        int base_partition = orig_ts.getBasePartition();
        SerializableException orig_error = orig_ts.getPendingError();
        
        // If this txn has been restarted too many times, then we'll just give up
        // and reject it outright
        int restart_limit = (orig_ts.isSysProc() ? hstore_conf.site.txn_restart_limit_sysproc :
                                                   hstore_conf.site.txn_restart_limit);
        if (orig_ts.getRestartCounter() > restart_limit) {
            if (orig_ts.isSysProc()) {
                String msg = String.format("%s has been restarted %d times! Rejecting...",
                                           orig_ts, orig_ts.getRestartCounter());
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
                 status != Status.ABORT_EVICTEDACCESS) {
            // Figure out whether this transaction should be redirected based on what partitions it
            // tried to touch before it was aborted
            Histogram<Integer> touched = orig_ts.getTouchedPartitions();
            Collection<Integer> most_touched = touched.getMaxCountValues();
            assert(most_touched != null) :
                "Failed to get most touched partition for " + orig_ts + "\n" + touched;
            
            // XXX: We should probably decrement the base partition by one 
            //      so that we only consider where they actually executed queries
            if (t) LOG.trace(String.format("Touched partitions for mispredicted %s%s",
                                           orig_ts, (t ? "\n"+touched : " " + touched.values())));
            Integer redirect_partition = null;
            if (most_touched.size() == 1) {
                redirect_partition = CollectionUtil.first(most_touched);
            }
            // If the original base partition is in our most touched set, then
            // we'll prefer to use that
            else if (most_touched.isEmpty() == false) {
                if (most_touched.contains(base_partition)) {
                    redirect_partition = base_partition;
                } else {
                    redirect_partition = CollectionUtil.random(most_touched);
                }
            } else {
                redirect_partition = CollectionUtil.random(this.catalogContext.getAllPartitionIds());
            }
            assert(redirect_partition != null) : "Redirect partition is null!\n" + orig_ts.debug();
            if (t) {
                LOG.trace("Redirect Partition: " + redirect_partition + " -> " + (this.isLocalPartition(redirect_partition) == false));
                LOG.trace("Local Partitions: " + Arrays.toString(this.local_partitions_arr));
            }
            
            // If the txn wants to execute on another node, then we'll send them off *only* if this txn wasn't
            // already redirected at least once. If this txn was already redirected, then it's going to just
            // execute on the same partition, but this time as a multi-partition txn that locks all partitions.
            // That's what you get for messing up!!
            if (this.isLocalPartition(redirect_partition.intValue()) == false && orig_ts.getRestartCounter() == 0) {
                if (d) LOG.debug(String.format("%s - Redirecting to partition %d because of misprediction",
                                               orig_ts, redirect_partition));
                
                Procedure catalog_proc = orig_ts.getProcedure();
                StoredProcedureInvocation spi = new StoredProcedureInvocation(orig_ts.getClientHandle(),
                                                                              catalog_proc.getId(),
                                                                              catalog_proc.getName(),
                                                                              orig_ts.getProcedureParameters().toArray());
                spi.setBasePartition(redirect_partition.intValue());
                spi.setRestartCounter(orig_ts.getRestartCounter()+1);
                
                FastSerializer out = this.getOutgoingSerializer();
                try {
                    out.writeObject(spi);
                } catch (IOException ex) {
                    String msg = "Failed to serialize StoredProcedureInvocation to redirect txn";
                    throw new ServerFaultException(msg, ex, orig_ts.getTransactionId());
                }
                
                TransactionRedirectCallback callback;
                try {
                    callback = (TransactionRedirectCallback)objectPools.CALLBACKS_TXN_REDIRECT_REQUEST.borrowObject();
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
                if (redirect_partition.intValue() != base_partition &&
                    this.isLocalPartition(redirect_partition.intValue())) {
                    if (d) LOG.debug(String.format("%s - Redirecting to local partition %d [restartCtr=%d]%s",
                                                    orig_ts, redirect_partition, orig_ts.getRestartCounter(),
                                                    (t ? "\n"+touched : "")));
                    base_partition = redirect_partition.intValue();
                }
            } else {
                if (d) LOG.debug(String.format("%s - Mispredicted txn has already been aborted once before. " +
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
        if (status == Status.ABORT_RESTART || status == Status.ABORT_EVICTEDACCESS) {
            predict_touchedPartitions = new PartitionSet(orig_ts.getPredictTouchedPartitions());
            malloc = true;
        } else if (orig_ts.getRestartCounter() == 0) {
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
            if (t) LOG.trace(orig_ts + " Mispredicted Partitions: " + partitions);
        }
        
        if (predict_touchedPartitions.contains(base_partition) == false) {
            if (malloc == false) {
                predict_touchedPartitions = new PartitionSet(predict_touchedPartitions);
                malloc = true;
            }
            predict_touchedPartitions.add(base_partition);
        }
        if (predict_touchedPartitions.isEmpty()) 
            predict_touchedPartitions = this.catalogContext.getAllPartitionIds();
        
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
        if (status == Status.ABORT_EVICTEDACCESS) {
            if (this.anticacheManager == null) {
                String message = "Got eviction notice but anti-caching is not enabled";
                throw new ServerFaultException(message, orig_error, orig_ts.getTransactionId());
            }

            EvictedTupleAccessException error = (EvictedTupleAccessException)orig_error;
            Table catalog_tbl = error.getTableId(this.catalogContext.database);
            short block_ids[] = error.getBlockIds();
            this.anticacheManager.queue(new_ts, base_partition, catalog_tbl, block_ids);
        }
            
        // -------------------------------
        // REGULAR TXN REQUEUE
        // -------------------------------
        else {
            if (d) {
                LOG.debug(String.format("Re-executing %s as new %s-partition %s on partition %d " +
                          "[restarts=%d, partitions=%s]",
                          orig_ts,
                          (predict_touchedPartitions.size() == 1 ? "single" : "multi"),
                          new_ts,
                          base_partition,
                          new_ts.getRestartCounter(),
                          predict_touchedPartitions));
                if (t && status == Status.ABORT_MISPREDICT)
                    LOG.trace(String.format("%s Mispredicted partitions\n%s",
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
        if (this.commandLogger != null && status == Status.OK && ts.isSysProc() == false) {
            sendResponse = this.commandLogger.appendToLog(ts, cresponse);
        }

        if (sendResponse) {
            // NO GROUP COMMIT -- SEND OUT AND COMPLETE
            // NO COMMAND LOGGING OR TXN ABORTED -- SEND OUT AND COMPLETE
            this.responseSend(cresponse,
                              ts.getClientCallback(),
                              ts.getInitiateTime(),
                              ts.getRestartCounter());
        } else if (d) { 
            LOG.debug(String.format("%s - Holding the ClientResponse until logged to disk", ts));
        }
        if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.stopPostClient();
    }
    
    /**
     * Instead of having the PartitionExecutor send the ClientResponse directly back
     * to the client, this method will queue it up at one of the TransactionPostProcessors.
     * This feature is not useful if the command-logging is enabled.  
     * @param es
     * @param ts
     * @param cr
     */
    public void responseQueue(LocalTransaction ts, ClientResponseImpl cr) {
        assert(hstore_conf.site.exec_postprocessing_threads);
        if (d) LOG.debug(String.format("Adding ClientResponse for %s from partition %d to processing queue [status=%s, size=%d]",
                         ts, ts.getBasePartition(), cr.getStatus(), this.postProcessorQueue.size()));
        this.postProcessorQueue.add(Pair.of(ts,cr));
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
        if (d) LOG.debug(String.format("Txn #%d - Sending back ClientResponse [status=%s%s]",
                         cresponse.getTransactionId(), status,
                         (status == Status.ABORT_UNEXPECTED ? "\n" + StringUtil.join("\n", cresponse.getException().getStackTrace()) : "")));
        
        long now = System.currentTimeMillis();
        EstTimeUpdater.update(now);
        cresponse.setClusterRoundtrip((int)(now - initiateTime));
        cresponse.setRestartCounter(restartCounter);
        try {
            clientCallback.run(cresponse);
        } catch (ClientConnectionLostException ex) {
            // There is nothing else we can really do here. We'll clean up
            // the transaction just as normal and report the error
            // in our logs if they have debugging turned on
            if (t) LOG.warn("Failed to send back ClientResponse for txn #" + cresponse.getTransactionId(), ex);
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
        if (t) LOG.trace(String.format("Queueing txn #%d for deletion [status=%s]", txn_id, status));
        
        // Update Transaction profiler
        // We want to call this before we queue it so that the post-finish time is more accurate
        if (hstore_conf.site.txn_profiling) {
            AbstractTransaction ts = this.inflight_txns.get(txn_id);
            // XXX: Should we include totals for mispredicted txns?
            if (ts != null && status != Status.ABORT_MISPREDICT && ts instanceof LocalTransaction) {
                LocalTransaction local_ts = (LocalTransaction)ts;
                if (local_ts.profiler != null && local_ts.profiler.isDisabled() == false) {
                    local_ts.profiler.stopTransaction();
                    if (this.txnProfilerStats != null) {
                        this.txnProfilerStats.addTxnProfile(local_ts.getProcedure(), local_ts.profiler);
                    }
                    if (this.status_monitor != null) {
                        this.status_monitor.addTxnProfile(local_ts.getProcedure(), local_ts.profiler);
                    }
                }
            }
        }
        
        // Queue it up for deletion! There is no return for the txn from this!
        // int idx = (int)(txn_id.longValue() % this.deletable_txns.length);
        // this.deletable_txns[idx].offer(Pair.of(txn_id, status));
        this.deletable_txns[status.ordinal()].offer(txn_id);
        
        
//        AbstractTransaction ts = this.inflight_txns.get(txn_id);
//        if (ts != null) {
//            assert(txn_id.equals(ts.getTransactionId())) :
//                String.format("Mismatched %s - Expected[%d] != Actual[%s]",
//                              ts, txn_id, ts.getTransactionId());
//            if (ts instanceof RemoteTransaction) {
//                this.deleteRemoteTransaction((RemoteTransaction)ts, status);
//            }
//            // We need to check whether a LocalTransaction is ready to be deleted
//            else if (((LocalTransaction)ts).isDeletable()) {
//                this.deleteLocalTransaction((LocalTransaction)ts, status);
//            }
//        }
    }
    
    /**
     * Clean-up all of the state information about a RemoteTransaction that is finished
     * <B>Note:</B> This should only be invoked for non-local distributed txns
     * @param ts
     * @param status
     */
    private void deleteRemoteTransaction(RemoteTransaction ts, Status status) {
        // Nothing else to do for RemoteTransactions other than to just
        // return the object back into the pool
        AbstractTransaction rm = this.inflight_txns.remove(ts.getTransactionId());
        if (d) LOG.debug(String.format("Deleted %s [%s / inflightRemoval:%s]", ts, status, (rm != null)));
        
        EstimatorState t_state = ts.getEstimatorState(); 
        if (t_state != null) {
            this.remoteTxnEstimator.destroyEstimatorState(t_state);
        }
        
        this.objectPools.getRemoteTransactionPool(ts.getBasePartition())
                        .returnObject(ts);
        return;
    }

    /**
     * Clean-up all of the state information about a LocalTransaction that is finished
     * @param ts
     * @param status
     */
    private void deleteLocalTransaction(LocalTransaction ts, final Status status) {
        final int base_partition = ts.getBasePartition();
        final Procedure catalog_proc = ts.getProcedure();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
       
        if (t) LOG.trace(ts + " - State before delete:\n" + ts.debug());
        assert(ts.checkDeletableFlag()) :
            String.format("Trying to delete %s before it was marked as ready!", ts);
        
        // Clean-up any extra information that we may have for the txn
        TransactionEstimator t_estimator = null;
        EstimatorState t_state = ts.getEstimatorState(); 
        if (t_state != null) {
            t_estimator = this.executors[base_partition].getTransactionEstimator();
            assert(t_estimator != null);
        }
        try {
            switch (status) {
                case OK:
                    if (t_estimator != null) {
                        if (t) LOG.trace("Telling the TransactionEstimator to COMMIT " + ts);
                        t_estimator.commit(t_state);
                    }
                    // We always need to keep track of how many txns we process 
                    // in order to check whether we are hung or not
                    if (hstore_conf.site.txn_counters || hstore_conf.site.status_kill_if_hung) 
                        TransactionCounter.COMPLETED.inc(catalog_proc);
                    break;
                case ABORT_USER:
                    if (t_estimator != null) {
                        if (t) LOG.trace("Telling the TransactionEstimator to ABORT " + ts);
                        t_estimator.abort(t_state, status);
                    }
                    if (hstore_conf.site.txn_counters)
                        TransactionCounter.ABORTED.inc(catalog_proc);
                    break;
                case ABORT_MISPREDICT:
                case ABORT_RESTART:
                case ABORT_EVICTEDACCESS:
                    if (t_estimator != null) {
                        if (t) LOG.trace("Telling the TransactionEstimator to IGNORE " + ts);
                        t_estimator.abort(t_state, status);
                    }
                    if (hstore_conf.site.txn_counters) {
                        if (status == Status.ABORT_EVICTEDACCESS) {
                            TransactionCounter.EVICTEDACCESS.inc(catalog_proc);
                        } else {
                            (ts.isSpeculative() ? TransactionCounter.RESTARTED : TransactionCounter.MISPREDICTED).inc(catalog_proc);
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
            LOG.error(String.format("Unexpected error when cleaning up %s transaction %s",
                      status, ts), ex);
            // Pass...
        } finally {
            if (t_state != null && t_estimator != null) {
                assert(ts.getTransactionId() == t_state.getTransactionId()) :
                    String.format("Unexpected mismatch txnId in %s [%d != %d]",
                                  t_state.getClass().getSimpleName(),
                                  ts.getTransactionId(), t_state.getTransactionId());
                t_estimator.destroyEstimatorState(t_state);
            }
        }
        
        // Then update transaction profiling counters
        if (hstore_conf.site.txn_counters) {
            if (ts.isSpeculative()) TransactionCounter.SPECULATIVE.inc(catalog_proc);
            if (ts.isExecNoUndoBuffer(base_partition)) TransactionCounter.NO_UNDO.inc(catalog_proc);
            if (ts.isSysProc()) {
                TransactionCounter.SYSPROCS.inc(catalog_proc);
            } else if (status != Status.ABORT_MISPREDICT &&
                       status != Status.ABORT_REJECT &&
                       status != Status.ABORT_EVICTEDACCESS) {
                (singlePartitioned ? TransactionCounter.SINGLE_PARTITION : TransactionCounter.MULTI_PARTITION).inc(catalog_proc);
            }
        }
        
        // SANITY CHECK
        if (hstore_conf.site.exec_validate_work) {
            for (Integer p : this.local_partitions_arr) {
                assert(ts.equals(this.executors[p.intValue()].getDebugContext().getCurrentDtxn()) == false) :
                    String.format("About to finish %s but it is still the current DTXN at partition %d", ts, p);
            } // FOR
        }

        AbstractTransaction rm = this.inflight_txns.remove(ts.getTransactionId());
        assert(rm == null || rm == ts) : String.format("%s != %s", ts, rm);
        if (t) LOG.trace(String.format("Deleted %s [%s / inflightRemoval:%s]", ts, status, (rm != null)));
        
        assert(ts.isInitialized()) : "Trying to return uninititlized txn #" + ts.getTransactionId();
        if (d) LOG.debug(String.format("%s - Returning to ObjectPool [hashCode=%d]", ts, ts.hashCode()));
        if (ts.isMapReduce()) {
            objectPools.getMapReduceTransactionPool(base_partition).returnObject((MapReduceTransaction)ts);
        } else {
            objectPools.getLocalTransactionPool(base_partition).returnObject(ts);
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
        if (t) LOG.trace("Checking for PeriodicWork...");

        EstTimeUpdater.update(System.currentTimeMillis());
        
        if (this.clientInterface != null) {
            this.clientInterface.checkForDeadConnections(EstTime.currentTimeMillis());
        }
        
        // poll planner queue
        if (this.asyncCompilerWork_thread != null) {
            checkForFinishedCompilerWork();
        }
        
        // Delete txn handles
        // Pair<Long, Status> p = null;
        Long txn_id = null;
        // for (int i = 0; i < this.deletable_txns.length; i++) {
        
        if (hstore_conf.site.profiling) this.profiler.cleanup.start();
        for (Status status : Status.values()) {
            // Queue<Pair<Long, Status>> queue = this.deletable_txns[i]; 
            Queue<Long> queue = this.deletable_txns[status.ordinal()];
            this.deletable_txns_requeue.clear();
            // while ((p = queue.poll()) != null) {
            while ((txn_id = queue.poll()) != null) {
                // txn_id = p.getFirst();
                // Status status = p.getSecond();
                
                // It's ok for us to not have a transaction handle, because it could be
                // for a remote transaction that told us that they were going to need one
                // of our partitions but then they never actually sent work to us
                AbstractTransaction ts = this.inflight_txns.get(txn_id);
                if (ts != null) {
                    assert(txn_id.equals(ts.getTransactionId())) :
                        String.format("Mismatched %s - Expected[%d] != Actual[%s]",
                                      ts, txn_id, ts.getTransactionId());
                
                    // We will delete any RemoteTransaction right away
                    if (ts instanceof RemoteTransaction) {
                        this.deleteRemoteTransaction((RemoteTransaction)ts, status);
                    }
                    // We need to check whether a LocalTransaction is ready to be deleted
                    else if (((LocalTransaction)ts).isDeletable()) {
                        this.deleteLocalTransaction((LocalTransaction)ts, status);
                    }
                    // We can't delete this yet, so we'll just stop checking
                    else {
                        if (t) LOG.trace(String.format("%s - Cannot delete txn at this point [status=%s]\n%s",
                                         ts, status, ts.debug()));
                        this.deletable_txns_requeue.add(txn_id);
                        // this.deletable_txns_requeue.add(p);
                    }
                } else if (d) {
                    LOG.warn(String.format("Ignoring clean-up request for txn #%d because we do not have a handle " +
                             "[status=%s]", txn_id, status));
                }
            } // WHILE
            if (this.deletable_txns_requeue.isEmpty() == false) {
                if (t) LOG.trace(String.format("Adding %d undeletable txns back to deletable queue",
                                 this.deletable_txns_requeue.size()));
                queue.addAll(this.deletable_txns_requeue);
            }
        } // FOR
        if (hstore_conf.site.profiling) this.profiler.cleanup.stop();

        return;
    }

    /**
     * Added for @AdHoc processes
     * 
     */
    private void checkForFinishedCompilerWork() {
        if (t) LOG.trace("HStoreSite - Checking for finished compiled work.");
        AsyncCompilerResult result = null;
 
        while ((result = asyncCompilerWork_thread.getPlannedStmt()) != null) {
            if (d) LOG.debug("AsyncCompilerResult\n" + result);
            
            // ----------------------------------
            // BUSTED!
            // ----------------------------------
            if (result.errorMsg != null) {
                if (d)
                    LOG.error("Unexpected AsyncCompiler Error:\n" + result.errorMsg);
                
                ClientResponseImpl errorResponse =
                        new ClientResponseImpl(-1,
                                               result.clientHandle,
                                               this.local_partition_reverse[0],
                                               Status.ABORT_UNEXPECTED,
                                               HStoreConstants.EMPTY_RESULT,
                                               result.errorMsg);
                this.responseSend(result.ts, errorResponse);
                
                // We can just delete the LocalTransaction handle directly
                boolean deletable = result.ts.isDeletable();
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
                
                if (d) LOG.debug("Queuing AdHoc transaction: " + result.ts);
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
            for (int i = 0; i < deletable_txns.length; i++) {
                total += deletable_txns[i].size();
            }
            return (total);
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
        if (cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            cachedDebugContext = new HStoreSite.Debug(); 
        }
        return cachedDebugContext;
    }

}
