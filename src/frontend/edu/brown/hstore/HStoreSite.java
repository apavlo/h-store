/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
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

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.TransactionCleanupCallback;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.hstore.callbacks.TransactionRedirectCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.dtxn.RemoteTransaction;
import edu.brown.hstore.dtxn.TransactionQueueManager;
import edu.brown.hstore.estimators.AbstractEstimator;
import edu.brown.hstore.estimators.SEATSEstimator;
import edu.brown.hstore.estimators.TM1Estimator;
import edu.brown.hstore.estimators.TPCCEstimator;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.MapReduceHelperThread;
import edu.brown.hstore.util.PartitionExecutorPostProcessor;
import edu.brown.hstore.util.TxnCounter;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.TransactionEstimator;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.protorpc.NIOEventLoop;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class HStoreSite implements VoltProcedureListener.Handler, Shutdownable, Loggable, Runnable {
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
    // OBJECT POOLS
    // ----------------------------------------------------------------------------

    /**
     * This buffer pool is used to serialize ClientResponses to send back
     * to clients.
     */
    private final DBBPool buffer_pool = new DBBPool(false, false);
    
    private final HStoreThreadManager threadManager;
    
    private final TransactionQueueManager txnQueueManager;
    
    /**
     * This is the thing that we will actually use to generate txn ids used by our H-Store specific code
     */
    private final TransactionIdManager txnIdManagers[];
    
    /**
     * We will bind this variable after construction so that we can inject some
     * testing code as needed.
     */
    private HStoreCoordinator hstore_coordinator;

    /**
     * Local PartitionExecutor Stuff
     */
    private final PartitionExecutor executors[];
    private final Thread executor_threads[];
    
    /**
     * Procedure Listener Stuff
     */
    private VoltProcedureListener voltListener;
    private final NIOEventLoop procEventLoop = new NIOEventLoop();

    /**
     * 
     */
    private boolean ready = false;
    private CountDownLatch ready_latch;
    private final EventObservable<Object> ready_observable = new EventObservable<Object>();
    
    /**
     * This flag is set to true when we receive the first non-sysproc stored procedure
     * Other components of the system can attach to the EventObservable to be told when this occurs 
     */
    private boolean startWorkload = false;
    private final EventObservable<AbstractTransaction> startWorkload_observable = new EventObservable<AbstractTransaction>();
    
    /**
     * 
     */
    private Shutdownable.ShutdownState shutdown_state = ShutdownState.INITIALIZED;
    private final EventObservable<Object> shutdown_observable = new EventObservable<Object>();
    
    /** Catalog Stuff **/
    private final HStoreConf hstore_conf;
    private final Site catalog_site;
    private final int site_id;
    private final String site_name;
    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final AbstractHasher hasher;
    
    /** All of the partitions in the cluster */
    private final Collection<Integer> all_partitions;

    /** Request counter **/
    private final AtomicInteger request_counter = new AtomicInteger(0); 
    
    /**
     * Keep track of which txns that we have in-flight right now
     */
    private final Map<Long, AbstractTransaction> inflight_txns = new ConcurrentHashMap<Long, AbstractTransaction>();
    
    /**
     * ClientResponse Processor Thread
     */
    private final List<PartitionExecutorPostProcessor> processors = new ArrayList<PartitionExecutorPostProcessor>();
    private final LinkedBlockingDeque<LocalTransaction> ready_responses = new LinkedBlockingDeque<LocalTransaction>();
    
    /**
     * TODO(xin): MapReduceHelperThread
     */
    private final MapReduceHelperThread mr_helper;
    
    
    
    // ----------------------------------------------------------------------------
    // PARTITION SPECIFIC MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Collection of local partitions managed at this HStoreSite
     */
    private final ListOrderedSet<Integer> local_partitions = new ListOrderedSet<Integer>();
    
    /**
     * Integer list of all local partitions managed at this HStoreSite
     */
    private final Integer local_partitions_arr[];
    
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
    
    /**
     * PartitionId -> SiteId
     */
    private final int partition_site_xref[];
    
    /**
     * PartitionId -> Singleton set of that PartitionId
     */
    private final Collection<Integer> single_partition_sets[];
    
    /**
     * PartitionId Offset -> FastSerializer
     */
    private final FastSerializer partition_serializers[];
    
    // ----------------------------------------------------------------------------
    // TRANSACTION ESTIMATION
    // ----------------------------------------------------------------------------

    /**
     * Estimation Thresholds
     */
    private EstimationThresholds thresholds;
    
    /**
     * If we're using the TransactionEstimator, then we need to convert all primitive array ProcParameters
     * into object arrays...
     */
    private final Map<Procedure, ParameterMangler> param_manglers = new HashMap<Procedure, ParameterMangler>();
    
    /**
     * Fixed Markov Estimator
     */
    private final AbstractEstimator fixed_estimator;
    
    // ----------------------------------------------------------------------------
    // STATUS + PROFILING MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * Status Monitor
     */
    private HStoreSiteStatus status_monitor = null;
    
    /**
     * The number of incoming transaction requests per partition 
     */
    private final Histogram<Integer> incoming_partitions = new Histogram<Integer>();
    
    /**
     * How long the HStoreSite had no inflight txns
     */
    protected final ProfileMeasurement idle_time = new ProfileMeasurement("idle");
    
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
    @SuppressWarnings("unchecked")
    protected HStoreSite(Site catalog_site, HStoreConf hstore_conf) {
        assert(catalog_site != null);
        
        this.hstore_conf = hstore_conf;
        this.catalog_site = catalog_site;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog_site);
        this.site_id = this.catalog_site.getId();
        this.site_name = HStoreThreadManager.getThreadName(this.site_id, null);
        
        this.all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        final int num_partitions = this.all_partitions.size();
        this.local_partitions.addAll(CatalogUtil.getLocalPartitionIds(catalog_site));
        int num_local_partitions = this.local_partitions.size();
        
        // Get the hasher we will use for this HStoreSite
        this.hasher = ClassUtil.newInstance(hstore_conf.global.hasherClass,
                                            new Object[]{ this.catalog_db, num_partitions },
                                            new Class<?>[]{ Database.class, int.class });
        this.p_estimator = new PartitionEstimator(this.catalog_db, this.hasher);
        
        // **IMPORTANT**
        // We have to setup the partition offsets before we do anything else here
        
        this.local_partitions_arr = new Integer[num_local_partitions];
        this.executors = new PartitionExecutor[num_partitions];
        this.executor_threads = new Thread[num_partitions];
        this.single_partition_sets = new Collection[num_partitions];

        // **IMPORTANT**
        // Always clear out our various caches before we start our new HStoreSite
        if (d) LOG.debug("Preloading cached objects");
        try {
            // Don't forget our CatalogUtil friend!
            CatalogUtil.clearCache(this.catalog_db);
            CatalogUtil.preload(this.catalog_db);
            
            // Load up everything the QueryPlanUtil
            PlanNodeUtil.preload(this.catalog_db);
            
            // Then load up everything in the PartitionEstimator
            this.p_estimator.preload();
            
            // And the BatchPlanner
            BatchPlanner.clear(this.all_partitions.size());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to prepare HStoreSite", ex);
        }
        
        // Offset Hack
        this.local_partition_offsets = new int[num_partitions];
        Arrays.fill(this.local_partition_offsets, -1);
        this.local_partition_reverse = new int[num_local_partitions];
        this.partition_serializers = new FastSerializer[num_local_partitions];
        int offset = 0;
        for (int partition : this.local_partitions) {
            this.local_partition_offsets[partition] = offset;
            this.local_partition_reverse[offset] = partition; 
            this.local_partitions_arr[offset] = partition;
            this.partition_serializers[offset] = new FastSerializer(this.buffer_pool);
            this.single_partition_sets[partition] = Collections.singleton(partition);
            offset++;
        } // FOR
        this.partition_site_xref = new int[num_partitions];
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_site)) {
            this.partition_site_xref[catalog_part.getId()] = ((Site)catalog_part.getParent()).getId();
        } // FOR
        
        // Static Object Pools
        HStoreObjectPools.initialize(this);
        
        // General Stuff
        
        this.thresholds = new EstimationThresholds(); // default values
        
        // MapReduce Transaction helper thread
        if (CatalogUtil.getMapReduceProcedures(this.catalog_db).isEmpty() == false) { 
            this.mr_helper = new MapReduceHelperThread(this);
        } else {
            this.mr_helper = null;
        }
        
        // Distributed Transaction Queue Manager
        this.txnQueueManager = new TransactionQueueManager(this);
        
        // Separate TransactionIdManager per partition
        if (hstore_conf.site.txn_partition_id_managers) {
            this.txnIdManagers = new TransactionIdManager[num_partitions];
            for (int partition : this.local_partitions) {
                this.txnIdManagers[partition] = new TransactionIdManager(partition);
            } // FOR
        
        }
        // Single TransactionIdManager for the entire site
        else {
            this.txnIdManagers = new TransactionIdManager[]{
                new TransactionIdManager(this.site_id)
            };
        }
        
        // HStoreSite Thread Manager
        this.threadManager = new HStoreThreadManager(this);
        
        // Incoming Txn Request Listener
        this.voltListener = new VoltProcedureListener(this.procEventLoop, this);
        
        if (hstore_conf.site.status_show_executor_info) {
            this.idle_time.resetOnEvent(this.startWorkload_observable);
        }
        
        if (hstore_conf.site.exec_postprocessing_thread) {
            assert(hstore_conf.site.exec_postprocessing_thread_count > 0);
            if (d)
                LOG.debug(String.format("Starting %d post-processing threads", hstore_conf.site.exec_postprocessing_thread_count));
            for (int i = 0; i < hstore_conf.site.exec_postprocessing_thread_count; i++) {
                PartitionExecutorPostProcessor processor = new PartitionExecutorPostProcessor(this, this.ready_responses); 
                this.processors.add(processor);
            } // FOR
        }
        
        // Create all of our parameter manglers
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.param_manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (d) LOG.debug(String.format("Created ParameterManglers for %d procedures", this.param_manglers.size()));
        
        // HACK
        if (hstore_conf.site.exec_neworder_cheat) {
            if (catalog_db.getProcedures().containsKey("neworder")) {
                this.fixed_estimator = new TPCCEstimator(this);
            } else if (catalog_db.getProcedures().containsKey("UpdateLocation")) {
                this.fixed_estimator = new TM1Estimator(this);
            } else if (catalog_db.getProcedures().containsKey("FindOpenSeats")) {
                this.fixed_estimator = new SEATSEstimator(this);
            } else {
                this.fixed_estimator = null;
            }
        } else {
            this.fixed_estimator = null;
        }
        
        // CACHED MESSAGES
        this.REJECTION_MESSAGE = "Transaction was rejected by " + this.getSiteName();;
    }
    

    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
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
    
    public void addPartitionExecutor(int partition, PartitionExecutor executor) {
        assert(executor != null);
        this.executors[partition] = executor;
    }
    public PartitionExecutor getPartitionExecutor(int partition) {
        PartitionExecutor es = this.executors[partition]; 
        assert(es != null) : "Unexpected null PartitionExecutor for partition #" + partition + " on " + this.getSiteName();
        return (es);
    }
    public Collection<PartitionExecutorPostProcessor> getExecutionSitePostProcessors() {
        return (this.processors);
    }
    /**
     * Return a new HStoreCoordinator for this HStoreSite. Note that this
     * should only be called by HStoreSite.init(), otherwise the 
     * internal state for this HStoreSite will be incorrect. If you want
     * the HStoreCoordinator at runtime, use HStoreSite.getHStoreCoordinator()
     * @return
     */
    protected HStoreCoordinator initHStoreCoordinator() {
        return new HStoreCoordinator(this);
    }
    public HStoreCoordinator getHStoreCoordinator() {
        return (this.hstore_coordinator);
    }

    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public Map<Procedure, ParameterMangler> getParameterManglers() {
        return (this.param_manglers);
    }
    public ParameterMangler getParameterMangler(String proc_name) {
        Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null) : "Invalid Procedure name '" + proc_name + "'";
        return (this.param_manglers.get(catalog_proc));
    }
    public TransactionQueueManager getTransactionQueueManager() {
        return (this.txnQueueManager);
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
    public void setTransactionIdManagerTimeDelta(long delta) {
        for (TransactionIdManager t : this.txnIdManagers) {
            if (t != null) t.setTimeDelta(delta);
        } // FOR
    }
    
    public EstimationThresholds getThresholds() {
        return thresholds;
    }
    protected void setThresholds(EstimationThresholds thresholds) {
         this.thresholds = thresholds;
//         if (d) 
         LOG.info("Set new EstimationThresholds: " + thresholds);
    }
    
    public Database getDatabase() {
        return (this.catalog_db);
    }
    
    /**
     * Return the Site catalog object for this HStoreSiteNode
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
    
    /**
     * Return the list of all the partition ids in this H-Store database cluster
     */
    public Collection<Integer> getAllPartitionIds() {
        return (this.all_partitions);
    }
    
    /**
     * Return the list of partition ids managed by this HStoreSite 
     */
    public Collection<Integer> getLocalPartitionIds() {
        return (this.local_partitions);
    }
    /**
     * Return an immutable array of the local partition ids managed by this HStoreSite
     * Use this array is prefable to the Collection<Integer> if you must iterate of over them.
     * This avoids having to create a new Iterator instance each time.
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
    
    public int getSiteIdForPartitionId(int partition_id) {
        return (this.partition_site_xref[partition_id]);
    }
    
    @SuppressWarnings("unchecked")
    public <T extends AbstractTransaction> T getTransaction(Long txn_id) {
        return ((T)this.inflight_txns.get(txn_id));
    }
    /**
     * Get the MapReduce Helper thread 
     */
    public MapReduceHelperThread getMapReduceHelper() {
        return mr_helper;
    }
    
    /**
     * Get the total number of transactions inflight for all partitions 
     */
    protected int getInflightTxnCount() {
        return (this.inflight_txns.size());
    }
    /**
     * Get the collection of inflight Transaction state handles
     * THIS SHOULD ONLY BE USED FOR TESTING!
     * @return
     */
    protected Collection<AbstractTransaction> getInflightTransactions() {
        return (this.inflight_txns.values());
    }
    
//    /**
//     * Get the number of transactions inflight for this partition
//     */
//    protected int getInflightTxnCount(int partition) {
////        return (this.inflight_txns_ctr[partition].get());
//        return (this.txnQueueManager.getQueueSize(partition));
//    }
    
    protected int getQueuedResponseCount() {
        return (this.ready_responses.size());
    }

    /**
     * Relative marker used 
     */
    private int getNextRequestCounter() {
        return (this.request_counter.getAndIncrement());
    }
    
    // ----------------------------------------------------------------------------
    // LOCAL PARTITION OFFSETS
    // ----------------------------------------------------------------------------
    
    /**
     * 
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
     * 
     * @param offset
     * @return
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
    public EventObservable<Object> getReadyObservable() {
        return (this.ready_observable);
    }

    /**
     * Get the Observable handle for this HStore for when the first non-sysproc
     * transaction request arrives and we are technically beginning the workload
     * portion of a benchmark run.
     */
    public EventObservable<AbstractTransaction> getStartWorkloadObservable() {
        return (this.startWorkload_observable);
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
        
        EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        EventObserver<Pair<Thread, Throwable>> observer = new EventObserver<Pair<Thread, Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> arg) {
                Thread thread = arg.getFirst();
                Throwable error = arg.getSecond();
                LOG.fatal(String.format("Thread %s had a fatal error: %s", thread.getName(), (error != null ? error.getMessage() : null)));
                hstore_coordinator.shutdownClusterBlocking(error);
            }
        };
        handler.addObserver(observer);
        
        // First we need to tell the HStoreMessenger to start-up and initialize its connections
        if (d) LOG.debug("Starting HStoreCoordinator for " + this.getSiteName());
        this.hstore_coordinator.start();

        // Start TransactionQueueManager
        Thread t = new Thread(this.txnQueueManager);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(handler);
        t.start();
        
        // Start Status Monitor
        if (hstore_conf.site.status_enable) {
            assert(hstore_conf.site.status_interval >= 0);
            if (d) LOG.debug("Starting HStoreSiteStatus monitor thread");
            this.status_monitor = new HStoreSiteStatus(this, hstore_conf);
            t = new Thread(this.status_monitor);
            t.setPriority(Thread.MIN_PRIORITY);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(handler);
            t.start();
        }
        
        // Start the ExecutionSitePostProcessor
        if (hstore_conf.site.exec_postprocessing_thread) {
            for (PartitionExecutorPostProcessor espp : this.processors) {
                t = new Thread(espp);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(handler);
                t.start();    
            } // FOR
        }
        
        // Start the MapReduceHelperThread
        if (this.mr_helper != null) {
            t = new Thread(this.mr_helper);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(handler);
            t.start();
        }
        
        // Then we need to start all of the PartitionExecutor in threads
        if (d) LOG.debug("Starting PartitionExecutor threads for " + this.local_partitions_arr.length + " partitions on " + this.getSiteName());
        for (int partition : this.local_partitions_arr) {
            PartitionExecutor executor = this.getPartitionExecutor(partition);
            executor.initHStoreSite(this);
            
            t = new Thread(executor);
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY); // Probably does nothing...
            t.setUncaughtExceptionHandler(handler);
            this.executor_threads[partition] = t;
            t.start();
        } // FOR
        
        // Add in our shutdown hook
        // Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
        
        return (this);
    }
    
    /**
     * Mark this HStoreSite as ready for action!
     */
    protected synchronized HStoreSite start() {
        if (this.ready) {
            LOG.warn("Already told that we were ready... Ignoring");
            return (this);
        }
        this.shutdown_state = ShutdownState.STARTED;
        
        String msg = String.format("%s / Site=%s / Address=%s:%d / Partitions=%s",
                                   HStoreConstants.SITE_READY_MSG,
                                   this.getSiteName(),
                                   this.catalog_site.getHost().getIpaddr(),
                                   CollectionUtil.first(CatalogUtil.getExecutionSitePorts(this.catalog_site)),
                                   Arrays.toString(this.local_partitions_arr));
        // IMPORTANT: This message must always be printed in order for the BenchmarkController
        //            to know that we're ready! That's why we have to use System.out instead of LOG
        System.out.println(msg);
        this.ready = true;
        this.ready_observable.notifyObservers();
        
        return (this);
    }
    
    /**
     * Returns true if this HStoreSite is fully initialized and running
     * This will be set to false if the system is shutting down
     */
    public boolean isRunning() {
        return (this.ready);
    }

    /**
     * Returns true if this HStoreSite is throttling incoming transactions
     */
    protected Histogram<Integer> getIncomingPartitionHistogram() {
        return (this.incoming_partitions);
    }
    public ProfileMeasurement getEmptyQueueTime() {
        return (this.idle_time);
    }
    
    // ----------------------------------------------------------------------------
    // HSTORESTITE SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    /**
     * Shutdown Hook Thread
     */
//    private final class ShutdownHook implements Runnable {
//        @Override
//        public void run() {
//            // Dump out our status
//            int num_inflight = inflight_txns.size();
//            if (num_inflight > 0) {
//                System.err.println("Shutdown [" + num_inflight + " txns inflight]");
//            }
//        }
//    } // END CLASS

    @Override
    public void prepareShutdown(boolean error) {
        this.shutdown_state = ShutdownState.PREPARE_SHUTDOWN;
        if (this.hstore_coordinator != null)
            this.hstore_coordinator.prepareShutdown(false);
        for (PartitionExecutorPostProcessor espp : this.processors) {
            espp.prepareShutdown(false);
        } // FOR
        
        if (this.mr_helper != null)
            this.mr_helper.prepareShutdown(error);
        
        for (int p : this.local_partitions_arr) {
            if (this.executors[p] != null) 
                this.executors[p].prepareShutdown(error);
        } // FOR
    }
    
    /**
     * Perform shutdown operations for this HStoreSiteNode
     * This should only be called by HStoreMessenger 
     */
    @Override
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
            if (d) LOG.debug("Already told to shutdown... Ignoring");
            return;
        }
        if (this.shutdown_state != ShutdownState.PREPARE_SHUTDOWN) this.prepareShutdown(false);
        this.shutdown_state = ShutdownState.SHUTDOWN;
//      if (d)
        LOG.info("Shutting down everything at " + this.getSiteName());

        // Stop the monitor thread
        if (this.status_monitor != null) this.status_monitor.shutdown();
        
        // Kill the queue manager
        this.txnQueueManager.shutdown();
        
        // Tell our local boys to go down too
        for (PartitionExecutorPostProcessor p : this.processors) {
            p.shutdown();
        }
        // Tell the MapReduceHelperThread to shutdown too
        if (this.mr_helper != null) this.mr_helper.shutdown();
        
        for (int p : this.local_partitions_arr) {
            if (t) LOG.trace("Telling the PartitionExecutor for partition " + p + " to shutdown");
            this.executors[p].shutdown();
        } // FOR
      
        // Tell anybody that wants to know that we're going down
        if (t) LOG.trace("Notifying " + this.shutdown_observable.countObservers() + " observers that we're shutting down");
        this.shutdown_observable.notifyObservers();
        
        // Tell all of our event loops to stop
        if (t) LOG.trace("Telling Procedure Listener event loops to exit");
        this.procEventLoop.exitLoop();
        if (this.voltListener != null) this.voltListener.close();
        
        if (this.hstore_coordinator != null){
        // Tell the ClusterReorganizer to shutdown too
            if(this.hstore_coordinator.getClusterReorganizer() != null){
                this.hstore_coordinator.getClusterReorganizer().shutdown();
            }
            this.hstore_coordinator.shutdown();
        }
        LOG.info(String.format("Completed shutdown process at %s [hashCode=%d]", this.getSiteName(), this.hashCode()));
    }
    
    /**
     * Returns true if HStoreSite is in the process of shutting down
     * @return
     */
    @Override
    public boolean isShuttingDown() {
        return (this.shutdown_state == ShutdownState.SHUTDOWN || this.shutdown_state == ShutdownState.PREPARE_SHUTDOWN);
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void procedureInvocation(StoredProcedureInvocation request, byte[] serializedRequest, RpcCallback<byte[]> done) {
        long timestamp = (hstore_conf.site.txn_profiling ? ProfileMeasurement.getTime() : -1);
        
        // Extract the stuff we need to figure out whether this guy belongs at our site
        request.buildParameterSet();
        assert(request.getParams() != null) :
            "The parameters object is null for new txn from client #" + request.getClientHandle();
        final Object args[] = request.getParams().toArray(); 
        Procedure catalog_proc = this.catalog_db.getProcedures().get(request.getProcName());
        if (catalog_proc == null) {
            catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(request.getProcName());
        }
        if (catalog_proc == null) throw new RuntimeException("Unknown procedure '" + request.getProcName() + "'");
        final boolean sysproc = request.isSysProc();
        int base_partition = request.getBasePartition();
        if (d) LOG.debug(String.format("Received new stored procedure invocation request for %s [handle=%d]", catalog_proc.getName(), request.getClientHandle()));

        // Profiling Updates
        if (hstore_conf.site.status_show_txn_info) TxnCounter.RECEIVED.inc(request.getProcName());
        if (hstore_conf.site.exec_profiling && base_partition != -1) {
            this.incoming_partitions.put(base_partition);
        }
        
        // -------------------------------
        // BASE PARTITION
        // -------------------------------
        
        // DB2-style Transaction Redirection
        if (base_partition != -1 || hstore_conf.site.exec_db2_redirects) {
            if (d) LOG.debug(String.format("Using embedded base partition from %s request", request.getProcName()));
            assert(base_partition == request.getBasePartition());    
        }
        // If it's a sysproc, then it doesn't need to go to a specific partition
        else if (sysproc) {
            // For now we'll just run the sysproc on a random partition at this site
            
            // HACK: Check if we should shutdown. This allows us to kill things even if the
            // DTXN coordinator is stuck.
            // TODO: Execute as a regular transaction
            if (catalog_proc.getName().equalsIgnoreCase("@Shutdown")) {
                ClientResponseImpl cresponse = new ClientResponseImpl(1, 1, 1, Status.OK, HStoreConstants.EMPTY_RESULT, "");
                FastSerializer fs = new FastSerializer();
                try {
                    fs.writeObject(cresponse);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                done.run(fs.getBytes());

                // Non-blocking....
                Exception error = new Exception("Shutdown command received at " + this.getSiteName());
                this.hstore_coordinator.shutdownCluster(error);
                return;
            }
        }
        // Otherwise we use the PartitionEstimator to figure out where this thing needs to go
        else if (hstore_conf.site.exec_force_localexecution == false) {
            if (d) LOG.debug(String.format("Using PartitionEstimator for %s request", request.getProcName()));
            try {
                Integer p = this.p_estimator.getBasePartition(catalog_proc, args, false);
                if (p != null) base_partition = p.intValue(); 
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // If we don't have a partition to send this transaction to, then we will just pick
        // one our partitions at random. This can happen if we're forcing txns to execute locally
        // or if there are no input parameters <-- this should be in the paper!!!
        if (base_partition == -1) {
            if (t) 
                LOG.trace(String.format("Selecting a random local partition to execute %s request [force_local=%s]",
                                        request.getProcName(), hstore_conf.site.exec_force_localexecution));
            int idx = (int)(Math.abs(request.getClientHandle()) % this.local_partitions_arr.length);
            base_partition = this.local_partitions_arr[idx].intValue();
        }
        
        if (d) LOG.debug(String.format("Incoming %s transaction request [handle=%d, partition=%d]",
                                       request.getProcName(), request.getClientHandle(), base_partition));
        
        // -------------------------------
        // REDIRECT TXN TO PROPER BASE PARTITION
        // If the base_partition isn't local, then we need to ship it off to the right site
        // -------------------------------
        if (this.isLocalPartition(base_partition) == false) {
            assert(request.hasBasePartition() == false) : 
                "Trying to redirect " + catalog_proc.getName() + " transaction more than once!";
            this.transactionRedirect(catalog_proc, serializedRequest, base_partition, done);
            return;
        }
        
        // Grab a new LocalTransactionState object from the target base partition's PartitionExecutor object pool
        // This will be the handle that is used all throughout this txn's lifespan to keep track of what it does
        Long txn_id = this.getTransactionIdManager(base_partition).getNextUniqueTransactionId();
        LocalTransaction ts = null;
        try {
            if (catalog_proc.getMapreduce()) {
                ts = HStoreObjectPools.STATES_TXN_MAPREDUCE.borrowObject();
            } else {
                ts = HStoreObjectPools.STATES_TXN_LOCAL.borrowObject();
            }
            assert (ts.isInitialized() == false);
        } catch (Throwable ex) {
            LOG.fatal(String.format("Failed to instantiate new LocalTransactionState for %s txn #%s",
                                    request.getProcName(), txn_id));
            throw new RuntimeException(ex);
        }
        
        // Disable transaction profiling for sysprocs
        if (hstore_conf.site.txn_profiling && sysproc) {
            ts.profiler.disableProfiling();
        }
        
        // -------------------------------
        // TRANSACTION EXECUTION PROPERTIES
        // -------------------------------
        
        boolean predict_abortable = (hstore_conf.site.exec_no_undo_logging_all == false);
        boolean predict_readOnly = catalog_proc.getReadonly();
        Collection<Integer> predict_touchedPartitions = null;
        TransactionEstimator.State t_state = null; 
        
        // Sysprocs can be either all partitions or single-partitioned
        if (sysproc) {
            // TODO: It would be nice if the client could pass us a hint when loading the tables
            // It would be just for the loading, and not regular transactions
            if (catalog_proc.getSinglepartition()) {
                predict_touchedPartitions = this.single_partition_sets[base_partition];
            } else {
                predict_touchedPartitions = this.all_partitions;
            }
        }
        // MapReduceTransactions always need all partitions
        else if (catalog_proc.getMapreduce()) {
            if (t) LOG.trace(String.format("New request is for MapReduce %s, so it has to be multi-partitioned [clientHandle=%d]",
                                           request.getProcName(), request.getClientHandle()));
            predict_touchedPartitions = this.all_partitions;
        }
        // Force all transactions to be single-partitioned
        else if (hstore_conf.site.exec_force_singlepartitioned) {
            if (t) LOG.trace(String.format("The \"Always Single-Partitioned\" flag is true. Marking new %s transaction as single-partitioned on partition %d [clientHandle=%d]",
                             request.getProcName(), base_partition, request.getClientHandle()));
            predict_touchedPartitions = this.single_partition_sets[base_partition];
        }    
        // Assume we're executing TPC-C neworder. Manually examine the input parameters and figure
        // out what partitions it's going to need to touch
        else if (hstore_conf.site.exec_neworder_cheat) {
            if (t) LOG.trace(String.format("Using fixed transaction estimator [clientHandle=%d]", request.getClientHandle()));
            if (this.fixed_estimator != null)
                predict_touchedPartitions = this.fixed_estimator.initializeTransaction(catalog_proc, args);
            if (predict_touchedPartitions == null)
                predict_touchedPartitions = this.single_partition_sets[base_partition];
        }    
        // Otherwise, we'll try to estimate what the transaction will do (if we can)
        else {
            if (d) LOG.debug(String.format("Using TransactionEstimator to check whether new %s request is single-partitioned [clientHandle=%d]",
                                           request.getProcName(), request.getClientHandle()));
            
            // Grab the TransactionEstimator for the destination partition and figure out whether
            // this mofo is likely to be single-partition or not. Anything that we can't estimate
            // will just have to be multi-partitioned. This includes sysprocs
            TransactionEstimator t_estimator = this.executors[base_partition].getTransactionEstimator();
            
            try {
                // HACK: Convert the array parameters to object arrays...
                Object cast_args[] = this.param_manglers.get(catalog_proc).convert(args);
                if (t) LOG.trace(String.format("Txn #%d Parameters:\n%s", txn_id, this.param_manglers.get(catalog_proc).toString(cast_args)));
                
                if (hstore_conf.site.txn_profiling) ts.profiler.startInitEstimation();
                t_state = t_estimator.startTransaction(txn_id, base_partition, catalog_proc, cast_args);
                
                // If there is no TransactinEstimator.State, then there is nothing we can do
                // It has to be executed as multi-partitioned
                if (t_state == null) {
                    if (d) LOG.debug(String.format("No TransactionEstimator.State was returned for %s. Executing as multi-partitioned",
                                                            AbstractTransaction.formatTxnName(catalog_proc, txn_id))); 
                    predict_touchedPartitions = this.all_partitions;
                    
                // We have a TransactionEstimator.State, so let's see what it says...
                } else {
                    if (t) LOG.trace("\n" + StringUtil.box(t_state.toString()));
                    MarkovEstimate m_estimate = t_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a MarkovEstimate for some reason...
                    if (m_estimate == null) {
                        if (d) LOG.debug(String.format("No MarkovEstimate was found for %s. Executing as multi-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                        predict_touchedPartitions = this.all_partitions;
                        
                    // Invalid MarkovEstimate. Stick with defaults
                    } else if (m_estimate.isValid() == false) {
                        if (d) LOG.warn(String.format("Invalid MarkovEstimate for %s. Marking as not read-only and multi-partitioned.\n%s",
                                AbstractTransaction.formatTxnName(catalog_proc, txn_id), m_estimate));
                        predict_readOnly = catalog_proc.getReadonly();
                        predict_abortable = true;
                        predict_touchedPartitions = this.all_partitions;
                        
                    // Use MarkovEstimate to determine things
                    } else {
                        if (d) {
                            LOG.debug(String.format("Using MarkovEstimate for %s to determine if single-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                            LOG.debug(String.format("%s MarkovEstimate:\n%s", AbstractTransaction.formatTxnName(catalog_proc, txn_id), m_estimate));
                        }
                        predict_touchedPartitions = m_estimate.getTouchedPartitions(this.thresholds);
                        predict_readOnly = m_estimate.isReadOnlyAllPartitions(this.thresholds);
                        predict_abortable = (predict_touchedPartitions.size() == 1 || m_estimate.isAbortable(this.thresholds)); // || predict_readOnly == false
                        
                    }
                }
            } catch (Throwable ex) {
                if (t_state != null) {
                    MarkovGraph markov = t_state.getMarkovGraph();
                    GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(t_state.getInitialPath()));
                    gv.highlightPath(markov.getPath(t_state.getActualPath()), "blue");
                    LOG.warn("WROTE MARKOVGRAPH: " + gv.writeToTempFile(catalog_proc));
                }
                LOG.error(String.format("Failed calculate estimate for %s request", AbstractTransaction.formatTxnName(catalog_proc, txn_id)), ex);
                predict_touchedPartitions = this.all_partitions;
                predict_readOnly = false;
                predict_abortable = true;
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopInitEstimation();
            }
        }
        
        if (catalog_proc.getMapreduce()) {
            ((MapReduceTransaction)ts).init(
                    txn_id, request.getClientHandle(), base_partition,
                    predict_touchedPartitions, predict_readOnly, predict_abortable,
                    catalog_proc, request, done);
        } else {
            ts.init(
                    txn_id, request.getClientHandle(), base_partition,
                    predict_touchedPartitions, predict_readOnly, predict_abortable,
                    catalog_proc, request, done);
        }
        if (t_state != null) ts.setEstimatorState(t_state);
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startTransaction(timestamp);
        if (d) {
            LOG.debug(String.format("Initializing %s on partition %d [clientHandle=%d, partitions=%s, readOnly=%s, abortable=%s]",
                      ts, base_partition,
                      request.getClientHandle(),
                      predict_touchedPartitions, predict_readOnly, predict_abortable));
        }
        
        // If this is the first non-sysproc transaction that we've seen, then
        // we will notify anybody that is waiting for this event. This is used to clear
        // out any counters or profiling information that got recorded when we were loading data
        if (this.startWorkload == false && sysproc == false) {
            synchronized (this) {
                if (this.startWorkload == false) {
                    this.startWorkload = true;
                    this.startWorkload_observable.notifyObservers(ts);
                }
            } // SYNCH
        }
        this.dispatchInvocation(ts);
        
        if (d) LOG.debug("Finished initial processing of new txn #" + txn_id + ". Returning back to listen on incoming socket");
    }

    /**
     * 
     * @param ts
     */
    private void dispatchInvocation(LocalTransaction ts) {
        assert(ts.isInitialized()) : 
            "Unexpected uninitialized LocalTranaction for " + ts;
        Long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
                
        // For some odd reason we sometimes get duplicate transaction ids from the VoltDB id generator
        // So we'll just double check to make sure that it's unique, and if not, we'll just ask for a new one
        LocalTransaction dupe = (LocalTransaction)this.inflight_txns.put(txn_id, ts);
        if (dupe != null) {
            // HACK!
            this.inflight_txns.put(txn_id, dupe);
            // long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
            Long new_txn_id = this.getTransactionIdManager(base_partition).getNextUniqueTransactionId();
            if (new_txn_id == txn_id) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("ORIG TRANSACTION:\n" + dupe);
                LOG.fatal("NEW TRANSACTION:\n" + ts);
                Exception error = new Exception(msg);
                this.hstore_coordinator.shutdownClusterBlocking(error);
            }
            LOG.warn(String.format("Had to fix duplicate txn ids: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            ts.setTransactionId(txn_id);
            this.inflight_txns.put(txn_id, ts);
        }
        if (d) LOG.debug(ts + " - Dispatching new transaction invocation");
        
        // -------------------------------
        // SINGLE-PARTITION TRANSACTION
        // -------------------------------
        if (ts.isPredictSinglePartition()) {
            if (d) LOG.debug(String.format("%s - Fast path single-partition execution on partition %d [handle=%d]",
                             ts, base_partition, ts.getClientHandle()));
            this.transactionStart(ts, base_partition);
        }
        // -------------------------------    
        // DISTRIBUTED TRANSACTION
        // -------------------------------
        else {
            if (d) LOG.debug(String.format("%s - Queuing distributed transaction to execute at partition %d [handle=%d]",
                             ts, base_partition, ts.getClientHandle()));
            
            // Partitions
            // Figure out what partitions we plan on touching for this transaction
            Collection<Integer> predict_touchedPartitions = ts.getPredictTouchedPartitions();
            
            if (ts.isMapReduce() == false) {
                // TransactionEstimator
                // If we know we're single-partitioned, then we *don't* want to tell the Dtxn.Coordinator
                // that we're done at any partitions because it will throw an error
                // Instead, if we're not single-partitioned then that's that only time that 
                // we Tell the Dtxn.Coordinator that we are finished with partitions if we have an estimate
                TransactionEstimator.State s = ts.getEstimatorState(); 
                if (s != null && s.getInitialEstimate() != null) {
                    MarkovEstimate est = s.getInitialEstimate();
                    assert(est != null);
                    predict_touchedPartitions.addAll(est.getTouchedPartitions(this.thresholds));
                }
                assert(predict_touchedPartitions.isEmpty() == false) : 
                    "Trying to mark " + ts + " as done at EVERY partition!\n" + ts.debug();
            }

            // Check whether our transaction can't run right now because its id is less than
            // the last seen txnid from the remote partitions that it wants to touch
            for (int partition : predict_touchedPartitions) {
                Long last_txn_id = this.txnQueueManager.getLastLockTransaction(partition); 
                if (txn_id.compareTo(last_txn_id) < 0) {
                    // If we catch it here, then we can just block ourselves until
                    // we generate a txn_id with a greater value and then re-add ourselves
                    if (d) {
                        LOG.warn(String.format("%s - Unable to queue transaction because the last txn id at partition %d is %d. Restarting...",
                                       ts, partition, last_txn_id));
                        LOG.warn(String.format("LastTxnId:#%s / NewTxnId:#%s",
                                           TransactionIdManager.toString(last_txn_id),
                                           TransactionIdManager.toString(txn_id)));
                    }
                    if (hstore_conf.site.status_show_txn_info && ts.getRestartCounter() == 1) TxnCounter.BLOCKED_LOCAL.inc(ts.getProcedure());
                    this.txnQueueManager.blockTransaction(ts, partition, last_txn_id);
                    return;
                }
            } // FOR
            
            // This callback prevents us from making additional requests to the Dtxn.Coordinator until
            // we get hear back about our our initialization request
            if (hstore_conf.site.txn_profiling) ts.profiler.startInitDtxn();
            this.txnQueueManager.initTransaction(ts);
        }
    }

    // ----------------------------------------------------------------------------
    // TRANSACTION HANDLE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Create a MapReduceTransaction handle. This should only be invoked on a remote site.
     * @param txn_id
     * @param invocation
     * @param base_partition
     * @return
     */
    public MapReduceTransaction createMapReduceTransaction(Long txn_id, StoredProcedureInvocation invocation, int base_partition) {
        String proc_name = invocation.getProcName();
        Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(proc_name);
        if (catalog_proc == null) throw new RuntimeException("Unknown procedure '" + proc_name + "'");
        
        MapReduceTransaction ts = null;
        try {
            ts = HStoreObjectPools.STATES_TXN_MAPREDUCE.borrowObject();
            assert(ts.isInitialized() == false);
        } catch (Throwable ex) {
            LOG.fatal(String.format("Failed to instantiate new MapReduceTransaction state for %s txn #%s",
                                    proc_name, txn_id));
            throw new RuntimeException(ex);
        }
        // We should never already have a transaction handle for this txnId
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
        assert(dupe == null) : "Trying to create multiple transaction handles for " + dupe;

        ts.init(txn_id, base_partition, catalog_proc, invocation);
        if (d) LOG.debug(String.format("Created new MapReduceTransaction state %s from remote partition %d",
                                       ts, base_partition));
        return (ts);
    }
    
    /**
     * Create a RemoteTransaction handle. This obviously only for a remote site.
     * @param txn_id
     * @param request
     * @return
     */
    public RemoteTransaction createRemoteTransaction(Long txn_id, TransactionWorkRequest request) {
        RemoteTransaction ts = null;
        try {
            // Remote Transaction
            ts = HStoreObjectPools.STATES_TXN_REMOTE.borrowObject();
            ts.init(txn_id, request.getSourcePartition(), request.getSysproc(), true);
            if (d) LOG.debug(String.format("Creating new RemoteTransactionState %s from remote partition %d [singlePartitioned=%s, hashCode=%d]",
                                           ts, request.getSourcePartition(), false, ts.hashCode()));
        } catch (Exception ex) {
            LOG.fatal("Failed to construct TransactionState for txn #" + txn_id, ex);
            throw new RuntimeException(ex);
        }
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
        assert(dupe == null) : "Trying to create multiple transaction handles for " + dupe;
        
        if (t) LOG.trace(String.format("Stored new transaction state for %s", ts));
        return (ts);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION OPERATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Add the given transaction id to this site's queue manager
     * @param txn_id
     * @param partitions The list of partitions that this transaction needs to access
     * @param callback
     */
    public void transactionInit(Long txn_id, Collection<Integer> partitions, TransactionInitQueueCallback callback) {
        // We should always force a txn from a remote partition into the queue manager
        this.txnQueueManager.lockInsert(txn_id, partitions, callback);
    }

    /**
     * This function can really block transaction executing on that partition
     * IMPORTANT: The transaction could be deleted after calling this if it is rejected
     * @param ts, base_partition
     */
    public void transactionStart(LocalTransaction ts, int base_partition) {
        Long txn_id = ts.getTransactionId();
        //int base_partition = ts.getBasePartition();
        Procedure catalog_proc = ts.getProcedure();
        if (d) LOG.debug(String.format("Starting %s %s on partition %d",
                        (ts.isPredictSinglePartition() ? "single-partition" : "distributed"), ts, base_partition));
        
        PartitionExecutor executor = this.executors[base_partition];
        assert(executor != null) :
            "Unable to start " + ts + " - No PartitionExecutor exists for partition #" + base_partition + " at HStoreSite " + this.site_id;
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startQueue();
        boolean ret = executor.queueNewTransaction(ts);
        if (hstore_conf.site.status_show_txn_info && ret) {
            assert(catalog_proc != null) :
                String.format("Null Procedure for txn #%d [hashCode=%d]", txn_id, ts.hashCode());
            TxnCounter.EXECUTED.inc(catalog_proc);
        }
    }
    
    /**
     * Execute some work on a particular PartitionExecutor
     * @param request
     * @param done
     */
    public void transactionWork(RemoteTransaction ts, TransactionWorkRequest request, WorkFragment fragment) {
        if (d) LOG.debug(String.format("Queuing FragmentTaskMessage on partition %d for txn #%d",
                                                fragment.getPartitionId(), ts.getTransactionId()));
        int partition = fragment.getPartitionId();
        FragmentTaskMessage ftask = ts.getFragmentTaskMessage(fragment);
        this.executors[partition].queueWork(ts, ftask);
    }


    /**
     * This method is the first part of two phase commit for a transaction.
     * If speculative execution is enabled, then we'll notify each the PartitionExecutors
     * for the listed partitions that it is done. This will cause all the 
     * that are blocked on this transaction to be released immediately and queued 
     * @param txn_id
     * @param partitions
     * @param updated
     */
    public void transactionPrepare(Long txn_id, Collection<Integer> partitions, Collection<Integer> updated) {
        if (d) LOG.debug(String.format("2PC:PREPARE Txn #%d [partitions=%s]", txn_id, partitions));
        
        // We could have been asked to participate in a distributed transaction but
        // they never actually sent us anything, so we should just tell the queue manager
        // that the txn is done. There is nothing that we need to do at the PartitionExecutors
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        TransactionPrepareCallback callback = null;
        if (ts instanceof LocalTransaction) {
            callback = ((LocalTransaction)ts).getTransactionPrepareCallback();
        }
        
        int spec_cnt = 0;
        for (Integer p : partitions) {
            if (this.local_partition_offsets[p.intValue()] == -1) continue;
            
            // Always tell the queue stuff that the transaction is finished at this partition
            if (d) LOG.debug(String.format("Telling queue manager that txn #%d is finished at partition %d", txn_id, p));
            this.txnQueueManager.lockFinished(txn_id, Status.OK, p.intValue());
            
            // If speculative execution is enabled, then we'll turn it on at the PartitionExecutor
            // for this partition
            if (ts != null && hstore_conf.site.exec_speculative_execution) {
                if (d) LOG.debug(String.format("Telling partition %d to enable speculative execution because of txn #%d", p, txn_id));
                boolean ret = this.executors[p.intValue()].enableSpeculativeExecution(ts, false);
                if (d && ret) {
                    spec_cnt++;
                    LOG.debug(String.format("Partition %d - Speculative Execution!", p));
                }
            }
            if (updated != null) updated.add(p);
            if (callback != null) callback.decrementCounter(1);

        } // FOR
        if (d && spec_cnt > 0)
            LOG.debug(String.format("Enabled speculative execution at %d partitions because of waiting for txn #%d", spec_cnt, txn_id));
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
    public void transactionFinish(Long txn_id, Status status, Collection<Integer> partitions) {
        if (d) LOG.debug(String.format("2PC:FINISH Txn #%d [commitStatus=%s, partitions=%s]",
                                       txn_id, status, partitions));
        boolean commit = (status == Status.OK);
        
        // If we don't have a AbstractTransaction handle, then we know that we never did anything
        // for this transaction and we can just ignore this finish request. We do have to tell
        // the TransactionQueue manager that we're done though
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        TransactionFinishCallback finish_callback = null;
        TransactionCleanupCallback cleanup_callback = null;
        if (ts != null) {
            if (ts instanceof RemoteTransaction || ts instanceof MapReduceTransaction) {
                if (d) LOG.debug(ts + " - Initialzing the TransactionCleanupCallback");
                cleanup_callback = ts.getCleanupCallback();
                assert(cleanup_callback != null);
                cleanup_callback.init(ts, status, partitions);
            } else {
                finish_callback = ((LocalTransaction)ts).getTransactionFinishCallback();
                assert(finish_callback != null);
            }
        }
        
        for (int p : partitions) {
            if (this.isLocalPartition(p) == false) {
                if (t) LOG.trace(String.format("#%d - Skipping finish at partition %d", txn_id, p));
                continue;
            }
            if (t) LOG.trace(String.format("#%d - Invoking finish at partition %d", txn_id, p));
            
            // We only need to tell the queue stuff that the transaction is finished
            // if it's not a commit because there won't be a 2PC:PREPARE message
            if (commit == false) this.txnQueueManager.lockFinished(txn_id, status, p);

            // Then actually commit the transaction in the execution engine
            // We only need to do this for distributed transactions, because all single-partition
            // transactions will commit/abort immediately
            if (ts != null && ts.isPredictSinglePartition() == false && ts.hasStarted(p)) {
                if (d) LOG.debug(String.format("%s - Calling finishTransaction on partition %d", ts, p));
                try {
                    this.executors[p].queueFinish(ts, status);
                } catch (Throwable ex) {
                    LOG.error(String.format("Unexpected error when trying to finish %s\nHashCode: %d / Status: %s / Partitions: %s",
                                            ts, ts.hashCode(), status, partitions));
                    throw new RuntimeException(ex);
                }
            }
            // If this is a LocalTransaction, then we want to just decrement their TransactionFinishCallback counter
            else if (finish_callback != null) {
                if (d) LOG.debug(String.format("%s - Notifying %s that the txn is finished at partition %d",
                                               ts, finish_callback.getClass().getSimpleName(), p));
                finish_callback.decrementCounter(1);
            }
            // If we didn't queue the transaction to be finished at this partition, then we need to make sure
            // that we mark the transaction as finished for this callback
            else if (cleanup_callback != null) {
                if (d) LOG.debug(String.format("%s - Notifying %s that the txn is finished at partition %d",
                                               ts, cleanup_callback.getClass().getSimpleName(), p));
                cleanup_callback.run(p);
            }
        } // FOR            
    }

    // ----------------------------------------------------------------------------
    // FAILED TRANSACTIONS (REQUEUE / REJECT / RESTART)
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param catalog_proc
     * @param serializedRequest
     * @param base_partition
     * @param done
     */
    public void transactionRedirect(Procedure catalog_proc, byte serializedRequest[], int base_partition, RpcCallback<byte[]> done) {
        if (d) LOG.debug(String.format("Forwarding %s request to partition %d", catalog_proc.getName(), base_partition));
        
        // Make a wrapper for the original callback so that when the result comes back frm the remote partition
        // we will just forward it back to the client. How sweet is that??
        TransactionRedirectCallback callback = null;
        try {
            callback = (TransactionRedirectCallback)HStoreObjectPools.CALLBACKS_TXN_REDIRECT_REQUEST.borrowObject();
            callback.init(done);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get ForwardTxnRequestCallback", ex);
        }
        
        // Mark this request as having been redirected
        StoredProcedureInvocation.markRawBytesAsRedirected(base_partition, serializedRequest);
        
        this.hstore_coordinator.transactionRedirect(serializedRequest, callback, base_partition);
        if (hstore_conf.site.status_show_txn_info) TxnCounter.REDIRECTED.inc(catalog_proc);
    }
    
    /**
     * A non-blocking method for requeuing an aborted transaction using the
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
        
        ClientResponseImpl cresponse = ts.getClientResponse();
        cresponse.init(ts.getTransactionId(),
                       ts.getClientHandle(),
                       ts.getBasePartition(),
                       status,
                       HStoreConstants.EMPTY_RESULT,
                       this.REJECTION_MESSAGE,
                       ts.getPendingError());
        this.sendClientResponse(ts, cresponse);

        if (hstore_conf.site.status_show_txn_info) {
            if (status == Status.ABORT_THROTTLED) {
                TxnCounter.THROTTLED.inc(ts.getProcedure());
            } else if (status == Status.ABORT_REJECT) {
                TxnCounter.REJECTED.inc(ts.getProcedure());
            } else {
                assert(false) : "Unexpected " + ts + ": " + status;
            }
        }
    }

    /**
     * Restart the given transaction with a brand new transaction handle.
     * This method will perform the following operations:
     *  (1) Restart the transaction as new multi-partitioned transaction
     *  (2) Mark the original transaction as aborted
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
        if (d) LOG.debug(String.format("%s got hit with a %s! Going to clean-up our mess and re-execute [restarts=%d]",
                                   orig_ts , status, orig_ts.getRestartCounter()));
        int base_partition = orig_ts.getBasePartition();
        StoredProcedureInvocation spi = orig_ts.getInvocation();
        assert(spi != null) : "Missing StoredProcedureInvocation for " + orig_ts;
        
        // If this txn has been restarted too many times, then we'll just give up
        // and reject it outright
        int restart_limit = (orig_ts.isSysProc() ? hstore_conf.site.txn_restart_limit_sysproc :
                                                   hstore_conf.site.txn_restart_limit);
        if (orig_ts.getRestartCounter() > restart_limit) {
            if (orig_ts.isSysProc()) {
                throw new RuntimeException(String.format("%s has been restarted %d times! Rejecting...",
                                                         orig_ts, orig_ts.getRestartCounter()));
            } else {
                this.transactionReject(orig_ts, Status.ABORT_REJECT);
                return (Status.ABORT_REJECT);
            }
        }
        
        // Figure out whether this transaction should be redirected based on what partitions it
        // tried to touch before it was aborted 
        if (status != Status.ABORT_RESTART && hstore_conf.site.exec_db2_redirects) {
            Histogram<Integer> touched = orig_ts.getTouchedPartitions();
            Collection<Integer> most_touched = touched.getMaxCountValues();
            assert(most_touched != null);
            
            // HACK: We should probably decrement the base partition by one 
            // so that we only consider where they actually executed queries
            
            if (d) LOG.debug(String.format("Touched partitions for mispredicted %s\n%s",
                                           orig_ts, touched));
            Integer redirect_partition = null;
            if (most_touched.size() == 1) {
                redirect_partition = CollectionUtil.first(most_touched);
            } else if (most_touched.isEmpty() == false) {
                redirect_partition = CollectionUtil.random(most_touched);
            } else {
                redirect_partition = CollectionUtil.random(this.all_partitions);
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
            if (this.isLocalPartition(redirect_partition.intValue()) == false && spi.hasBasePartition() == false) {
                if (d) LOG.debug(String.format("%s - Redirecting to partition %d because of misprediction",
                                               orig_ts, redirect_partition));
                
                spi.setBasePartition(redirect_partition.intValue());
                
                // Add all the partitions that the txn touched before it got aborted
                spi.addPartitions(touched.values());
                
                byte serializedRequest[] = null;
                try {
                    serializedRequest = FastSerializer.serialize(spi);
                } catch (IOException ex) {
                    throw new RuntimeException("Failed to serialize StoredProcedureInvocation to redirect %s" + orig_ts, ex);
                }
                assert(serializedRequest != null);
                
                TransactionRedirectCallback callback;
                try {
                    callback = (TransactionRedirectCallback)HStoreObjectPools.CALLBACKS_TXN_REDIRECT_REQUEST.borrowObject();
                    callback.init(orig_ts.getClientCallback());
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to get ForwardTxnRequestCallback", ex);   
                }
                this.hstore_coordinator.transactionRedirect(serializedRequest, callback, redirect_partition);
                if (hstore_conf.site.status_show_txn_info) TxnCounter.REDIRECTED.inc(orig_ts.getProcedure());
                return (Status.ABORT_RESTART);
                
            // Allow local redirect
            } else if (orig_ts.getRestartCounter() <= 1 || spi.hasBasePartition() == false) {
                if (redirect_partition.intValue() != base_partition && this.isLocalPartition(redirect_partition.intValue())) {
                    if (d) LOG.debug(String.format("Redirecting %s to local partition %d. " +
                                                    "[restartCtr=%d]\n%s",
                                                    orig_ts, redirect_partition, orig_ts.getRestartCounter(), touched));
                    base_partition = redirect_partition.intValue();
                    spi.setBasePartition(base_partition);
                }
            } else {
                if (d) LOG.debug(String.format("Mispredicted %s has already been aborted once before. " +
                                               "Restarting as all-partition txn [restartCtr=%d, redirectPartition=%d]\n%s",
                                               orig_ts, orig_ts.getRestartCounter(), redirect_partition, touched));
                touched.putAll(this.local_partitions);
            }
        }

        Long new_txn_id = this.getTransactionIdManager(base_partition).getNextUniqueTransactionId();
        LocalTransaction new_ts = null;
        try {
            new_ts = HStoreObjectPools.STATES_TXN_LOCAL.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for mispredicted " + orig_ts);
            throw new RuntimeException(ex);
        }
        
        // Restart the new transaction
        if (hstore_conf.site.txn_profiling) new_ts.profiler.startTransaction(ProfileMeasurement.getTime());
        
        boolean malloc = false;
        Collection<Integer> predict_touchedPartitions = null;
        if (status == Status.ABORT_RESTART) {
            predict_touchedPartitions = orig_ts.getPredictTouchedPartitions();
        } else if (orig_ts.getRestartCounter() == 0) {
            // HACK: Ignore ConcurrentModificationException
            predict_touchedPartitions = new HashSet<Integer>();
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
            predict_touchedPartitions = this.all_partitions;
        }
        
        if (status == Status.ABORT_MISPREDICT && orig_ts.getPendingError() instanceof MispredictionException) {
            MispredictionException ex = (MispredictionException)orig_ts.getPendingError();
            Collection<Integer> partitions = ex.getPartitions().values();
            if (predict_touchedPartitions.containsAll(partitions) == false) {
                if (malloc == false) {
                    predict_touchedPartitions = new HashSet<Integer>(predict_touchedPartitions);
                    malloc = true;
                }
                predict_touchedPartitions.addAll(partitions);
            }
            if (d) LOG.debug(orig_ts + " Mispredicted Partitions: " + partitions);
        }
        
        if (predict_touchedPartitions.contains(base_partition) == false) {
            if (malloc == false) {
                predict_touchedPartitions = new HashSet<Integer>(predict_touchedPartitions);
                malloc = true;
            }
            predict_touchedPartitions.add(base_partition);
        }
        
        if (predict_touchedPartitions.isEmpty()) predict_touchedPartitions = this.all_partitions;
        boolean predict_readOnly = orig_ts.getProcedure().getReadonly(); // FIXME
        boolean predict_abortable = true; // FIXME
        new_ts.init(new_txn_id,
                    orig_ts.getClientHandle(),
                    base_partition,
                    predict_touchedPartitions,
                    predict_readOnly,
                    predict_abortable,
                    orig_ts.getProcedure(),
                    orig_ts.getInvocation(),
                    orig_ts.getClientCallback()
        );
        new_ts.setRestartCounter(orig_ts.getRestartCounter() + 1);
        
         if (d) {
            LOG.debug(String.format("Re-executing %s as new %s-partition %s on partition %d [restarts=%d, partitions=%s]",
                                    orig_ts,
                                    (predict_touchedPartitions.size() == 1 ? "single" : "multi"),
                                    new_ts,
                                    base_partition,
                                    new_ts.getRestartCounter(),
                                    predict_touchedPartitions));
            if (t && status == Status.ABORT_MISPREDICT)
                LOG.trace(String.format("%s Mispredicted partitions\n%s", new_ts, orig_ts.getTouchedPartitions().values()));
        }
        
        this.dispatchInvocation(new_ts);
        return (Status.ABORT_RESTART);
    }

    
    // ----------------------------------------------------------------------------
    // TRANSACTION FINISH/CLEANUP METHODS
    // ----------------------------------------------------------------------------

    /**
     * Send back the given ClientResponse to the actual client waiting for it
     * At this point the transaction should been properly committed or aborted at
     * the PartitionExecutor, including if it was mispredicted. This is the only place that
     * we will invoke the original Client callback and send back the results.
     * Note that the ClientResponse's status cannot be ABORT_MISPREDICT.
     * @param ts
     * @param cresponse
     */
    public void sendClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        assert(cresponse != null) :
            "Missing ClientResponse for " + ts;
        assert(cresponse.getClientHandle() != -1) :
            "The client handle for " + ts + " was not set properly";
        assert(cresponse.getStatus() != Status.ABORT_MISPREDICT) :
            "Trying to send back a client response for " + ts + " but the status is " + cresponse.getStatus();
        
        // Don't send anything back if it's a mispredict because it's as waste of time...
        // If the txn committed/aborted, then we can send the response directly back to the
        // client here. Note that we don't even need to call HStoreSite.finishTransaction()
        // since that doesn't do anything that we haven't already done!
        if (d) LOG.debug(String.format("%s - Sending back ClientResponse [status=%s]", ts, cresponse.getStatus()));

        // Check whether we should disable throttling
        cresponse.setRequestCounter(this.getNextRequestCounter());
        cresponse.setThrottleFlag(cresponse.getStatus() == Status.ABORT_THROTTLED);
        
        // So we have a bit of a problem here.
        // It would be nice if we could use the BufferPool to get a block of memory so
        // that we can serialize the ClientResponse out to a byte array
        // Since we know what we're doing here, we can just free the memory back to the
        // buffer pool once we call deleteTransaction()
        // The problem is that we need access to the underlying array of the ByteBuffer,
        // but we can't get that from here.
        byte bytes[] = null;
        int offset = this.getLocalPartitionOffset(ts.getBasePartition());
        FastSerializer out = this.partition_serializers[offset]; 
        synchronized (out) {
            out.clear();
            try {
                out.writeObject(cresponse);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            bytes = out.getBytes();
        } // SYNCH
        if (d) LOG.debug(String.format("Serialized ClientResponse for %s [throttle=%s, requestCtr=%d]",
                                       ts, cresponse.getThrottleFlag(), cresponse.getRequestCounter()));
        
        // Send result back to client!
        try {
            ts.getClientCallback().run(bytes);
        } catch (CancelledKeyException ex) {
            // IGNORE
        }
    }
    
    /**
     * 
     * @param es
     * @param ts
     * @param cr
     */
    public void queueClientResponse(LocalTransaction ts, ClientResponseImpl cr) {
        assert(hstore_conf.site.exec_postprocessing_thread);
        if (d) LOG.debug(String.format("Adding ClientResponse for %s from partition %d to processing queue [status=%s, size=%d]",
                                       ts, ts.getBasePartition(), cr.getStatus(), this.ready_responses.size()));
        this.ready_responses.add(ts);
    }

    /**
     * Perform final cleanup and book keeping for a completed txn
     * If you call this, you can never access anything in this txn's AbstractTransaction again
     * @param txn_id
     */
    public void deleteTransaction(final Long txn_id, final Status status) {
        assert(txn_id != null) : "Unexpected null transaction id";
        if (d) LOG.debug("Deleting internal info for txn #" + txn_id);
        AbstractTransaction abstract_ts = this.inflight_txns.remove(txn_id);
        
        // It's ok for us to not have a transaction handle, because it could be
        // for a remote transaction that told us that they were going to need one
        // of our partitions but then they never actually sent work to us
        if (abstract_ts == null) {
            if (d) LOG.warn(String.format("Ignoring clean-up request for txn #%d because we don't have a handle [status=%s]",
                                          txn_id, status));
            return;
        }
        
        assert(txn_id.equals(abstract_ts.getTransactionId())) :
            String.format("Mismatched %s - Expected[%d] != Actual[%s]", abstract_ts, txn_id, abstract_ts.getTransactionId());

        // Nothing else to do for RemoteTransactions other than to just
        // return the object back into the pool
        if (abstract_ts instanceof RemoteTransaction) {
            if (d) LOG.debug(String.format("Returning %s to ObjectPool [hashCode=%d]", abstract_ts, abstract_ts.hashCode()));
            HStoreObjectPools.STATES_TXN_REMOTE.returnObject((RemoteTransaction)abstract_ts);
            return;
        }
        
        final LocalTransaction ts = (LocalTransaction)abstract_ts; 
        final int base_partition = ts.getBasePartition();
        final Procedure catalog_proc = ts.getProcedure();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
       
        assert(ts.checkDeletableFlag()) :
            String.format("Trying to delete %s before it was marked as ready!", ts);
        if (t) LOG.trace(ts + " - State before delete:\n" + ts.debug());
        
        // Update Transaction profiles
        // We have to calculate the profile information *before* we call PartitionExecutor.cleanup!
        // XXX: Should we include totals for mispredicted txns?
        if (hstore_conf.site.txn_profiling && this.status_monitor != null &&
            ts.profiler.isDisabled() == false && status != Status.ABORT_MISPREDICT) {
            ts.profiler.stopTransaction();
            this.status_monitor.addTxnProfile(catalog_proc, ts.profiler);
        }
        
        // Clean-up any extra information that we may have for the txn
        TransactionEstimator t_estimator = null;
        if (ts.getEstimatorState() != null) {
            t_estimator = this.executors[base_partition].getTransactionEstimator();
            assert(t_estimator != null);
        }
        try {
            switch (status) {
                case OK:
                    if (t_estimator != null) {
                        if (t) LOG.trace("Telling the TransactionEstimator to COMMIT " + ts);
                        t_estimator.commit(txn_id);
                    }
                    // We always need to keep track of how many txns we process 
                    // in order to check whether we are hung or not
                    if (hstore_conf.site.status_show_txn_info || hstore_conf.site.status_kill_if_hung) 
                        TxnCounter.COMPLETED.inc(catalog_proc);
                    break;
                case ABORT_USER:
                    if (t_estimator != null) {
                        if (t) LOG.trace("Telling the TransactionEstimator to ABORT " + ts);
                        t_estimator.abort(txn_id);
                    }
                    if (hstore_conf.site.status_show_txn_info)
                        TxnCounter.ABORTED.inc(catalog_proc);
                    break;
                case ABORT_MISPREDICT:
                case ABORT_RESTART:
                    if (t_estimator != null) {
                        if (t) LOG.trace("Telling the TransactionEstimator to IGNORE " + ts);
                        t_estimator.mispredict(txn_id);
                    }
                    if (hstore_conf.site.status_show_txn_info) {
                        (ts.isSpeculative() ? TxnCounter.RESTARTED : TxnCounter.MISPREDICTED).inc(catalog_proc);
                    }
                    break;
                case ABORT_REJECT:
                case ABORT_THROTTLED:
                    if (hstore_conf.site.status_show_txn_info)
                        TxnCounter.REJECTED.inc(catalog_proc);
                    break;
                case ABORT_UNEXPECTED:
                case ABORT_GRACEFUL:
                    // TODO: Make new counter?
                    break;
                default:
                    LOG.warn(String.format("Unexpected status %s for %s", status, ts));
            } // SWITCH
        } catch (Throwable ex) {
            LOG.error(String.format("Unexpected error when cleaning up %s transaction %s",
                                    status, ts), ex);
            // Pass...
        }
        
        // Then update transaction profiling counters
        if (hstore_conf.site.status_show_txn_info) {
            if (ts.isSpeculative()) TxnCounter.SPECULATIVE.inc(catalog_proc);
            if (ts.isExecNoUndoBuffer(base_partition)) TxnCounter.NO_UNDO.inc(catalog_proc);
            if (ts.isSysProc()) {
                TxnCounter.SYSPROCS.inc(catalog_proc);
            } else if (status != Status.ABORT_MISPREDICT && ts.isRejected() == false) {
                (singlePartitioned ? TxnCounter.SINGLE_PARTITION : TxnCounter.MULTI_PARTITION).inc(catalog_proc);
            }
        }
        
        // SANITY CHECK
        if (hstore_conf.site.exec_validate_work) {
            for (Integer p : this.local_partitions_arr) {
                assert(ts.equals(this.executors[p.intValue()].getCurrentDtxn()) == false) :
                    String.format("About to finish %s but it is still the current DTXN at partition %d", ts, p);
            } // FOR
        }
        
        assert(ts.isInitialized()) : "Trying to return uninititlized txn #" + txn_id;
        if (d) LOG.debug(String.format("%s - Returning to ObjectPool [hashCode=%d]", ts, ts.hashCode()));
        if (ts.isMapReduce()) {
            HStoreObjectPools.STATES_TXN_MAPREDUCE.returnObject((MapReduceTransaction)ts);
        } else {
            HStoreObjectPools.STATES_TXN_LOCAL.returnObject(ts);
        }
    }

    // ----------------------------------------------------------------------------
    // MAGIC HSTORESITE LAUNCHER
    // ----------------------------------------------------------------------------
    

    /**
     * Magic HStoreSite launcher
     * This is a blocking call!
     * @throws Exception
     */
    @Override
    public void run() {
        List<Runnable> runnables = new ArrayList<Runnable>();
        final HStoreSite hstore_site = this;
        final Site catalog_site = hstore_site.getSite();
        
        // ----------------------------------------------------------------------------
        // (1) Procedure Request Listener Thread (one per Site)
        // ----------------------------------------------------------------------------
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(HStoreThreadManager.getThreadName(hstore_site, "listen"));
                if (hstore_site.getHStoreConf().site.cpu_affinity)
                    hstore_site.getThreadManager().registerProcessingThread();
                
                // Then fire off this thread to have it do some work as it comes in 
                Throwable error = null;
                try {
                    hstore_site.voltListener.bind(catalog_site.getProc_port());
                    hstore_site.procEventLoop.setExitOnSigInt(true);
                    hstore_site.ready_latch.countDown();
                    hstore_site.procEventLoop.run();
                } catch (Throwable ex) {
                    if (ex != null && ex.getMessage() != null && ex.getMessage().contains("Connection closed") == false) {
                        error = ex;
                    }
                }
                if (error != null && hstore_site.isShuttingDown() == false) {
                    LOG.warn(String.format("Procedure Listener is stopping! [error=%s, hstore_shutdown=%s]",
                                           (error != null ? error.getMessage() : null), hstore_site.shutdown_state), error);
                    hstore_site.hstore_coordinator.shutdownCluster(error);
                }
            };
        });
        
        // ----------------------------------------------------------------------------
        // (5) HStoreSite Setup Thread
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Starting HStoreSite [site=%d]", hstore_site.getSiteId()));
        hstore_site.ready_latch = new CountDownLatch(runnables.size());
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(HStoreThreadManager.getThreadName(hstore_site, "setup"));
                if (hstore_site.getHStoreConf().site.cpu_affinity)
                    hstore_site.getThreadManager().registerProcessingThread();
                
                // Always invoke HStoreSite.start() right away, since it doesn't depend on any
                // of the stuff being setup yet
                hstore_site.init();
                
                // But then wait for all of the threads to be finished with their initializations
                // before we tell the world that we're ready!
                if (hstore_site.ready_latch.getCount() > 0) {
                    if (d) LOG.debug(String.format("Waiting for %d threads to complete initialization tasks", hstore_site.ready_latch.getCount()));
                    try {
                        hstore_site.ready_latch.await();
                    } catch (Exception ex) {
                        LOG.error("Unexpected interuption while waiting for engines to start", ex);
                        hstore_site.hstore_coordinator.shutdownCluster(ex);
                    }
                }
                hstore_site.start();
            }
        });
        
        // This will block the MAIN thread!
        ThreadUtil.runNewPool(runnables);
    }
}
