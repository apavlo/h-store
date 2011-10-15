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
package edu.mit.hstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.ExecutionSiteHelper;
import org.voltdb.ExecutionSitePostProcessor;
import org.voltdb.ProcedureProfiler;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.utils.Pair;

import ca.evanjones.protorpc.NIOEventLoop;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.Hstore;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.*;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.Workload;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;
import edu.mit.hstore.callbacks.TransactionRedirectCallback;
import edu.mit.hstore.callbacks.TransactionWorkCallback;
import edu.mit.hstore.dtxn.AbstractTransaction;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.RemoteTransaction;
import edu.mit.hstore.interfaces.Loggable;
import edu.mit.hstore.interfaces.Shutdownable;
import edu.mit.hstore.util.NewOrderInspector;
import edu.mit.hstore.util.TransactionQueueManager;
import edu.mit.hstore.util.TxnCounter;

/**
 * 
 * @author pavlo
 */
public class HStoreSite implements VoltProcedureListener.Handler, Shutdownable, Loggable {
    private static final Logger LOG = Logger.getLogger(HStoreSite.class);
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
    
    /**
     * Formatted site name
     * @param site_id
     * @param suffix - Can be null
     * @param partition - Can be null
     * @return
     */
    public static final String getThreadName(int site_id, String suffix, Integer partition) {
        if (suffix == null) suffix = "";
        if (suffix.isEmpty() == false) {
            suffix = "-" + suffix;
            if (partition != null) suffix = String.format("-%03d%s", partition.intValue(), suffix);
        } else if (partition != null) {
            suffix = String.format("-%03d", partition.intValue());
        }
        return (String.format("H%02d%s", site_id, suffix));
    }

    public static final String getThreadName(HStoreSite hstore_site, String suffix, Integer partition) {
        return (HStoreSite.getThreadName(hstore_site.site_id, suffix, partition));
    }
    public static final String getThreadName(HStoreSite hstore_site, String suffix) {
        return (HStoreSite.getThreadName(hstore_site.site_id, suffix, null));
    }
    public static final String getThreadName(HStoreSite hstore_site, Integer partition) {
        return (HStoreSite.getThreadName(hstore_site.site_id, null, partition));
    }
    public static final String formatSiteName(Integer site_id) {
        if (site_id == null) return (null);
        return (HStoreSite.getThreadName(site_id, null, null));
    }
    public static final String formatPartitionName(int site_id, int partition_id) {
        return (HStoreSite.getThreadName(site_id, null, partition_id));
    }
    
    /**
     * TODO
     */
    private static HStoreSite SHUTDOWN_HANDLE = null;

    
    /**
     * 
     */
    public static int LOCAL_PARTITION_OFFSETS[];
    
    /**
     * For a given offset from LOCAL_PARTITION_OFFSETS, this array
     * will contain the partition id
     */
    public static int LOCAL_PARTITION_REVERSE[];
    
    // ----------------------------------------------------------------------------
    // OBJECT POOLS
    // ----------------------------------------------------------------------------

    private final HStoreThreadManager threadManager;
    
    private final TransactionQueueManager txnQueueManager;
    
    /**
     * This is the thing that we will actually use to generate txn ids used by our H-Store specific code
     */
    private final TransactionIdManager txnid_managers[];
    
    private final HStoreCoordinator hstore_coordinator;

    /**
     * Local ExecutionSite Stuff
     */
    private final ExecutionSite executors[];
    private final Thread executor_threads[];
    
    /**
     * Procedure Listener Stuff
     */
    private final VoltProcedureListener voltListeners[];
    private final NIOEventLoop procEventLoops[];

    /**
     * 
     */
    private boolean ready = false;
    private CountDownLatch ready_latch;
    private final EventObservable ready_observable = new EventObservable();
    
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
    private final EventObservable shutdown_observable = new EventObservable();
    
    /** Catalog Stuff **/
    protected final Site catalog_site;
    protected final int site_id;
    protected final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final AbstractHasher hasher;
    
    /** Cached Rejection Response **/
    private final ByteBuffer cached_ClientResponse;
    
    /** All of the partitions in the cluster */
    private final Collection<Integer> all_partitions;

    /** List of local partitions at this HStoreSite */
    private final ListOrderedSet<Integer> local_partitions = new ListOrderedSet<Integer>();
    
    private final Collection<Integer> single_partition_sets[]; 
    
    private final int num_local_partitions;
    
    /** PartitionId -> SiteId */
    private final Map<Integer, Integer> partition_site_xref = new HashMap<Integer, Integer>();
    
    /** Request counter **/
    private final AtomicInteger request_counter = new AtomicInteger(0); 
    
    /**
     * ClientResponse Processor Thread
     */
    private final List<ExecutionSitePostProcessor> processors = new ArrayList<ExecutionSitePostProcessor>();
    private final LinkedBlockingDeque<Object[]> ready_responses = new LinkedBlockingDeque<Object[]>();
    
    private final HStoreConf hstore_conf;
    
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
     * Keep track of which txns that we have in-flight right now
     */
    private final ConcurrentHashMap<Long, AbstractTransaction> inflight_txns = new ConcurrentHashMap<Long, AbstractTransaction>();
    private final AtomicInteger inflight_txns_ctr[];

    /**
     * Helper Thread Stuff
     */
    private ExecutionSiteHelper helper;
    private final ScheduledExecutorService helper_pool;
    
    /**
     * TPC-C NewOrder Cheater
     */
    private final NewOrderInspector tpcc_inspector;
    
    /**
     * Status Monitor
     */
    private HStoreSiteStatus status_monitor = null;
    
    
    /**
     * For whatever...
     */
    private final Random rand = new Random();

    private final boolean incoming_throttle[];
    private final int incoming_queue_max[];
    private final int incoming_queue_release[];
    private final Histogram<Integer> incoming_partitions = new Histogram<Integer>();
    private final Histogram<String> incoming_listeners = new Histogram<String>();
    protected final ProfileMeasurement incoming_throttle_time[];

//    private final boolean redirect_throttle[];
//    private final int redirect_queue_max;
//    private final int redirect_queue_release;
//    protected final ProfileMeasurement redirect_throttle_time = new ProfileMeasurement("redirectThrottle");
    
    /** How long the HStoreSite had no inflight txns */
    protected final ProfileMeasurement idle_time = new ProfileMeasurement("idle");
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param coordinators
     * @param p_estimator
     */
    @SuppressWarnings("unchecked")
    public HStoreSite(Site catalog_site, Map<Integer, ExecutionSite> executors, PartitionEstimator p_estimator) throws Exception {
        assert(catalog_site != null);
        assert(p_estimator != null);
        
        this.hstore_conf = HStoreConf.singleton();
        this.catalog_site = catalog_site;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog_site);
        this.site_id = this.catalog_site.getId();
        
        // **IMPORTANT**
        // We have to setup the partition offsets before we do anything else here
        this.all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        final int num_partitions = this.all_partitions.size();
        this.local_partitions.addAll(executors.keySet());
        this.num_local_partitions = this.local_partitions.size();
        LOCAL_PARTITION_OFFSETS = new int[num_partitions];
        LOCAL_PARTITION_REVERSE = new int[this.num_local_partitions];
        int offset = 0;
        for (Integer p : executors.keySet()) {
            LOCAL_PARTITION_OFFSETS[p.intValue()] = offset;
            LOCAL_PARTITION_REVERSE[offset] = p.intValue(); 
            offset++;
        } // FOR
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_site)) {
            this.partition_site_xref.put(catalog_part.getId(), ((Site)catalog_part.getParent()).getId());
        } // FOR
        
        // Static Object Pools
        HStoreObjectPools.initialize(this);
        
        // General Stuff
        this.p_estimator = p_estimator;
        this.hasher = this.p_estimator.getHasher();
        this.thresholds = new EstimationThresholds(); // default values

        // Distributed Transaction Queue Manager
        this.txnQueueManager = new TransactionQueueManager(this);
        
        this.executors = new ExecutionSite[num_partitions];
        this.executor_threads = new Thread[num_partitions];
        this.txnid_managers = new TransactionIdManager[num_partitions];
        this.inflight_txns_ctr = new AtomicInteger[num_partitions];
        this.incoming_throttle = new boolean[num_partitions];
        this.incoming_throttle_time = new ProfileMeasurement[num_partitions];
        this.incoming_queue_max = new int[num_partitions];
        this.incoming_queue_release = new int[num_partitions];
        this.single_partition_sets = new Collection[num_partitions];
        
        for (int partition : executors.keySet()) {
            this.executors[partition] = executors.get(partition);
            this.txnid_managers[partition] = new TransactionIdManager(partition);
            this.inflight_txns_ctr[partition] = new AtomicInteger(0); 
            this.incoming_throttle[partition] = false;
            this.incoming_throttle_time[partition] = new ProfileMeasurement("incoming-" + partition);
            this.incoming_queue_max[partition] = hstore_conf.site.txn_incoming_queue_max_per_partition;
            this.incoming_queue_release[partition] = Math.max((int)(this.incoming_queue_max[partition] * hstore_conf.site.txn_incoming_queue_release_factor), 1);
            this.single_partition_sets[partition] = Collections.singleton(partition); 
            
            if (hstore_conf.site.status_show_executor_info) {
                this.incoming_throttle_time[partition].resetOnEvent(this.startWorkload_observable);
            }
            
        } // FOR
        this.threadManager = new HStoreThreadManager(this);
        this.voltListeners = new VoltProcedureListener[this.num_local_partitions];
        this.procEventLoops = new NIOEventLoop[this.num_local_partitions];
        
        if (hstore_conf.site.status_show_executor_info) {
            this.idle_time.resetOnEvent(this.startWorkload_observable);
        }
        
        if (hstore_conf.site.exec_postprocessing_thread) {
            assert(hstore_conf.site.exec_postprocessing_thread_count > 0);
            if (d)
                LOG.debug(String.format("Starting %d post-processing threads", hstore_conf.site.exec_postprocessing_thread_count));
            for (int i = 0; i < hstore_conf.site.exec_postprocessing_thread_count; i++) {
                this.processors.add(new ExecutionSitePostProcessor(this, this.ready_responses));
            } // FOR
        }
        this.hstore_coordinator = new HStoreCoordinator(this);
        this.helper_pool = Executors.newScheduledThreadPool(1);
        
        // Create all of our parameter manglers
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.param_manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (d) LOG.debug(String.format("Created ParameterManglers for %d procedures", this.param_manglers.size()));
        
        // Reusable Cached Messages
        ClientResponseImpl cresponse = new ClientResponseImpl(-1, -1, Hstore.Status.ABORT_REJECT, HStoreConstants.EMPTY_RESULT, "");
        this.cached_ClientResponse = ByteBuffer.wrap(FastSerializer.serialize(cresponse));
        
        // NewOrder Hack
        if (hstore_conf.site.exec_neworder_cheat) {
            this.tpcc_inspector = new NewOrderInspector(this);
        } else {
            this.tpcc_inspector = null;
        }
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
        return HStoreSite.LOCAL_PARTITION_OFFSETS[partition];
    }
    
    /**
     * 
     * @param offset
     * @return
     */
    public int getLocalPartitionFromOffset(int offset) {
        return HStoreSite.LOCAL_PARTITION_REVERSE[offset];
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
    public ExecutionSite getExecutionSite(int partition) {
        ExecutionSite es = this.executors[partition]; 
        assert(es != null) : "Unexpected null ExecutionSite for partition #" + partition + " on " + this.getSiteName();
        return (es);
    }
    public ExecutionSiteHelper getExecutionSiteHelper() {
        return (this.helper);
    }
    public Collection<ExecutionSitePostProcessor> getExecutionSitePostProcessors() {
        return (this.processors);
    }
    public HStoreCoordinator getCoordinator() {
        return (this.hstore_coordinator);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public ParameterMangler getParameterMangler(String proc_name) {
        Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null) : "Invalid Procedure name '" + proc_name + "'";
        return (this.param_manglers.get(catalog_proc));
    }
    protected TransactionQueueManager getTransactionQueueManager() {
        return (this.txnQueueManager);
    }
    public EstimationThresholds getThresholds() {
        return thresholds;
    }
    private void setThresholds(EstimationThresholds thresholds) {
         this.thresholds = thresholds;
//         if (d) 
         LOG.info("Set new EstimationThresholds: " + thresholds);
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
        return (HStoreSite.getThreadName(this.site_id, null, null));
    }
    public Collection<Integer> getAllPartitionIds() {
        return (this.all_partitions);
    }
    
    /**
     * Return the list of partition ids managed by this HStoreSite 
     * @return
     */
    public Collection<Integer> getLocalPartitionIds() {
        return (this.local_partitions);
    }
    
    public Integer getSiteIdForPartitionId(Integer partition_id) {
        return this.partition_site_xref.get(partition_id);
    }
    
    @SuppressWarnings("unchecked")
    public <T extends AbstractTransaction> T getTransaction(long txn_id) {
        return ((T)this.inflight_txns.get(txn_id));
    }
    
    /**
     * Get the total number of transactions inflight for all partitions 
     */
    protected int getInflightTxnCount() {
        return (this.inflight_txns.size());
    }
    /**
     * Get the number of transactions inflight for this partition
     */
    protected int getInflightTxnCount(int partition) {
        return (this.inflight_txns_ctr[partition].get());
    }
    
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
    // INITIALIZATION STUFF
    // ----------------------------------------------------------------------------

    /**
     * Initializes all the pieces that we need to start this HStore site up
     */
    public void init() {
        if (d) LOG.debug("Initializing HStoreSite " + this.getSiteName());

        List<ExecutionSite> executor_list = new ArrayList<ExecutionSite>();
        for (int partition : this.local_partitions) {
            executor_list.add(this.getExecutionSite(partition));
        } // FOR
        
        EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        EventObserver<Pair<Thread, Throwable>> observer = new EventObserver<Pair<Thread, Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> arg) {
                LOG.error(String.format("Thread %s had an Exception. Halting H-Store Cluster", arg.getFirst().getName()),
                          arg.getSecond());
                hstore_coordinator.shutdownCluster(arg.getSecond(), true);
            }
        };
        handler.addObserver(observer);
        
        // First we need to tell the HStoreMessenger to start-up and initialize its connections
        if (d) LOG.debug("Starting HStoreMessenger for " + this.getSiteName());
        this.hstore_coordinator.start();

        // Start TransactionQueueManager
        Thread t = new Thread(this.txnQueueManager);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(handler);
        t.start();
        
        // Start Status Monitor
        if (hstore_conf.site.status_interval > 0) {
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
            for (ExecutionSitePostProcessor espp : this.processors) {
                t = new Thread(espp);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(handler);
                t.start();    
            } // FOR
        }
        
        // Schedule the ExecutionSiteHelper
        if (d) LOG.debug(String.format("Scheduling ExecutionSiteHelper to run every %.1f seconds", hstore_conf.site.helper_interval / 1000f));
        this.helper = new ExecutionSiteHelper(this,
                                              executor_list,
                                              hstore_conf.site.helper_txn_per_round,
                                              hstore_conf.site.helper_txn_expire,
                                              hstore_conf.site.txn_profiling);
        this.helper_pool.scheduleAtFixedRate(this.helper,
                                             hstore_conf.site.helper_initial_delay,
                                             hstore_conf.site.helper_interval,
                                             TimeUnit.MILLISECONDS);
        
        // Then we need to start all of the ExecutionSites in threads
        if (d) LOG.debug("Starting ExecutionSite threads for " + this.local_partitions.size() + " partitions on " + this.getSiteName());
        for (int partition : this.local_partitions) {
            ExecutionSite executor = this.getExecutionSite(partition);
            executor.initHStoreSite(this);

            t = new Thread(executor);
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY); // Probably does nothing...
            t.setUncaughtExceptionHandler(handler);
            this.executor_threads[partition] = t;
            t.start();
        } // FOR
        
        if (d) LOG.debug("Preloading cached objects");
        try {
            // Load up everything the QueryPlanUtil
            PlanNodeUtil.preload(this.catalog_db);
            
            // Then load up everything in the PartitionEstimator
            this.p_estimator.preload();
         
            // Don't forget our CatalogUtil friend!
            CatalogUtil.preload(this.catalog_db);
            
        } catch (Exception ex) {
            LOG.fatal("Failed to prepare HStoreSite", ex);
            System.exit(1);
        }
        
        // Add in our shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
        
        // And mark ourselves as the current HStoreSite in case somebody wants to take us down!
        synchronized (HStoreSite.class) {
            if (SHUTDOWN_HANDLE == null) SHUTDOWN_HANDLE = this;
        } // SYNCH
    }
    
    /**
     * Mark this HStoreSite as ready for action!
     */
    private synchronized void ready() {
        if (this.ready) {
            LOG.warn("Already told that we were ready... Ignoring");
            return;
        }
        
        // This message must always be printed in order for the BenchmarkController
        // to know that we're ready!
        LOG.info(String.format("%s [site=%s, ports=%s, #partitions=%d]",
                               HStoreConstants.SITE_READY_MSG,
                               this.getSiteName(),
                               CatalogUtil.getExecutionSitePorts(catalog_site),
                               this.local_partitions.size()));
        this.ready = true;
        this.ready_observable.notifyObservers();
    }
    
    /**
     * Returns true if this HStoreSite is ready
     */
    public boolean isReady() {
        return (this.ready);
    }
    /**
     * Get the Observable handle for this HStoreSite that can alert others when the party is
     * getting started
     */
    public EventObservable getReadyObservable() {
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
     * Returns true if this HStoreSite is throttling incoming transactions
     */
    protected boolean isIncomingThrottled(int partition) {
        return (this.incoming_throttle[partition]);
    }
    protected int getIncomingQueueMax(int partition) {
        return (this.incoming_queue_max[partition]);
    }
    protected int getIncomingQueueRelease(int partition) {
        return (this.incoming_queue_release[partition]);
    }
    protected Histogram<Integer> getIncomingPartitionHistogram() {
        return (this.incoming_partitions);
    }
    protected Histogram<String> getIncomingListenerHistogram() {
        return (this.incoming_listeners);
    }

    public ProfileMeasurement getEmptyQueueTime() {
        return (this.idle_time);
    }
    
    // ----------------------------------------------------------------------------
    // THROTTLING METHODS
    // ----------------------------------------------------------------------------

    /**
     * 
     * @param partition
     */
    public void checkEnableThrottling(int partition) {
        // Look at the number of inflight transactions and see whether we should block and wait for the 
        // queue to drain for a bit
        int queue_size = this.inflight_txns_ctr[partition].incrementAndGet();
        
        // This partition's queue was empty, but now it's not. So we can halt the idle time
        if (hstore_conf.site.status_show_executor_info && queue_size == 1) {
            ProfileMeasurement.stop(true, idle_time);
        }
        // This partition is not throttled, but now the queue size is greater than our 
        // max limit. So we're going to need to throttle it
        if (this.incoming_throttle[partition] == false && queue_size > this.incoming_queue_max[partition]) {
            if (d) LOG.debug(String.format("INCOMING overloaded at partition %d!. Waiting for queue to drain [size=%d, trigger=%d]",
                                           partition, queue_size, this.incoming_queue_release[partition]));
            this.incoming_throttle[partition] = true;
            if (hstore_conf.site.status_show_executor_info) 
                ProfileMeasurement.start(true, this.incoming_throttle_time[partition]);
            
            // HACK: Randomly discard some distributed TransactionStates because we can't
            // tell the Dtxn.Coordinator to prune its queue.
            if (hstore_conf.site.txn_enable_queue_pruning && rand.nextBoolean() == true) {
                int ctr = 0;
                for (Long dtxn_id : this.inflight_txns.keySet()) {
                    LocalTransaction _ts = (LocalTransaction)this.inflight_txns.get(dtxn_id);
                    if (_ts == null) continue;
                    if (_ts.isPredictSinglePartition() == false && _ts.hasStarted(partition) == false && rand.nextInt(10) == 0) {
                        _ts.markAsRejected();
                        ctr++;
                    }
                } // FOR
                if (d && ctr > 0) LOG.debug("Pruned " + ctr + " queued distributed transactions to try to free up the queue");
            }
        }
    }
    
    /**
     * Check to see whether this HStoreSite can disable throttling mode, and does so if it can
     * Returns true if throttling is still enabled.
     * @param txn_id
     * @return
     */
    public boolean checkDisableThrottling(long txn_id, int partition) {
        if (this.incoming_throttle[partition]) {
            int queue_size = this.inflight_txns_ctr[partition].get(); // XXX - this.ready_responses.size(); 
            if (this.incoming_throttle[partition] && queue_size < this.incoming_queue_release[partition]) {
                this.incoming_throttle[partition] = false;
                if (hstore_conf.site.status_show_executor_info)
                    ProfileMeasurement.stop(true, this.incoming_throttle_time[partition]);
                if (d) LOG.debug(String.format("Disabling INCOMING throttling for Partition %2d because txn #%d finished [inflight=%d, release=%d]",
                                               partition, txn_id, queue_size, this.incoming_queue_release[partition]));
            }
        }
        return (this.incoming_throttle[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // HSTORESTITE SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    public static void crash() {
        if (SHUTDOWN_HANDLE != null) {
            SHUTDOWN_HANDLE.hstore_coordinator.shutdownCluster();
        } else {
            LOG.fatal("H-Store has encountered an unrecoverable error and is exiting.");
            LOG.fatal("The log may contain additional information.");
            System.exit(-1);
        }
        
    }
    
    /**
     * Shutdown Hook Thread
     */
    private final class ShutdownHook implements Runnable {
        @Override
        public void run() {
            // Dump out our status
            int num_inflight = inflight_txns.size();
            if (num_inflight > 0) {
                System.err.println("Shutdown [" + num_inflight + " txns inflight]");
            }
        }
    } // END CLASS

    @Override
    public void prepareShutdown() {
        this.shutdown_state = ShutdownState.PREPARE_SHUTDOWN;
        this.hstore_coordinator.prepareShutdown();
        for (ExecutionSitePostProcessor espp : this.processors) {
            espp.prepareShutdown();
        } // FOR
        for (int p : this.local_partitions) {
            this.executors[p].prepareShutdown();
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
        this.shutdown_state = ShutdownState.SHUTDOWN;
//      if (d)
        LOG.info("Shutting down everything at " + this.getSiteName());

        // Stop the monitor thread
        if (this.status_monitor != null) this.status_monitor.shutdown();
        
        // Tell our local boys to go down too
        for (ExecutionSitePostProcessor p : this.processors) {
            p.shutdown();
        }
        for (int p : this.local_partitions) {
            if (t) LOG.trace("Telling the ExecutionSite for partition " + p + " to shutdown");
            this.executors[p].shutdown();
        } // FOR
      
        // Tell anybody that wants to know that we're going down
        if (t) LOG.trace("Notifying " + this.shutdown_observable.countObservers() + " observers that we're shutting down");
        this.shutdown_observable.notifyObservers();
        
        // Stop the helper
        this.helper_pool.shutdown();
        
        // Tell all of our event loops to stop
        if (t) LOG.trace("Telling Procedure Listener event loops to exit");
        for (NIOEventLoop l : this.procEventLoops) {
            l.exitLoop();
        }
        
        // if (t) LOG.trace("Telling Dtxn.Engine event loop to exit");
        // this.engineEventLoop.exitLoop();
        
//        if (d) 
            LOG.info("Completed shutdown process at " + this.getSiteName());
    }
    
    /**
     * Returns true if HStoreSite is in the process of shutting down
     * @return
     */
    @Override
    public boolean isShuttingDown() {
        return (this.shutdown_state == ShutdownState.SHUTDOWN || this.shutdown_state == ShutdownState.PREPARE_SHUTDOWN);
    }
    
    /**
     * Get the Oberservable handle for this HStoreSite that can alert others when the party is ending
     * @return
     */
    public void addShutdownObservable(EventObserver<?> observer) {
        this.shutdown_observable.addObserver(observer);
    }

    // ----------------------------------------------------------------------------
    // EXECUTION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void procedureInvocation(byte[] serializedRequest, RpcCallback<byte[]> done) {
        long timestamp = (hstore_conf.site.txn_profiling ? ProfileMeasurement.getTime() : -1);
        
        // The serializedRequest is a StoredProcedureInvocation object
        StoredProcedureInvocation request = null;
        FastDeserializer fds = new FastDeserializer(serializedRequest);
        try {
            request = fds.readObject(StoredProcedureInvocation.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize incoming StoredProcedureInvocation", e);
        } finally {
            if (request == null)
                throw new RuntimeException("Failed to get ProcedureInvocation object from request bytes");
        }

        // Extract the stuff we need to figure out whether this guy belongs at our site
        request.buildParameterSet();
        assert(request.getParams() != null) : "The parameters object is null for new txn from client #" + request.getClientHandle();
        final Object args[] = request.getParams().toArray(); 
        final Procedure catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(request.getProcName());
        final boolean sysproc = request.isSysProc();
        int base_partition = request.getBasePartition();
        if (catalog_proc == null) throw new RuntimeException("Unknown procedure '" + request.getProcName() + "'");
        if (d) LOG.debug(String.format("Received new stored procedure invocation request for %s [handle=%d, bytes=%d]", catalog_proc.getName(), request.getClientHandle(), serializedRequest.length));

        // Profiling Updates
        if (hstore_conf.site.status_show_txn_info) TxnCounter.RECEIVED.inc(request.getProcName());
        if (hstore_conf.site.exec_profiling) {
            if (base_partition != -1)
                this.incoming_partitions.put(base_partition);
            this.incoming_listeners.put(Thread.currentThread().getName());
        }
        
        // First figure out where this sucker needs to go
        // If it's a sysproc, then it doesn't need to go to a specific partition
        if (sysproc) {
            // HACK: Check if we should shutdown. This allows us to kill things even if the
            // DTXN coordinator is stuck.
            if (catalog_proc.getName().equalsIgnoreCase("@Shutdown")) {
                ClientResponseImpl cresponse = new ClientResponseImpl(1, 1, Hstore.Status.OK, HStoreConstants.EMPTY_RESULT, "");
                FastSerializer out = new FastSerializer();
                try {
                    out.writeObject(cresponse);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                done.run(out.getBytes());
                // Non-blocking....
                this.hstore_coordinator.shutdownCluster(new Exception("Shutdown command received at " + this.getSiteName()), false);
                return;
            }
        // DB2-style Transaction Redirection
        } else if (base_partition != -1 || hstore_conf.site.exec_db2_redirects) {
            if (d) LOG.debug(String.format("Using embedded base partition from %s request", request.getProcName()));
            assert(base_partition == request.getBasePartition());    
            
        // Otherwise we use the PartitionEstimator to figure out where this thing needs to go
        } else if (hstore_conf.site.exec_force_localexecution == false) {
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
            base_partition = this.local_partitions.get((int)(Math.abs(request.getClientHandle()) % this.num_local_partitions));
        }
        
        if (d) LOG.debug(String.format("Incoming %s transaction request [handle=%d, partition=%d]",
                                       request.getProcName(), request.getClientHandle(), base_partition));
        
        // -------------------------------
        // REDIRECT TXN TO PROPER PARTITION
        // If the dest_partition isn't local, then we need to ship it off to the right location
        // -------------------------------
        TransactionIdManager id_generator = this.txnid_managers[base_partition];
        if (id_generator == null) {
            if (d) LOG.debug(String.format("Forwarding %s request to partition %d", request.getProcName(), base_partition));
            
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
            assert(request.hasBasePartition() == false) : "Trying to redirect " + request.getProcName() + " transaction more than once!";
            StoredProcedureInvocation.markRawBytesAsRedirected(base_partition, serializedRequest);
            
            this.hstore_coordinator.transactionRedirect(serializedRequest, callback, base_partition);
            if (hstore_conf.site.status_show_txn_info) TxnCounter.REDIRECTED.inc(catalog_proc);
            return;
        }
        
        // Check whether we're throttled and should just reject this transaction
        //  (1) It's not a sysproc
        //  (2) It's a new txn request and we're throttled on incoming requests
        //  (2) It's a redirect request and we're throttled on redirects
        if (sysproc == false && this.incoming_throttle[base_partition]) {
            if (hstore_conf.site.status_show_txn_info) TxnCounter.RECEIVED.inc(request.getProcName());
            int request_ctr = this.getNextRequestCounter();
            long clientHandle = request.getClientHandle();
            
            if (d)
                LOG.debug(String.format("Throttling is enabled. Rejecting transaction and asking client to wait [clientHandle=%d, requestCtr=%d]",
                                        clientHandle, request_ctr));
            
            synchronized (this.cached_ClientResponse) {
                ClientResponseImpl.setServerTimestamp(this.cached_ClientResponse, request_ctr);
                ClientResponseImpl.setThrottleFlag(this.cached_ClientResponse, this.incoming_throttle[base_partition]);
                ClientResponseImpl.setClientHandle(this.cached_ClientResponse, clientHandle);
                done.run(this.cached_ClientResponse.array());
            } // SYNCH
            if (hstore_conf.site.status_show_txn_info) TxnCounter.REJECTED.inc(request.getProcName());
            return;
        }
        
        // Grab a new LocalTransactionState object from the target base partition's ExecutionSite object pool
        // This will be the handle that is used all throughout this txn's lifespan to keep track of what it does
        long txn_id = id_generator.getNextUniqueTransactionId();
        LocalTransaction ts = null;
        try {
            ts = HStoreObjectPools.STATES_TXN_LOCAL.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for txn #" + txn_id);
            throw new RuntimeException(ex);
        }
        
        // Disable transaction profiling for sysprocs
        if (hstore_conf.site.txn_profiling && sysproc) ts.profiler.disableProfiling();
        
        // -------------------------------
        // TRANSACTION EXECUTION PROPERTIES
        // -------------------------------
        
        boolean predict_abortable = (hstore_conf.site.exec_no_undo_logging_all == false);
        boolean predict_readOnly = catalog_proc.getReadonly();
        Collection<Integer> predict_touchedPartitions = null;
        TransactionEstimator.State t_state = null; 
        
        // Sysprocs are always multi-partitioned
        if (sysproc) {
            if (t) LOG.trace(String.format("New request is for a sysproc %s, so it has to be multi-partitioned [clientHandle=%d]",
                                           request.getProcName(), request.getClientHandle()));
            predict_touchedPartitions = this.all_partitions;
            
        // Force all transactions to be single-partitioned
        } else if (hstore_conf.site.exec_force_singlepartitioned) {
            if (t) LOG.trace(String.format("The \"Always Single-Partitioned\" flag is true. Marking new %s transaction as single-partitioned on partition %d [clientHandle=%d]",
                                           request.getProcName(), base_partition, request.getClientHandle()));
            predict_touchedPartitions = this.single_partition_sets[base_partition];
            
        // Assume we're executing TPC-C neworder. Manually examine the input parameters and figure
        // out what partitions it's going to need to touch
        } else if (hstore_conf.site.exec_neworder_cheat && catalog_proc.getName().equalsIgnoreCase("neworder")) {
            if (t) LOG.trace(String.format("Using neworder argument hack for VLDB paper [clientHandle=%d]", request.getClientHandle()));
            predict_touchedPartitions = this.tpcc_inspector.initializeTransaction(args);
            
        // Otherwise, we'll try to estimate what the transaction will do (if we can)
        } else {
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
                    if (d) LOG.debug(String.format("No TransactionEstimator.State was returned for %s. Executing as multi-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id))); 
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
                    System.err.println("WROTE MARKOVGRAPH: " + gv.writeToTempFile(catalog_proc));
                }
                LOG.error(String.format("Failed calculate estimate for %s request", AbstractTransaction.formatTxnName(catalog_proc, txn_id)), ex);
                predict_touchedPartitions = this.all_partitions;
                predict_readOnly = false;
                predict_abortable = true;
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopInitEstimation();
            }
        }
        
        ts.init(txn_id, request.getClientHandle(), base_partition,
                        predict_touchedPartitions, predict_readOnly, predict_abortable,
                        t_state, catalog_proc, request, done);
        if (hstore_conf.site.txn_profiling) ts.profiler.startTransaction(timestamp);
        if (d) {
            LOG.debug(String.format("Initializing %s on partition %d [clientHandle=%d, touchedPartition=%s, readOnly=%s, abortable=%s]",
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
                    this.startWorkload_observable.notifyObservers();
                }
            } // SYNCH
        }
        
        this.initializeInvocation(ts);
    }

    /**
     * 
     * @param ts
     */
    private void initializeInvocation(LocalTransaction ts) {
        long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
                
        // For some odd reason we sometimes get duplicate transaction ids from the VoltDB id generator
        // So we'll just double check to make sure that it's unique, and if not, we'll just ask for a new one
        LocalTransaction dupe = (LocalTransaction)this.inflight_txns.put(txn_id, ts);
        if (dupe != null) {
            // HACK!
            this.inflight_txns.put(txn_id, dupe);
            long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
            if (new_txn_id == txn_id) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("ORIG TRANSACTION:\n" + dupe);
                LOG.fatal("NEW TRANSACTION:\n" + ts);
                this.hstore_coordinator.shutdownCluster(new Exception(msg), true);
            }
            LOG.warn(String.format("Had to fix duplicate txn ids: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            ts.setTransactionId(txn_id);
            this.inflight_txns.put(txn_id, ts);
        }
        
        // -------------------------------
        // SINGLE-PARTITION TRANSACTION
        // -------------------------------
        if (hstore_conf.site.exec_avoid_coordinator && ts.isPredictSinglePartition()) {
            this.transactionStart(ts);
            if (d) LOG.debug(String.format("Fast path single-partition execution for %s on partition %d [handle=%d]",
                                           ts, base_partition, ts.getClientHandle()));
        }
        
        // -------------------------------    
        // DISTRIBUTED TRANSACTION
        // -------------------------------
        else {
            if (d) LOG.debug(String.format("Queuing distributed %s to running at partition %d [handle=%d]",
                                           ts, base_partition, ts.getClientHandle()));
            
            // Since we know that this txn came over from the Dtxn.Coordinator, we'll throw it in
            // our set of coordinator txns. This way we can prevent ourselves from executing
            // single-partition txns straight at the ExecutionSite
//            if (d && dtxn_txns.isEmpty()) LOG.debug(String.format("Enabling CANADIAN mode [txn=#%d]", txn_id));
//            dtxn_txns.add(txn_id);
            
            // Partitions
            // Figure out what partitions we plan on touching for this transaction
            Collection<Integer> predict_touchedPartitions = ts.getPredictTouchedPartitions();
            
            // TransactionEstimator
            // If we know we're single-partitioned, then we *don't* want to tell the Dtxn.Coordinator
            // that we're done at any partitions because it will throw an error
            // Instead, if we're not single-partitioned then that's that only time that 
            // we Tell the Dtxn.Coordinator that we are finished with partitions if we have an estimate
            TransactionEstimator.State s = ts.getEstimatorState(); 
            if (ts.getOriginalTransactionId() == null && s != null && s.getInitialEstimate() != null) {
                MarkovEstimate est = s.getInitialEstimate();
                assert(est != null);
                predict_touchedPartitions.addAll(est.getTouchedPartitions(this.thresholds));
            }
            assert(predict_touchedPartitions.isEmpty() == false) : "Trying to mark " + ts + " as done at EVERY partition!";

            // This callback prevents us from making additional requests to the Dtxn.Coordinator until
            // we get hear back about our our initialization request
            if (hstore_conf.site.txn_profiling) ts.profiler.startCoordinatorBlocked();
            this.hstore_coordinator.transactionInit(ts, ts.getTransactionInitCallback());
        }
        
        this.checkEnableThrottling(base_partition);
    }
    
    /**
     * 
     * @param txn_id
     * @param callback
     */
    public void transactionInit(long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback) {
        this.txnQueueManager.insert(txn_id, partitions, callback);
    }

    /**
     * 
     * @param ts
     */
    public void transactionStart(LocalTransaction ts) {
        long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
        if (d) 
            LOG.debug(String.format("Starting %s on partition %d", ts, base_partition));
        
        // We have to wrap the StoredProcedureInvocation object into an InitiateTaskMessage so that it can be put
        // into the ExecutionSite's execution queue
        InitiateTaskMessage wrapper = new InitiateTaskMessage(txn_id, base_partition, base_partition, ts.isPredictReadOnly(), ts.getInvocation());
        
        // Always execute this mofo right away and let each ExecutionSite figure out what it needs to do
        ExecutionSite executor = this.executors[base_partition];
        assert(executor != null) : "No ExecutionSite exists for partition #" + base_partition + " at HStoreSite " + this.site_id;
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startQueue();
        executor.queueNewTransaction(ts, wrapper);
        
        if (hstore_conf.site.status_show_txn_info) {
            assert(ts.getProcedure() != null) : "Null Procedure for txn #" + txn_id;
            TxnCounter.EXECUTED.inc(ts.getProcedure());
        }
    }
    
    /**
     * Execute some work on a particular ExecutionSite
     * @param request
     * @param done
     */
    public void transactionWork(long txn_id, FragmentTaskMessage ftask, TransactionWorkCallback callback) {
        // TODO: The HStoreSite should pass a AbstractTransaction handle here so 
        // that we can re-use the same handle per HStoreSite
        RemoteTransaction ts = (RemoteTransaction)this.inflight_txns.get(txn_id);
        if (ts == null) {
            try {
                // Remote Transaction
                ts = (RemoteTransaction)HStoreObjectPools.STATES_TXN_REMOTE.borrowObject();
                ts.init(txn_id, ftask.getClientHandle(), ftask.getSourcePartitionId(), ftask.isReadOnly(), true);
                if (d) LOG.debug(String.format("Creating new RemoteTransactionState %s from remote partition %d to execute at partition %d [readOnly=%s, singlePartitioned=%s]",
                                               ts, ftask.getSourcePartitionId(), ftask.getDestinationPartitionId(), ftask.isReadOnly(), false));
            } catch (Exception ex) {
                LOG.fatal("Failed to construct TransactionState for txn #" + txn_id, ex);
                throw new RuntimeException(ex);
            }
            this.inflight_txns.put(txn_id, ts);
            if (t) LOG.trace(String.format("Stored new transaction state for %s at partition %d", ts, ftask.getDestinationPartitionId()));
        }
        if (t)
            LOG.trace(String.format("Queuing FragmentTaskMessage on partition %d for txn #%d",
                                    ftask.getDestinationPartitionId(), txn_id));
        this.executors[ftask.getDestinationPartitionId()].queueWork(ts, ftask, callback);
    }


    /**
     * This method is the first part of two phase commit for a transaction.
     * If speculative execution is enabled, then we'll notify each the ExecutionSites
     * for the listed partitions that it is done. This will cause all the 
     * that are blocked on this transaction to be released immediately and queued 
     * @param txn_id
     * @param partitions
     * @param updated
     */
    public void transactionPrepare(long txn_id, Collection<Integer> partitions, Collection<Integer> updated) {
        if (d) 
            LOG.debug(String.format("2PC:PREPARE Txn #%d [partitions=%s]", txn_id, partitions));
        
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        assert(ts != null) : String.format("Missing TransactionState for txn #%d at site %d", txn_id, this.site_id);
        boolean is_local = (ts instanceof LocalTransaction);
        
        int spec_cnt = 0;
        for (Integer p : partitions) {
            if (this.local_partitions.contains(p) == false) continue;
            
            // Always tell the queue stuff that the transaction is finished at this partition
            this.txnQueueManager.done(txn_id, p.intValue());
            
            // If speculative execution is enabled, then we'll turn it on at the ExecutionSite
            // for this partition
            if (hstore_conf.site.exec_speculative_execution) {
                boolean ret = this.executors[p.intValue()].enableSpeculativeExecution(ts, false);
                if (debug.get() && ret) {
                    spec_cnt++;
                    LOG.debug(String.format("Partition %d - Speculative Execution!", p));
                }
            }
            
            if (updated != null) updated.add(p);
            if (is_local) ((LocalTransaction)ts).getTransactionPrepareCallback().decrementCounter(1);

        } // FOR
        if (debug.get() && spec_cnt > 0)
            LOG.debug(String.format("Enabled speculative execution at %d partitions because of waiting for txn #%d", spec_cnt, txn_id));
    }
    
    /**
     * This method is used to finally complete the transaction.
     * The ExecutionSite will either commit or abort the transaction at the specified partitions
     * @param txn_id
     * @param status
     * @param partitions
     */
    public void transactionFinish(long txn_id, Hstore.Status status, Collection<Integer> partitions) {
        if (d) LOG.debug(String.format("2PC:FINISH Txn #%d [commitStatus=%s, partitions=%s]",
                                       txn_id, status, partitions));
        boolean commit = (status == Hstore.Status.OK);
        
        // If we don't have a AbstractTransaction handle, then we know that we never did anything
        // for this transaction and we can just ignore this finish request. We do have to tell
        // the TransactionQueue manager that we're done though
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        for (int p : partitions) {
            if (this.local_partitions.contains(p) == false) continue;
            
            // We only need to tell the queue stuff that the transaction is finished
            // on an ABORT_REJECT because there won't be a 2PC:PREPARE message
            if (status == Hstore.Status.ABORT_REJECT) {
                this.txnQueueManager.done(txn_id, p);
            }

            // Then actually commit the transaction in the execution engine
            // We only need to do this for distributed transactions, because all single-partition
            // transactions will commit/abort immediately
            if (ts != null && ts.isPredictSinglePartition() == false && ts.hasStarted(p)) {
                if (d) LOG.debug(String.format("Calling finishTransaction for %s on partition %d", ts, p));
                this.executors[p].finishTransaction(ts, commit);
            }
        } // FOR            
    }

    public ByteBuffer serializeClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        FastSerializer out = new FastSerializer(ExecutionSite.buffer_pool);
        try {
            out.writeObject(cresponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        // Check whether we should disable throttling
        boolean throttle = this.checkDisableThrottling(ts.getTransactionId(), ts.getBasePartition());
        int timestamp = this.getNextRequestCounter();
        
        ByteBuffer buffer = ByteBuffer.wrap(out.getBytes());
        ClientResponseImpl.setThrottleFlag(buffer, throttle);
        ClientResponseImpl.setServerTimestamp(buffer, timestamp);
        
        if (d) 
            LOG.debug(String.format("Serialized ClientResponse for %s [throttle=%s, timestamp=%d]",
                                    ts, throttle, timestamp));
        return (buffer);
    }
    
    /**
     * 
     * At this point the transaction should been properly committed or aborted at
     * the ExecutionSite, including if it was mispredicted.
     * @param ts
     * @param cresponse
     */
    public void sendClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        if (d)
            LOG.debug(String.format("Sending back ClientResponse for " + ts));
        Hstore.Status status = cresponse.getStatus();
        assert(cresponse.getClientHandle() != -1) : "The client handle for " + ts + " was not set properly";
        
        // Don't send anything back if it's a mispredict because it's as waste of time...
        // If the txn committed/aborted, then we can send the response directly back to the
        // client here. Note that we don't even need to call HStoreSite.finishTransaction()
        // since that doesn't do anything that we haven't already done!
        if (status != Hstore.Status.ABORT_MISPREDICT) {
            // Send result back to client!
            ts.getClientCallback().run(this.serializeClientResponse(ts, cresponse).array());
        }
        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
        // as soon as possible...
        else {
            if (d) LOG.debug(String.format("Restarting %s because it mispredicted", ts));
            this.transactionRestart(ts, status);
        }
    }

    /**
     * The transaction was mispredicted as single-partitioned
     * This method will perform the following operations:
     *  (1) Restart the transaction as new multi-partitioned transaction
     *  (2) Mark the original transaction as aborted
     * @param txn_id
     * @param orig_callback - the original callback to the client
     */
    public void transactionRestart(LocalTransaction orig_ts, Hstore.Status status) {
        if (d) LOG.debug(orig_ts + " was mispredicted! Going to clean-up our mess and re-execute");
        int base_partition = orig_ts.getBasePartition();
        StoredProcedureInvocation spi = orig_ts.getInvocation();
        assert(spi != null) : "Missing StoredProcedureInvocation for " + orig_ts;
        
        // Figure out whether this transaction should be redirected based on what partitions it
        // tried to touch before it was aborted 
        if (status != Hstore.Status.ABORT_REJECT && hstore_conf.site.exec_db2_redirects) {
            Histogram<Integer> touched = orig_ts.getTouchedPartitions();
            Set<Integer> most_touched = touched.getMaxCountValues();
            if (d) LOG.debug(String.format("Touched partitions for mispredicted %s\n%s", orig_ts, touched));
            Integer redirect_partition = null;
            if (most_touched.size() == 1) {
                redirect_partition = CollectionUtil.first(most_touched);
            } else if (most_touched.isEmpty() == false) {
                redirect_partition = CollectionUtil.random(most_touched);
            } else {
                redirect_partition = CollectionUtil.random(this.all_partitions);
            }
            
            // If the txn wants to execute on another node, then we'll send them off *only* if this txn wasn't
            // already redirected at least once. If this txn was already redirected, then it's going to just
            // execute on the same partition, but this time as a multi-partition txn that locks all partitions.
            // That's what you get for messing up!!
            if (this.local_partitions.contains(redirect_partition) == false && spi.hasBasePartition() == false) {
                if (d) LOG.debug(String.format("Redirecting mispredicted %s to partition %d", orig_ts, redirect_partition));
                
                spi.setBasePartition(redirect_partition.intValue());
                
                // Add all the partitions that the txn touched before it got aborted
                spi.addPartitions(touched.values());
                
                byte serializedRequest[] = null;
                try {
                    serializedRequest = FastSerializer.serialize(spi);
                } catch (IOException ex) {
                    LOG.fatal("Failed to serialize StoredProcedureInvocation to redirect %s" + orig_ts);
                    this.hstore_coordinator.shutdownCluster(ex, false);
                    return;
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
                return;
                
            // Allow local redirect
            } else if (spi.hasBasePartition() == false) {    
                base_partition = redirect_partition.intValue();
                spi.setBasePartition(base_partition);
            } else {
                if (d) LOG.debug(String.format("Mispredicted %s has already been aborted once before. Restarting as all-partition txn", orig_ts));
                touched.putAll(this.local_partitions);
            }
        }

        long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
        LocalTransaction new_ts = null;
        try {
            new_ts = HStoreObjectPools.STATES_TXN_LOCAL.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for mispredicted " + orig_ts);
            throw new RuntimeException(ex);
        }
        
        // Restart the new transaction
        if (hstore_conf.site.txn_profiling) new_ts.profiler.startTransaction(ProfileMeasurement.getTime());
        
        Collection<Integer> predict_touchedPartitions = null;
        if (status == Hstore.Status.ABORT_REJECT) {
            predict_touchedPartitions = orig_ts.getPredictTouchedPartitions();
        } else if (orig_ts.getOriginalTransactionId() == null) {
            predict_touchedPartitions = new HashSet<Integer>(orig_ts.getTouchedPartitions().values());
        } else {
            predict_touchedPartitions = this.all_partitions;
        }
        boolean predict_readOnly = orig_ts.getProcedure().getReadonly(); // FIXME
        boolean predict_abortable = true; // FIXME
        new_ts.init(new_txn_id, base_partition, orig_ts, predict_touchedPartitions, predict_readOnly, predict_abortable);
        
        if (d) {
            LOG.debug(String.format("Re-executing %s as new %s-partition %s on partition %d",
                                    orig_ts, (predict_touchedPartitions.size() == 1 ? "single" : "multi"), new_ts, base_partition));
            if (t && status == Hstore.Status.ABORT_MISPREDICT)
                LOG.trace(String.format("%s Mispredicted partitions\n%s", new_ts, orig_ts.getTouchedPartitions().values()));
        }
        
        this.initializeInvocation(new_ts);
    }

    
    // ----------------------------------------------------------------------------
    // TRANSACTION FINISH/CLEANUP METHODS
    // ----------------------------------------------------------------------------

    /**
     * 
     * @param es
     * @param ts
     * @param cr
     */
    public void queueClientResponse(ExecutionSite es, LocalTransaction ts, ClientResponseImpl cr) {
        assert(hstore_conf.site.exec_postprocessing_thread);
        if (d) LOG.debug(String.format("Adding ClientResponse for %s from partition %d to processing queue [status=%s, size=%d]",
                                       ts, es.getPartitionId(), cr.getStatus(), this.ready_responses.size()));
        this.ready_responses.add(new Object[]{es, ts, cr});
    }

    /**
     * Perform final cleanup and book keeping for a completed txn
     * @param txn_id
     */
    public void completeTransaction(final long txn_id, final Hstore.Status status) {
        if (d) LOG.debug("Cleaning up internal info for Txn #" + txn_id);
        AbstractTransaction abstract_ts = this.inflight_txns.remove(txn_id);
        assert(abstract_ts != null) : String.format("Missing TransactionState for txn #%d at site %d", txn_id, this.site_id);

        // Nothing else to do for RemoteTransactions other than to just
        // return the object back into the pool
        if (abstract_ts instanceof RemoteTransaction) {
            HStoreObjectPools.STATES_TXN_REMOTE.returnObject((RemoteTransaction)abstract_ts);
            return;
        }
        
        final LocalTransaction ts = (LocalTransaction)abstract_ts; 
        final int base_partition = ts.getBasePartition();
        final Procedure catalog_proc = ts.getProcedure();
        
        // If this partition is completely idle, then we will increase the size of its upper limit
        if (this.inflight_txns_ctr[base_partition].decrementAndGet() == 0) {
            if (this.startWorkload && hstore_conf.site.txn_incoming_queue_increase > 0) {
                this.incoming_queue_max[base_partition] += hstore_conf.site.txn_incoming_queue_increase;
                this.incoming_queue_release[base_partition] = Math.max((int)(this.incoming_queue_max[base_partition] * hstore_conf.site.txn_incoming_queue_release_factor), 1); 
            }
            if (hstore_conf.site.status_show_executor_info) ProfileMeasurement.start(true, idle_time);
        }
        
        // Update Transaction profiles
        // We have to calculate the profile information *before* we call ExecutionSite.cleanup!
        // XXX: Should we include totals for mispredicted txns?
        if (hstore_conf.site.txn_profiling && this.status_monitor != null &&
            ts.profiler.isDisabled() == false && status != Hstore.Status.ABORT_MISPREDICT) {
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
                    if (t) LOG.trace("Telling the TransactionEstimator to COMMIT " + ts);
                    if (t_estimator != null) t_estimator.commit(txn_id);
                    // We always need to keep track of how many txns we process 
                    // in order to check whether we are hung or not
                    if (this.status_monitor != null) TxnCounter.COMPLETED.inc(catalog_proc);
                    break;
                case ABORT_USER:
                    if (t) LOG.trace("Telling the TransactionEstimator to ABORT " + ts);
                    if (t_estimator != null) t_estimator.abort(txn_id);
                    if (hstore_conf.site.status_show_txn_info)
                        TxnCounter.ABORTED.inc(catalog_proc);
                    break;
                case ABORT_MISPREDICT:
                    if (t) LOG.trace("Telling the TransactionEstimator to IGNORE " + ts);
                    if (t_estimator != null) t_estimator.mispredict(txn_id);
                    if (hstore_conf.site.status_show_txn_info) {
                        (ts.isSpeculative() ? TxnCounter.RESTARTED : TxnCounter.MISPREDICTED).inc(catalog_proc);
                    }
                    break;
                case ABORT_REJECT:
                    if (hstore_conf.site.status_show_txn_info)
                        TxnCounter.REJECTED.inc(catalog_proc);
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
            if (ts.sysproc) {
                TxnCounter.SYSPROCS.inc(catalog_proc);
            } else if (status != Hstore.Status.ABORT_MISPREDICT && ts.isRejected() == false) {
                (ts.isPredictSinglePartition() ? TxnCounter.SINGLE_PARTITION : TxnCounter.MULTI_PARTITION).inc(catalog_proc);
            }
        }
        
        HStoreObjectPools.STATES_TXN_LOCAL.returnObject(ts);
    }

    // ----------------------------------------------------------------------------
    // MAGIC HSTORESITE LAUNCHER
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param hstore_site
     * @param hstore_conf_path
     * @param dtxnengine_path
     * @param dtxncoordinator_path
     * @param dtxncoord_path
     * @param execHost
     * @param execPort
     * @param coordinatorHost
     * @param coordinatorPort
     * @throws Exception
     */
    public static void launch(final HStoreSite hstore_site,
                              final String hstore_conf_path, final String dtxnengine_path, final String dtxncoordinator_path,
                              final String coordinatorHost, final int coordinatorPort) throws Exception {
        List<Runnable> runnables = new ArrayList<Runnable>();
        final Site catalog_site = hstore_site.getSite();
        
        // ----------------------------------------------------------------------------
        // (4) Procedure Request Listener Thread (one per Partition)
        // ----------------------------------------------------------------------------
        List<Partition> p = new ArrayList<Partition>(catalog_site.getPartitions());
        for (int i = 0; i < hstore_site.voltListeners.length; i++) {
            final int id = i;
            final Partition catalog_part = p.get(id);
            final int port = catalog_part.getProc_port();
            
            hstore_site.procEventLoops[id] = new NIOEventLoop();
            hstore_site.voltListeners[id] = new VoltProcedureListener(hstore_site.procEventLoops[id], hstore_site); 
            
            runnables.add(new Runnable() {
                public void run() {
                    final Thread self = Thread.currentThread();
                    self.setName(HStoreSite.getThreadName(hstore_site.site_id, "listen", id));
                    if (hstore_site.getHStoreConf().site.cpu_affinity)
                        hstore_site.getThreadManager().registerProcessingThread();
                    
                    // Then fire off this thread to have it do some work as it comes in 
                    Throwable error = null;
                    try {
                        hstore_site.voltListeners[id].bind(port);
                        hstore_site.procEventLoops[id].setExitOnSigInt(true);
                        hstore_site.ready_latch.countDown();
                        hstore_site.procEventLoops[id].run();
                    } catch (Throwable ex) {
                        if (ex != null && ex.getMessage() != null && ex.getMessage().contains("Connection closed") == false) {
                            error = ex;
                        }
                    }
                    if (error != null && hstore_site.isShuttingDown() == false) {
                        LOG.warn(String.format("Procedure Listener #%d is stopping! [error=%s, hstore_shutdown=%s]",
                                               id, (error != null ? error.getMessage() : null), hstore_site.shutdown_state), error);
                        hstore_site.hstore_coordinator.shutdownCluster(error);
                    }
                };
            });
        } // FOR
        
        // ----------------------------------------------------------------------------
        // (5) HStoreSite Setup Thread
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Starting HStoreSite [site=%d]", hstore_site.getSiteId()));
        hstore_site.ready_latch = new CountDownLatch(runnables.size());
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(HStoreSite.getThreadName(hstore_site, "setup"));
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
                hstore_site.ready();
            }
        });
        
        // This will block the MAIN thread!
        ThreadUtil.runNewPool(runnables);
    }
    
    /**
     * Required Arguments
     * catalog.jar=<path/to/catalog.jar>
     * coordinator.host=<hostname>
     * coordinator.port=<#>
     * 
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                    ArgumentsParser.PARAM_CATALOG,
                    ArgumentsParser.PARAM_COORDINATOR_HOST,
                    ArgumentsParser.PARAM_COORDINATOR_PORT,
                    ArgumentsParser.PARAM_SITE_ID,
                    ArgumentsParser.PARAM_DTXN_CONF,
                    ArgumentsParser.PARAM_DTXN_ENGINE,
                    ArgumentsParser.PARAM_CONF
        );
        
        // HStoreSite Stuff
        final int site_id = args.getIntParam(ArgumentsParser.PARAM_SITE_ID);
        Thread t = Thread.currentThread();
        t.setName(HStoreSite.getThreadName(site_id, "main", null));
        
        final Site catalog_site = CatalogUtil.getSiteFromId(args.catalog_db, site_id);
        if (catalog_site == null) throw new RuntimeException("Invalid site #" + site_id);
        
        HStoreConf.initArgumentsParser(args, catalog_site);
        if (d) LOG.info("HStoreConf Parameters:\n" + HStoreConf.singleton().toString(true));

        // HStoreSite Stuff
        final String coordinatorHost = args.getParam(ArgumentsParser.PARAM_COORDINATOR_HOST);
        final int coordinatorPort = args.getIntParam(ArgumentsParser.PARAM_COORDINATOR_PORT);

        if (FileUtil.exists(args.getParam(ArgumentsParser.PARAM_DTXN_CONF)) == false) {
            throw new IOException("The Dtxn.Coordinator file '" + args.getParam(ArgumentsParser.PARAM_DTXN_CONF) + "' does not exist");
        }
        
        // For every partition in our local site, we want to setup a new ExecutionSite
        // Thankfully I had enough sense to have PartitionEstimator take in the local partition
        // as a parameter, so we can share a single instance across all ExecutionSites
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();

        // ----------------------------------------------------------------------------
        // MarkovGraphs
        // ----------------------------------------------------------------------------
        Map<Integer, MarkovGraphsContainer> markovs = null;
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            File path = new File(args.getParam(ArgumentsParser.PARAM_MARKOV));
            if (path.exists()) {
                markovs = MarkovGraphContainersUtil.loadIds(args.catalog_db, path.getAbsolutePath(), CatalogUtil.getLocalPartitionIds(catalog_site));
                MarkovGraphContainersUtil.setHasher(markovs, p_estimator.getHasher());
                LOG.info("Finished loading MarkovGraphsContainer '" + path + "'");
            } else {
                if (LOG.isDebugEnabled()) LOG.warn("The Markov Graphs file '" + path + "' does not exist");
            }
        }

        // ----------------------------------------------------------------------------
        // Workload Trace Output
        // ----------------------------------------------------------------------------
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT)) {
            ProcedureProfiler.profilingLevel = ProcedureProfiler.Level.INTRUSIVE;
            String traceClass = Workload.class.getName();
            String tracePath = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT) + "-" + site_id;
            String traceIgnore = args.getParam(ArgumentsParser.PARAM_WORKLOAD_PROC_EXCLUDE);
            ProcedureProfiler.initializeWorkloadTrace(args.catalog, traceClass, tracePath, traceIgnore);
            LOG.info("Enabled workload logging '" + tracePath + "'");
        }
        
        // ----------------------------------------------------------------------------
        // Partition Initialization
        // ----------------------------------------------------------------------------
        for (Partition catalog_part : catalog_site.getPartitions()) {
            int local_partition = catalog_part.getId();
            MarkovGraphsContainer local_markovs = null;
            if (markovs != null) {
                if (markovs.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) {
                    local_markovs = markovs.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
                } else {
                    local_markovs = markovs.get(local_partition);
                }
                assert(local_markovs != null) : "Failed to get the proper MarkovGraphsContainer that we need for partition #" + local_partition;
            }

            // Initialize TransactionEstimator stuff
            // Load the Markov models if we were given an input path and pass them to t_estimator
            // HACK: For now we have to create a TransactionEstimator for all partitions, since
            // it is written under the assumption that it was going to be running at just a single partition
            // I'm not proud of this...
            // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
            // stick them into the HStoreSite
            LOG.debug("Creating Estimator for " + HStoreSite.formatSiteName(site_id));
            TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, args.param_mappings, local_markovs);

            // setup the EE
            LOG.debug("Creating ExecutionSite for Partition #" + local_partition);
            ExecutionSite executor = new ExecutionSite(
                    local_partition,
                    args.catalog,
                    BackendTarget.NATIVE_EE_JNI, // BackendTarget.NULL,
                    p_estimator,
                    t_estimator);
            executors.put(local_partition, executor);
        } // FOR
        
        // Now we need to create an HStoreMessenger and pass it to all of our ExecutionSites
        HStoreSite site = new HStoreSite(catalog_site, executors, p_estimator);
        if (args.thresholds != null) site.setThresholds(args.thresholds);
        
        // ----------------------------------------------------------------------------
        // Bombs Away!
        // ----------------------------------------------------------------------------
        LOG.info("Instantiating HStoreSite network connections...");
        HStoreSite.launch(site,
                args.getParam(ArgumentsParser.PARAM_DTXN_CONF), 
                args.getParam(ArgumentsParser.PARAM_DTXN_ENGINE),
                args.getParam(ArgumentsParser.PARAM_DTXN_COORDINATOR),
                coordinatorHost, coordinatorPort);
    }
}
