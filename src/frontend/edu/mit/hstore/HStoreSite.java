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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.ExecutionSitePostProcessor;
import org.voltdb.ProcedureProfiler;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FinishTaskMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.utils.Pair;

import ca.evanjones.protorpc.NIOEventLoop;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
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
import edu.brown.workload.Workload;
import edu.mit.hstore.callbacks.TransactionCleanupCallback;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;
import edu.mit.hstore.callbacks.TransactionRedirectCallback;
import edu.mit.hstore.dtxn.AbstractTransaction;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.RemoteTransaction;
import edu.mit.hstore.estimators.AbstractEstimator;
import edu.mit.hstore.estimators.TM1Estimator;
import edu.mit.hstore.estimators.TPCCEstimator;
import edu.mit.hstore.interfaces.Loggable;
import edu.mit.hstore.interfaces.Shutdownable;
import edu.mit.hstore.util.TransactionQueueManager;
import edu.mit.hstore.util.TxnCounter;

/**
 * 
 * @author pavlo
 */
public class HStoreSite implements VoltProcedureListener.Handler, Shutdownable, Loggable {
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
    private final int LOCAL_PARTITION_OFFSETS[];
    
    /**
     * For a given offset from LOCAL_PARTITION_OFFSETS, this array
     * will contain the partition id
     */
    private final int LOCAL_PARTITION_REVERSE[];
    
    // ----------------------------------------------------------------------------
    // OBJECT POOLS
    // ----------------------------------------------------------------------------

    private final HStoreThreadManager threadManager;
    
    private final TransactionQueueManager txnQueueManager;
    
    /**
     * This is the thing that we will actually use to generate txn ids used by our H-Store specific code
     */
    private final TransactionIdManager txnid_manager;
    
    private final HStoreCoordinator hstore_coordinator;

    /**
     * Local ExecutionSite Stuff
     */
    private final ExecutionSite executors[];
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
    protected final Site catalog_site;
    protected final int site_id;
    protected final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final AbstractHasher hasher;
    
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
    
    /**
     * Fixed Markov Estimator
     */
    private final AbstractEstimator fixed_estimator;
    
    /**
     * Status Monitor
     */
    private HStoreSiteStatus status_monitor = null;
    
    
    /**
     * For whatever...
     */
    
    private final Histogram<Integer> incoming_partitions = new Histogram<Integer>();
    
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
        this.LOCAL_PARTITION_OFFSETS = new int[num_partitions];
        this.LOCAL_PARTITION_REVERSE = new int[this.num_local_partitions];
        int offset = 0;
        for (Integer p : executors.keySet()) {
            this.LOCAL_PARTITION_OFFSETS[p.intValue()] = offset;
            this.LOCAL_PARTITION_REVERSE[offset] = p.intValue(); 
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
        this.txnid_manager = new TransactionIdManager(this.site_id);
        this.single_partition_sets = new Collection[num_partitions];
        
        for (int partition : executors.keySet()) {
            this.executors[partition] = executors.get(partition);
//            this.inflight_txns_ctr[partition] = new AtomicInteger(0); 
            this.single_partition_sets[partition] = Collections.singleton(partition); 
        } // FOR
        this.threadManager = new HStoreThreadManager(this);
        this.voltListener = new VoltProcedureListener(this.procEventLoop, this);
        
        if (hstore_conf.site.status_show_executor_info) {
            this.idle_time.resetOnEvent(this.startWorkload_observable);
        }
        
        if (hstore_conf.site.exec_postprocessing_thread) {
            assert(hstore_conf.site.exec_postprocessing_thread_count > 0);
            if (d)
                LOG.debug("__FILE__:__LINE__ " + String.format("Starting %d post-processing threads", hstore_conf.site.exec_postprocessing_thread_count));
            for (int i = 0; i < hstore_conf.site.exec_postprocessing_thread_count; i++) {
                this.processors.add(new ExecutionSitePostProcessor(this, this.ready_responses));
            } // FOR
        }
        this.hstore_coordinator = new HStoreCoordinator(this);
//        this.helper_pool = Executors.newScheduledThreadPool(1);
        
        // Create all of our parameter manglers
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.param_manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Created ParameterManglers for %d procedures", this.param_manglers.size()));
        
        // HACK
        if (hstore_conf.site.exec_neworder_cheat) {
            if (catalog_db.getProcedures().containsKey("neworder")) {
                this.fixed_estimator = new TPCCEstimator(this);
            } else if (catalog_db.getProcedures().containsKey("UpdateLocation")) {
                this.fixed_estimator = new TM1Estimator(this);
            } else {
                this.fixed_estimator = null;
            }
        } else {
            this.fixed_estimator = null;
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
        return this.LOCAL_PARTITION_OFFSETS[partition];
    }
    
    /**
     * 
     * @param offset
     * @return
     */
    public int getLocalPartitionFromOffset(int offset) {
        return this.LOCAL_PARTITION_REVERSE[offset];
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
//    public ExecutionSiteHelper getExecutionSiteHelper() {
//        return (this.helper);
//    }
    public Collection<ExecutionSitePostProcessor> getExecutionSitePostProcessors() {
        return (this.processors);
    }
    public HStoreCoordinator getCoordinator() {
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
    public TransactionIdManager getTransactionIdManager() {
        return (this.txnid_manager);
    }
    public EstimationThresholds getThresholds() {
        return thresholds;
    }
    private void setThresholds(EstimationThresholds thresholds) {
         this.thresholds = thresholds;
//         if (d) 
         LOG.info("__FILE__:__LINE__ " + "Set new EstimationThresholds: " + thresholds);
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
    protected int getDTXNQueueSize() {
        int ctr = 0;
        for (Integer p : this.local_partitions) {
            ctr += this.txnQueueManager.getQueueSize(p.intValue());
        }
        return (ctr);
    }
    
    /**
     * Get the number of transactions inflight for this partition
     */
    protected int getInflightTxnCount(int partition) {
//        return (this.inflight_txns_ctr[partition].get());
        return (this.txnQueueManager.getQueueSize(partition));
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
        if (d) LOG.debug("__FILE__:__LINE__ " + "Initializing HStoreSite " + this.getSiteName());

        List<ExecutionSite> executor_list = new ArrayList<ExecutionSite>();
        for (int partition : this.local_partitions) {
            executor_list.add(this.getExecutionSite(partition));
        } // FOR
        
        EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        EventObserver<Pair<Thread, Throwable>> observer = new EventObserver<Pair<Thread, Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> arg) {
                LOG.error("__FILE__:__LINE__ " + String.format("Thread %s had an Exception. Halting H-Store Cluster", arg.getFirst().getName()),
                          arg.getSecond());
                hstore_coordinator.shutdownCluster(arg.getSecond(), true);
            }
        };
        handler.addObserver(observer);
        
        // First we need to tell the HStoreMessenger to start-up and initialize its connections
        if (d) LOG.debug("__FILE__:__LINE__ " + "Starting HStoreMessenger for " + this.getSiteName());
        this.hstore_coordinator.start();

        // Start TransactionQueueManager
        Thread t = new Thread(this.txnQueueManager);
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(handler);
        t.start();
        
        // Start Status Monitor
        if (hstore_conf.site.status_interval > 0) {
            if (d) LOG.debug("__FILE__:__LINE__ " + "Starting HStoreSiteStatus monitor thread");
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
//        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Scheduling ExecutionSiteHelper to run every %.1f seconds", hstore_conf.site.helper_interval / 1000f));
//        this.helper = new ExecutionSiteHelper(this,
//                                              executor_list,
//                                              hstore_conf.site.helper_txn_per_round,
//                                              hstore_conf.site.helper_txn_expire,
//                                              hstore_conf.site.txn_profiling);
//        this.helper_pool.scheduleAtFixedRate(this.helper,
//                                             hstore_conf.site.helper_initial_delay,
//                                             hstore_conf.site.helper_interval,
//                                             TimeUnit.MILLISECONDS);
        
        // Then we need to start all of the ExecutionSites in threads
        if (d) LOG.debug("__FILE__:__LINE__ " + "Starting ExecutionSite threads for " + this.local_partitions.size() + " partitions on " + this.getSiteName());
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
        
        if (d) LOG.debug("__FILE__:__LINE__ " + "Preloading cached objects");
        try {
            // Load up everything the QueryPlanUtil
            PlanNodeUtil.preload(this.catalog_db);
            
            // Then load up everything in the PartitionEstimator
            this.p_estimator.preload();
         
            // Don't forget our CatalogUtil friend!
            CatalogUtil.preload(this.catalog_db);
            
        } catch (Exception ex) {
            LOG.fatal("__FILE__:__LINE__ " + "Failed to prepare HStoreSite", ex);
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
            LOG.warn("__FILE__:__LINE__ " + "Already told that we were ready... Ignoring");
            return;
        }
        this.shutdown_state = ShutdownState.STARTED;
        
        // This message must always be printed in order for the BenchmarkController
        // to know that we're ready!
        System.out.println(String.format("%s [site=%s, ports=%s, #partitions=%d]",
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
     * Returns true if this HStoreSite is throttling incoming transactions
     */
//    protected boolean isIncomingThrottled(int partition) {
//        return (this.incoming_throttle[partition]);
//    }
//    protected int getIncomingQueueMax(int partition) {
//        return (this.incoming_queue_max[partition]);
//    }
//    protected int getIncomingQueueRelease(int partition) {
//        return (this.incoming_queue_release[partition]);
//    }
    protected Histogram<Integer> getIncomingPartitionHistogram() {
        return (this.incoming_partitions);
    }
    public ProfileMeasurement getEmptyQueueTime() {
        return (this.idle_time);
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
    // HSTORESTITE SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    public static void crash() {
        if (SHUTDOWN_HANDLE != null) {
            SHUTDOWN_HANDLE.hstore_coordinator.shutdownCluster();
        } else {
            LOG.fatal("__FILE__:__LINE__ " + "H-Store has encountered an unrecoverable error and is exiting.");
            LOG.fatal("__FILE__:__LINE__ " + "The log may contain additional information.");
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
    public void prepareShutdown(boolean error) {
        this.shutdown_state = ShutdownState.PREPARE_SHUTDOWN;
        this.hstore_coordinator.prepareShutdown(false);
        for (ExecutionSitePostProcessor espp : this.processors) {
            espp.prepareShutdown(false);
        } // FOR
        for (int p : this.local_partitions) {
            this.executors[p].prepareShutdown(false);
        } // FOR
//        for (AbstractTransaction ts : this.inflight_txns.values()) {
//            // TODO: Reject all of these
//        }
    }
    
    /**
     * Perform shutdown operations for this HStoreSiteNode
     * This should only be called by HStoreMessenger 
     */
    @Override
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
            if (d) LOG.debug("__FILE__:__LINE__ " + "Already told to shutdown... Ignoring");
            return;
        }
        if (this.shutdown_state != ShutdownState.PREPARE_SHUTDOWN) this.prepareShutdown(false);
        this.shutdown_state = ShutdownState.SHUTDOWN;
//      if (d)
        LOG.info("__FILE__:__LINE__ " + "Shutting down everything at " + this.getSiteName());

        // Stop the monitor thread
        if (this.status_monitor != null) this.status_monitor.shutdown();
        
        // Tell our local boys to go down too
        for (ExecutionSitePostProcessor p : this.processors) {
            p.shutdown();
        }
        for (int p : this.local_partitions) {
            if (t) LOG.trace("__FILE__:__LINE__ " + "Telling the ExecutionSite for partition " + p + " to shutdown");
            this.executors[p].shutdown();
        } // FOR
      
        // Tell anybody that wants to know that we're going down
        if (t) LOG.trace("__FILE__:__LINE__ " + "Notifying " + this.shutdown_observable.countObservers() + " observers that we're shutting down");
        this.shutdown_observable.notifyObservers();
        
        // Stop the helper
//        this.helper_pool.shutdown();
        
        // Tell all of our event loops to stop
        if (t) LOG.trace("__FILE__:__LINE__ " + "Telling Procedure Listener event loops to exit");
        this.procEventLoop.exitLoop();
        
        // if (t) LOG.trace("__FILE__:__LINE__ " + "Telling Dtxn.Engine event loop to exit");
        // this.engineEventLoop.exitLoop();
        
//        if (d) 
            LOG.info("__FILE__:__LINE__ " + "Completed shutdown process at " + this.getSiteName());
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
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Received new stored procedure invocation request for %s [handle=%d, bytes=%d]", catalog_proc.getName(), request.getClientHandle(), serializedRequest.length));

        // Profiling Updates
        if (hstore_conf.site.status_show_txn_info) TxnCounter.RECEIVED.inc(request.getProcName());
        if (hstore_conf.site.exec_profiling && base_partition != -1) {
            this.incoming_partitions.put(base_partition);
        }
        
        // First figure out where this sucker needs to go
        // If it's a sysproc, then it doesn't need to go to a specific partition
        if (sysproc) {
            // HACK: Check if we should shutdown. This allows us to kill things even if the
            // DTXN coordinator is stuck.
            if (catalog_proc.getName().equalsIgnoreCase("@Shutdown")) {
                ClientResponseImpl cresponse = new ClientResponseImpl(1, 1, 1, Hstore.Status.OK, HStoreConstants.EMPTY_RESULT, "");
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
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Using embedded base partition from %s request", request.getProcName()));
            assert(base_partition == request.getBasePartition());    
            
        // Otherwise we use the PartitionEstimator to figure out where this thing needs to go
        } else if (hstore_conf.site.exec_force_localexecution == false) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Using PartitionEstimator for %s request", request.getProcName()));
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
                LOG.trace("__FILE__:__LINE__ " + String.format("Selecting a random local partition to execute %s request [force_local=%s]",
                                        request.getProcName(), hstore_conf.site.exec_force_localexecution));
            base_partition = this.local_partitions.get((int)(Math.abs(request.getClientHandle()) % this.num_local_partitions));
        }
        
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Incoming %s transaction request [handle=%d, partition=%d]",
                                       request.getProcName(), request.getClientHandle(), base_partition));
        
        // -------------------------------
        // REDIRECT TXN TO PROPER PARTITION
        // If the dest_partition isn't local, then we need to ship it off to the right location
        // -------------------------------
        TransactionIdManager id_generator = this.txnid_manager; //  this.txnid_managers[base_partition];
        if (this.single_partition_sets[base_partition] == null) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Forwarding %s request to partition %d", request.getProcName(), base_partition));
            
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
        
        // Grab a new LocalTransactionState object from the target base partition's ExecutionSite object pool
        // This will be the handle that is used all throughout this txn's lifespan to keep track of what it does
        long txn_id = id_generator.getNextUniqueTransactionId();
        LocalTransaction ts = null;
        try {
            ts = HStoreObjectPools.STATES_TXN_LOCAL.borrowObject();
            assert(ts.isInitialized() == false);
        } catch (Throwable ex) {
            LOG.fatal("__FILE__:__LINE__ " + String.format("Failed to instantiate new LocalTransactionState for %s txn #%s",
                                    request.getProcName(), txn_id));
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
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("New request is for a sysproc %s, so it has to be multi-partitioned [clientHandle=%d]",
                                           request.getProcName(), request.getClientHandle()));
            predict_touchedPartitions = this.all_partitions;
            
        // Force all transactions to be single-partitioned
        } else if (hstore_conf.site.exec_force_singlepartitioned) {
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("The \"Always Single-Partitioned\" flag is true. Marking new %s transaction as single-partitioned on partition %d [clientHandle=%d]",
                                           request.getProcName(), base_partition, request.getClientHandle()));
            predict_touchedPartitions = this.single_partition_sets[base_partition];
            
        // Assume we're executing TPC-C neworder. Manually examine the input parameters and figure
        // out what partitions it's going to need to touch
        } else if (hstore_conf.site.exec_neworder_cheat) {
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Using fixed transaction estimator [clientHandle=%d]", request.getClientHandle()));
            if (this.fixed_estimator != null)
                predict_touchedPartitions = this.fixed_estimator.initializeTransaction(catalog_proc, args);
            if (predict_touchedPartitions == null)
                predict_touchedPartitions = this.single_partition_sets[base_partition];
            
        // Otherwise, we'll try to estimate what the transaction will do (if we can)
        } else {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Using TransactionEstimator to check whether new %s request is single-partitioned [clientHandle=%d]",
                                           request.getProcName(), request.getClientHandle()));
            
            // Grab the TransactionEstimator for the destination partition and figure out whether
            // this mofo is likely to be single-partition or not. Anything that we can't estimate
            // will just have to be multi-partitioned. This includes sysprocs
            TransactionEstimator t_estimator = this.executors[base_partition].getTransactionEstimator();
            
            try {
                // HACK: Convert the array parameters to object arrays...
                Object cast_args[] = this.param_manglers.get(catalog_proc).convert(args);
                if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Txn #%d Parameters:\n%s", txn_id, this.param_manglers.get(catalog_proc).toString(cast_args)));
                
                if (hstore_conf.site.txn_profiling) ts.profiler.startInitEstimation();
                t_state = t_estimator.startTransaction(txn_id, base_partition, catalog_proc, cast_args);
                
                // If there is no TransactinEstimator.State, then there is nothing we can do
                // It has to be executed as multi-partitioned
                if (t_state == null) {
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("No TransactionEstimator.State was returned for %s. Executing as multi-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id))); 
                    predict_touchedPartitions = this.all_partitions;
                    
                // We have a TransactionEstimator.State, so let's see what it says...
                } else {
                    if (t) LOG.trace("__FILE__:__LINE__ " + "\n" + StringUtil.box(t_state.toString()));
                    MarkovEstimate m_estimate = t_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a MarkovEstimate for some reason...
                    if (m_estimate == null) {
                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("No MarkovEstimate was found for %s. Executing as multi-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                        predict_touchedPartitions = this.all_partitions;
                        
                    // Invalid MarkovEstimate. Stick with defaults
                    } else if (m_estimate.isValid() == false) {
                        if (d) LOG.warn("__FILE__:__LINE__ " + String.format("Invalid MarkovEstimate for %s. Marking as not read-only and multi-partitioned.\n%s",
                                AbstractTransaction.formatTxnName(catalog_proc, txn_id), m_estimate));
                        predict_readOnly = catalog_proc.getReadonly();
                        predict_abortable = true;
                        predict_touchedPartitions = this.all_partitions;
                        
                    // Use MarkovEstimate to determine things
                    } else {
                        if (d) {
                            LOG.debug("__FILE__:__LINE__ " + String.format("Using MarkovEstimate for %s to determine if single-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                            LOG.debug("__FILE__:__LINE__ " + String.format("%s MarkovEstimate:\n%s", AbstractTransaction.formatTxnName(catalog_proc, txn_id), m_estimate));
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
                LOG.error("__FILE__:__LINE__ " + String.format("Failed calculate estimate for %s request", AbstractTransaction.formatTxnName(catalog_proc, txn_id)), ex);
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
//        ClientResponseImpl cresponse = new ClientResponseImpl(txn_id, request.getClientHandle(), Hstore.Status.OK, HStoreConstants.EMPTY_RESULT, "");
//        this.sendClientResponse(ts, cresponse);
//        if (true) return;
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startTransaction(timestamp);
        if (d) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Initializing %s on partition %d [clientHandle=%d, partitions=%s, readOnly=%s, abortable=%s]",
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
        
        this.dispatchInvocation(ts);
        if (d) LOG.debug("__FILE__:__LINE__ " + "Finished initial processing of " + ts + ". Returning back to listen on incoming socket");
    }

    /**
     * 
     * @param ts
     */
    private void dispatchInvocation(LocalTransaction ts) {
        assert(ts.isInitialized()) : String.format("Unexpected uninitialized LocalTranaction for txn #%d", ts.getTransactionId());
        long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
                
        // For some odd reason we sometimes get duplicate transaction ids from the VoltDB id generator
        // So we'll just double check to make sure that it's unique, and if not, we'll just ask for a new one
        LocalTransaction dupe = (LocalTransaction)this.inflight_txns.put(txn_id, ts);
        if (dupe != null) {
            // HACK!
            this.inflight_txns.put(txn_id, dupe);
            // long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
            long new_txn_id = this.txnid_manager.getNextUniqueTransactionId();
            if (new_txn_id == txn_id) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("__FILE__:__LINE__ " + "ORIG TRANSACTION:\n" + dupe);
                LOG.fatal("__FILE__:__LINE__ " + "NEW TRANSACTION:\n" + ts);
                this.hstore_coordinator.shutdownCluster(new Exception(msg), true);
            }
            LOG.warn("__FILE__:__LINE__ " + String.format("Had to fix duplicate txn ids: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            ts.setTransactionId(txn_id);
            this.inflight_txns.put(txn_id, ts);
        }
        
        // -------------------------------
        // SINGLE-PARTITION TRANSACTION
        // -------------------------------
        if (hstore_conf.site.exec_avoid_coordinator && ts.isPredictSinglePartition()) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Fast path single-partition execution for %s on partition %d [handle=%d]",
                                           ts, base_partition, ts.getClientHandle()));
            this.transactionStart(ts);

        }
        
        // -------------------------------    
        // DISTRIBUTED TRANSACTION
        // -------------------------------
        else {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Queuing distributed %s to running at partition %d [handle=%d]",
                             ts, base_partition, ts.getClientHandle()));
            
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
            assert(predict_touchedPartitions.isEmpty() == false) : "Trying to mark " + ts + " as done at EVERY partition!\n" + ts.debug();

            // Check whether our transaction can't run right now because its id is less than
            // the last seen txnid from the remote partitions that it wants to touch
            for (int partition : predict_touchedPartitions) {
                long last_txn_id = this.txnQueueManager.getLastTransaction(partition); 
                if (txn_id < last_txn_id) {
                    // If we catch it here, then we can just block ourselves until
                    // we generate a txn_id with a greater value and then re-add ourselves
                    if (debug.get()) {
                        LOG.warn("__FILE__:__LINE__ " + String.format("Unable to queue %s because the last txn id at partition %d is %d. Restarting...",
                                       ts, partition, last_txn_id));
                        LOG.warn("__FILE__:__LINE__ " + String.format("LastTxnId:#%s / NewTxnId:#%s",
                                           TransactionIdManager.toString(last_txn_id),
                                           TransactionIdManager.toString(txn_id)));
                    }
                    this.txnQueueManager.queueBlockedDTXN(ts, partition, last_txn_id);
                    return;
                }
            } // FOR
            
            // This callback prevents us from making additional requests to the Dtxn.Coordinator until
            // we get hear back about our our initialization request
            if (hstore_conf.site.txn_profiling) ts.profiler.startCoordinatorBlocked();
            this.hstore_coordinator.transactionInit(ts, ts.getTransactionInitCallback());
        }
    }

    /**
     * 
     * @param txn_id
     * @param callback
     */
    public void transactionInit(long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback) {
        // We should always force a txn from a remote partition into the queue manager
        this.txnQueueManager.insert(txn_id, partitions, callback, true);
    }

    /**
     * 
     * @param ts
     */
    public void transactionStart(LocalTransaction ts) {
        long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
        Procedure catalog_proc = ts.getProcedure();
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Starting %s %s on partition %d",
                        (ts.isPredictSinglePartition() ? "single-partition" : "distributed"), ts, base_partition));
        
        // We have to wrap the StoredProcedureInvocation object into an InitiateTaskMessage so that it can be put
        // into the ExecutionSite's execution queue
        InitiateTaskMessage itask = new InitiateTaskMessage(txn_id, base_partition, base_partition, ts.isPredictReadOnly(), ts.getInvocation());
        
        // Always execute this mofo right away and let each ExecutionSite figure out what it needs to do
        ExecutionSite executor = this.executors[base_partition];
        assert(executor != null) : "No ExecutionSite exists for partition #" + base_partition + " at HStoreSite " + this.site_id;
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startQueue();
        boolean ret = executor.queueNewTransaction(ts, itask);
        if (hstore_conf.site.status_show_txn_info && ret) {
            assert(catalog_proc != null) : String.format("Null Procedure for txn #%d [hashCode=%d]", txn_id, ts.hashCode());
            TxnCounter.EXECUTED.inc(catalog_proc);
        }
    }
    
    public RemoteTransaction createRemoteTransaction(long txn_id, FragmentTaskMessage ftask) {
        RemoteTransaction ts = null;
        try {
            // Remote Transaction
            ts = HStoreObjectPools.STATES_TXN_REMOTE.borrowObject();
            ts.init(txn_id, ftask.getClientHandle(), ftask.getSourcePartitionId(), ftask.isReadOnly(), true);
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Creating new RemoteTransactionState %s from remote partition %d to execute at partition %d [readOnly=%s, singlePartitioned=%s, hashCode=%d]",
                                           ts, ftask.getSourcePartitionId(), ftask.getDestinationPartitionId(), ftask.isReadOnly(), false, ts.hashCode()));
        } catch (Exception ex) {
            LOG.fatal("__FILE__:__LINE__ " + "Failed to construct TransactionState for txn #" + txn_id, ex);
            throw new RuntimeException(ex);
        }
        this.inflight_txns.put(txn_id, ts);
        if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Stored new transaction state for %s at partition %d", ts, ftask.getDestinationPartitionId()));
        return (ts);
    }
    
    /**
     * Execute some work on a particular ExecutionSite
     * @param request
     * @param done
     */
    public void transactionWork(RemoteTransaction ts, FragmentTaskMessage ftask) {
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Queuing FragmentTaskMessage on partition %d for txn #%d",
                                       ftask.getDestinationPartitionId(), ts.getTransactionId()));
        this.executors[ftask.getDestinationPartitionId()].queueWork(ts, ftask);
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
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("2PC:PREPARE Txn #%d [partitions=%s]", txn_id, partitions));
        
        // We could have been asked to participate in a distributed transaction but
        // they never actually sent us anything, so we should just tell the queue manager
        // that the txn is done. There is nothing that we need to do at the ExecutionSites
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        boolean is_local = (ts instanceof LocalTransaction);
        
        int spec_cnt = 0;
        for (Integer p : partitions) {
            if (this.local_partitions.contains(p) == false) continue;
            
            // Always tell the queue stuff that the transaction is finished at this partition
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Telling queue manager that txn #%d is finished at partition %d", txn_id, p));
            this.txnQueueManager.finished(txn_id, Hstore.Status.OK, p.intValue());
            
            // If speculative execution is enabled, then we'll turn it on at the ExecutionSite
            // for this partition
            if (ts != null && hstore_conf.site.exec_speculative_execution) {
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Telling partition %d to enable speculative execution because of txn #%d", p, txn_id));
                boolean ret = this.executors[p.intValue()].enableSpeculativeExecution(ts, false);
                if (debug.get() && ret) {
                    spec_cnt++;
                    LOG.debug("__FILE__:__LINE__ " + String.format("Partition %d - Speculative Execution!", p));
                }
            }
            
            if (updated != null) updated.add(p);
            if (is_local) ((LocalTransaction)ts).getTransactionPrepareCallback().decrementCounter(1);

        } // FOR
        if (debug.get() && spec_cnt > 0)
            LOG.debug("__FILE__:__LINE__ " + String.format("Enabled speculative execution at %d partitions because of waiting for txn #%d", spec_cnt, txn_id));
    }
    
    /**
     * This method is used to finally complete the transaction.
     * The ExecutionSite will either commit or abort the transaction at the specified partitions
     * @param txn_id
     * @param status
     * @param partitions
     */
    public void transactionFinish(long txn_id, Hstore.Status status, Collection<Integer> partitions) {
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("2PC:FINISH Txn #%d [commitStatus=%s, partitions=%s]",
                                       txn_id, status, partitions));
        boolean commit = (status == Hstore.Status.OK);
        
        // If we don't have a AbstractTransaction handle, then we know that we never did anything
        // for this transaction and we can just ignore this finish request. We do have to tell
        // the TransactionQueue manager that we're done though
        AbstractTransaction ts = this.inflight_txns.get(txn_id);
        if (ts != null && ts instanceof RemoteTransaction) {
            TransactionCleanupCallback cleanup_callback = ((RemoteTransaction)ts).getCleanupCallback();
            cleanup_callback.init((RemoteTransaction)ts, status, partitions);
        }
        
        FinishTaskMessage ftask = null;
        for (int p : partitions) {
            if (this.local_partitions.contains(p) == false) continue;
            
            // We only need to tell the queue stuff that the transaction is finished
            // if it's not an commit because there won't be a 2PC:PREPARE message
            if (commit == false) this.txnQueueManager.finished(txn_id, status, p);

            // Then actually commit the transaction in the execution engine
            // We only need to do this for distributed transactions, because all single-partition
            // transactions will commit/abort immediately
            if (ts != null && ts.isPredictSinglePartition() == false && (ts.hasStarted(p) || ts.getBasePartition() == p)) {
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Calling finishTransaction for %s on partition %d", ts, p));
                
                if (ftask == null) ftask = ts.getFinishTaskMessage(status);
                try {
                    this.executors[p].queueFinish(ts, ftask);
                } catch (Throwable ex) {
                    LOG.error("__FILE__:__LINE__ " + String.format("Unexpected error when trying to finish %s\nHashCode: %d / Status: %s / Partitions: %s",
                                            ts, ts.hashCode(), status, partitions));
                    throw new RuntimeException(ex);
                }
            }
        } // FOR            
    }

    /**
     * 
     * @param ts
     * @param cresponse
     * @return
     */
    private ByteBuffer serializeClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        FastSerializer out = new FastSerializer(ExecutionSite.buffer_pool);
        try {
            out.writeObject(cresponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        // Check whether we should disable throttling
        boolean throttle = (cresponse.getStatus() == Hstore.Status.ABORT_THROTTLED);
        int timestamp = this.getNextRequestCounter();
        
        ByteBuffer buffer = ByteBuffer.wrap(out.getBytes());
        ClientResponseImpl.setThrottleFlag(buffer, throttle);
        ClientResponseImpl.setServerTimestamp(buffer, timestamp);
        
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Serialized ClientResponse for %s [throttle=%s, timestamp=%d]",
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
        assert(cresponse != null) : "Missing ClientResponse for " + ts;
        Hstore.Status status = cresponse.getStatus();
        assert(cresponse.getClientHandle() != -1) : "The client handle for " + ts + " was not set properly";
        
        // Don't send anything back if it's a mispredict because it's as waste of time...
        // If the txn committed/aborted, then we can send the response directly back to the
        // client here. Note that we don't even need to call HStoreSite.finishTransaction()
        // since that doesn't do anything that we haven't already done!
        if (status != Hstore.Status.ABORT_MISPREDICT) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Sending back ClientResponse for " + ts));

            // Send result back to client!
            ts.getClientCallback().run(this.serializeClientResponse(ts, cresponse).array());
        }
        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
        // as soon as possible...
        else {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Restarting %s because it mispredicted", ts));
            this.transactionRestart(ts, status);
        }
    }
    
    // ----------------------------------------------------------------------------
    // FAILED TRANSACTIONS (REQUEUE / REJECT / RESTART)
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param ts
     */
    public void transactionRequeue(LocalTransaction ts) {
        long old_txn_id = ts.getTransactionId();

        // Make sure that we remove the old txn
        this.inflight_txns.remove(old_txn_id);
        
        long new_txn_id = this.txnid_manager.getNextUniqueTransactionId();
        ts.setTransactionId(new_txn_id);
        this.dispatchInvocation(ts);
        LOG.info("__FILE__:__LINE__ " + String.format("Released blocked txn #%d as new %s", old_txn_id, ts));
    }
    
    /**
     * 
     * @param ts
     */
    public void transactionReject(LocalTransaction ts, Hstore.Status status) {
        assert(ts.isInitialized());
        int request_ctr = this.getNextRequestCounter();
        long clientHandle = ts.getClientHandle();
       
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Rejecting %s with status %s [clientHandle=%d, requestCtr=%d]",
                                       ts, status, clientHandle, request_ctr));
        
        String statusString = String.format("Transaction was rejected by %s [restarts=%d]", this.getSiteName(), ts.getRestartCounter());
        ClientResponseImpl cresponse = new ClientResponseImpl(ts.getTransactionId(),
                                                              ts.getClientHandle(),
                                                              ts.getBasePartition(),
                                                              status,
                                                              HStoreConstants.EMPTY_RESULT,
                                                              statusString);
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
     * The transaction was mispredicted as single-partitioned
     * This method will perform the following operations:
     *  (1) Restart the transaction as new multi-partitioned transaction
     *  (2) Mark the original transaction as aborted
     * @param txn_id
     * @param orig_callback - the original callback to the client
     */
    public void transactionRestart(LocalTransaction orig_ts, Hstore.Status status) {
        assert(orig_ts != null) : "Null LocalTransaction handle [status=" + status + "]";
        assert(orig_ts.isInitialized()) : "Uninitialized transaction??";
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s got hit with a %s! Going to clean-up our mess and re-execute [restarts=%d]",
                                   orig_ts , status, orig_ts.getRestartCounter()));
        int base_partition = orig_ts.getBasePartition();
        StoredProcedureInvocation spi = orig_ts.getInvocation();
        assert(spi != null) : "Missing StoredProcedureInvocation for " + orig_ts;
        
        // If this txn has been restarted too many times, then we'll just give up
        // and reject it outright
        int restart_limit = (orig_ts.sysproc ? hstore_conf.site.txn_restart_limit_sysproc :
                                               hstore_conf.site.txn_restart_limit);
        if (orig_ts.getRestartCounter() > restart_limit) {
            if (orig_ts.sysproc) {
                throw new RuntimeException(String.format("%s has been restarted %d times! Rejecting...",
                                                         orig_ts, orig_ts.getRestartCounter()));
            } else {
                this.transactionReject(orig_ts, Hstore.Status.ABORT_REJECT);
                return;
            }
        }
        
        // Figure out whether this transaction should be redirected based on what partitions it
        // tried to touch before it was aborted 
        if (status != Hstore.Status.ABORT_RESTART && hstore_conf.site.exec_db2_redirects) {
            Histogram<Integer> touched = orig_ts.getTouchedPartitions();
            Set<Integer> most_touched = touched.getMaxCountValues();
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Touched partitions for mispredicted %s\n%s", orig_ts, touched));
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
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Redirecting mispredicted %s to partition %d", orig_ts, redirect_partition));
                
                spi.setBasePartition(redirect_partition.intValue());
                
                // Add all the partitions that the txn touched before it got aborted
                spi.addPartitions(touched.values());
                
                byte serializedRequest[] = null;
                try {
                    serializedRequest = FastSerializer.serialize(spi);
                } catch (IOException ex) {
                    LOG.fatal("__FILE__:__LINE__ " + "Failed to serialize StoredProcedureInvocation to redirect %s" + orig_ts);
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
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Mispredicted %s has already been aborted once before. Restarting as all-partition txn", orig_ts));
                touched.putAll(this.local_partitions);
            }
        }

        long new_txn_id = this.txnid_manager.getNextUniqueTransactionId();
        LocalTransaction new_ts = null;
        try {
            new_ts = HStoreObjectPools.STATES_TXN_LOCAL.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("__FILE__:__LINE__ " + "Failed to instantiate new LocalTransactionState for mispredicted " + orig_ts);
            throw new RuntimeException(ex);
        }
        
        // Restart the new transaction
        if (hstore_conf.site.txn_profiling) new_ts.profiler.startTransaction(ProfileMeasurement.getTime());
        
        boolean malloc = false;
        Collection<Integer> predict_touchedPartitions = null;
        if (status == Hstore.Status.ABORT_RESTART) {
            predict_touchedPartitions = orig_ts.getPredictTouchedPartitions();
        } else if (orig_ts.getOriginalTransactionId() == null && orig_ts.hasTouchedPartitions()) {
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
        
        if (status == Hstore.Status.ABORT_MISPREDICT && orig_ts.getPendingError() instanceof MispredictionException) {
            MispredictionException ex = (MispredictionException)orig_ts.getPendingError();
            Collection<Integer> partitions = ex.getPartitions().values();
            if (predict_touchedPartitions.containsAll(partitions) == false) {
                if (malloc == false) {
                    predict_touchedPartitions = new HashSet<Integer>(predict_touchedPartitions);
                    malloc = true;
                }
                predict_touchedPartitions.addAll(partitions);
            }
            if (d) LOG.debug("__FILE__:__LINE__ " + orig_ts + " Mispredicted Partitions: " + partitions);
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
        new_ts.init(new_txn_id, base_partition, orig_ts, predict_touchedPartitions, predict_readOnly, predict_abortable);
        new_ts.setRestartCounter(orig_ts.getRestartCounter() + 1);
        
         if (d) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Re-executing %s as new %s-partition %s on partition %d [partitions=%s]",
                                    orig_ts,
                                    (predict_touchedPartitions.size() == 1 ? "single" : "multi"),
                                    new_ts,
                                    base_partition,
                                    predict_touchedPartitions));
            if (t && status == Hstore.Status.ABORT_MISPREDICT)
                LOG.trace("__FILE__:__LINE__ " + String.format("%s Mispredicted partitions\n%s", new_ts, orig_ts.getTouchedPartitions().values()));
        }
        
        this.dispatchInvocation(new_ts);
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
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Adding ClientResponse for %s from partition %d to processing queue [status=%s, size=%d]",
                                       ts, es.getPartitionId(), cr.getStatus(), this.ready_responses.size()));
        this.ready_responses.add(new Object[]{es, ts, cr});
    }

    /**
     * Perform final cleanup and book keeping for a completed txn
     * @param txn_id
     */
    public void completeTransaction(final long txn_id, final Hstore.Status status) {
        if (d) LOG.debug("__FILE__:__LINE__ " + "Cleaning up internal info for txn #" + txn_id);
        AbstractTransaction abstract_ts = this.inflight_txns.remove(txn_id);
        
        // It's ok for us to not have a transaction handle, because it could be
        // for a remote transaction that told us that they were going to need one
        // of our partitions but then they never actually sent work to us
        if (abstract_ts == null) {
            if (d) LOG.warn("__FILE__:__LINE__ " + String.format("Ignoring clean-up request for txn #%d because we don't have a handle [status=%s]",
                                          txn_id, status));
            return;
        }
        
        assert(txn_id == abstract_ts.getTransactionId()) :
            String.format("Mismatched %s - Expected[%d] != Actual[%s]", abstract_ts, txn_id, abstract_ts.getTransactionId());

        // Nothing else to do for RemoteTransactions other than to just
        // return the object back into the pool
        if (abstract_ts instanceof RemoteTransaction) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Returning %s to ObjectPool [hashCode=%d]", abstract_ts, abstract_ts.hashCode()));
            HStoreObjectPools.STATES_TXN_REMOTE.returnObject((RemoteTransaction)abstract_ts);
            return;
        }
        
        final LocalTransaction ts = (LocalTransaction)abstract_ts; 
        final int base_partition = ts.getBasePartition();
        final Procedure catalog_proc = ts.getProcedure();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
       
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
                    if (t) LOG.trace("__FILE__:__LINE__ " + "Telling the TransactionEstimator to COMMIT " + ts);
                    if (t_estimator != null) t_estimator.commit(txn_id);
                    // We always need to keep track of how many txns we process 
                    // in order to check whether we are hung or not
                    if (this.status_monitor != null) TxnCounter.COMPLETED.inc(catalog_proc);
                    break;
                case ABORT_USER:
                    if (t) LOG.trace("__FILE__:__LINE__ " + "Telling the TransactionEstimator to ABORT " + ts);
                    if (t_estimator != null) t_estimator.abort(txn_id);
                    if (hstore_conf.site.status_show_txn_info)
                        TxnCounter.ABORTED.inc(catalog_proc);
                    break;
                case ABORT_MISPREDICT:
                    if (t) LOG.trace("__FILE__:__LINE__ " + "Telling the TransactionEstimator to IGNORE " + ts);
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
                    LOG.warn("__FILE__:__LINE__ " + String.format("Unexpected status %s for %s", status, ts));
            } // SWITCH
        } catch (Throwable ex) {
            LOG.error("__FILE__:__LINE__ " + String.format("Unexpected error when cleaning up %s transaction %s",
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
                (singlePartitioned ? TxnCounter.SINGLE_PARTITION : TxnCounter.MULTI_PARTITION).inc(catalog_proc);
            }
        }
        
        // SANITY CHECK
        for (int p : this.local_partitions) {
            assert(ts.equals(this.executors[p].getCurrentDtxn()) == false) :
                String.format("About to finish %s but it is still the current DTXN at partition %d", ts, p);
        } // FOR
        
        assert(ts.isInitialized()) : "Trying to return uninititlized txn #" + txn_id;
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Returning %s to ObjectPool [hashCode=%d]", ts, ts.hashCode()));
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
    public static void launch(final HStoreSite hstore_site, final String hstore_conf_path) throws Exception {
        List<Runnable> runnables = new ArrayList<Runnable>();
        final Site catalog_site = hstore_site.getSite();
        
        // ----------------------------------------------------------------------------
        // (1) Procedure Request Listener Thread (one per Site)
        // ----------------------------------------------------------------------------
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(HStoreSite.getThreadName(hstore_site, "listen"));
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
                    LOG.warn("__FILE__:__LINE__ " + String.format("Procedure Listener is stopping! [error=%s, hstore_shutdown=%s]",
                                           (error != null ? error.getMessage() : null), hstore_site.shutdown_state), error);
                    hstore_site.hstore_coordinator.shutdownCluster(error);
                }
            };
        });
        
        // ----------------------------------------------------------------------------
        // (5) HStoreSite Setup Thread
        // ----------------------------------------------------------------------------
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Starting HStoreSite [site=%d]", hstore_site.getSiteId()));
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
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Waiting for %d threads to complete initialization tasks", hstore_site.ready_latch.getCount()));
                    try {
                        hstore_site.ready_latch.await();
                    } catch (Exception ex) {
                        LOG.error("__FILE__:__LINE__ " + "Unexpected interuption while waiting for engines to start", ex);
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
                    ArgumentsParser.PARAM_SITE_ID,
                    ArgumentsParser.PARAM_CONF
        );
        
        // HStoreSite Stuff
        final int site_id = args.getIntParam(ArgumentsParser.PARAM_SITE_ID);
        Thread t = Thread.currentThread();
        t.setName(HStoreSite.getThreadName(site_id, "main", null));
        
        final Site catalog_site = CatalogUtil.getSiteFromId(args.catalog_db, site_id);
        if (catalog_site == null) throw new RuntimeException("Invalid site #" + site_id);
        
        HStoreConf.initArgumentsParser(args, catalog_site);
        if (d) LOG.debug("__FILE__:__LINE__ " + "HStoreConf Parameters:\n" + HStoreConf.singleton().toString(true));
        
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
                LOG.info("__FILE__:__LINE__ " + "Finished loading MarkovGraphsContainer '" + path + "'");
            } else if (d) LOG.warn("__FILE__:__LINE__ " + "The Markov Graphs file '" + path + "' does not exist");
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
            LOG.info("__FILE__:__LINE__ " + "Enabled workload logging '" + tracePath + "'");
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
            if (d) LOG.debug("__FILE__:__LINE__ " + "Creating Estimator for " + HStoreSite.formatSiteName(site_id));
            TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, args.param_mappings, local_markovs);

            // setup the EE
            if (d) LOG.debug("__FILE__:__LINE__ " + "Creating ExecutionSite for Partition #" + local_partition);
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
        LOG.info("__FILE__:__LINE__ " + "Instantiating HStoreSite network connections...");
        HStoreSite.launch(site, args.getParam(ArgumentsParser.PARAM_DTXN_CONF));
    }
}
