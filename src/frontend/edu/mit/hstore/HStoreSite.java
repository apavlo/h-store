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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.ExecutionSiteHelper;
import org.voltdb.ExecutionSitePostProcessor;
import org.voltdb.ProcedureProfiler;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoRpcController;
import ca.evanjones.protorpc.ProtoServer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.hashing.AbstractHasher;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.FileUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.Workload;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.FragmentResponse.Status;
import edu.mit.hstore.callbacks.ClientResponseFinalCallback;
import edu.mit.hstore.callbacks.ForwardTxnRequestCallback;
import edu.mit.hstore.callbacks.ForwardTxnResponseCallback;
import edu.mit.hstore.callbacks.InitiateCallback;
import edu.mit.hstore.callbacks.MultiPartitionTxnCallback;
import edu.mit.hstore.callbacks.SinglePartitionTxnCallback;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionState;
import edu.mit.hstore.interfaces.Loggable;
import edu.mit.hstore.interfaces.Shutdownable;

/**
 * 
 * @author pavlo
 */
public class HStoreSite extends Dtxn.ExecutionEngine implements VoltProcedureListener.Handler, Shutdownable, Loggable {
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
    
    public static final String DTXN_COORDINATOR = "protodtxncoordinator";
    public static final String DTXN_ENGINE = "protodtxnengine";
    public static final String SITE_READY_MSG = "Site is ready for action";
    private static final VoltTable EMPTY_RESULT[] = new VoltTable[0];
    
    private static final Map<Long, ByteString> CACHE_ENCODED_TXNIDS = new ConcurrentHashMap<Long, ByteString>();
    public static ByteString encodeTxnId(long txn_id) {
        ByteString bs = HStoreSite.CACHE_ENCODED_TXNIDS.get(txn_id);
        if (bs == null) {
            bs = ByteString.copyFrom(Long.toString(txn_id).getBytes());
            HStoreSite.CACHE_ENCODED_TXNIDS.put(txn_id, bs);
        }
        return (bs);
    }
    public static long decodeTxnId(ByteString bs) {
        return (Long.valueOf(bs.toStringUtf8()));
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

    // ----------------------------------------------------------------------------
    // OBJECT POOLS
    // ----------------------------------------------------------------------------

    /**
     * ForwardTxnRequestCallback Pool
     */
    public static ObjectPool POOL_FORWARDTXN_REQUEST;

    /**
     * ForwardTxnResponseCallback Pool
     */
    public static ObjectPool POOL_FORWARDTXN_RESPONSE;
    
    // ----------------------------------------------------------------------------
    // INTERNAL STUFF
    // ----------------------------------------------------------------------------
    
    private final HStoreThreadManager threadManager;
    
    /**
     * This is the thing that we will actually use to generate txn ids used by our H-Store specific code
     */
    private final TransactionIdManager txnid_managers[];
    
    private final DBBPool buffer_pool = new DBBPool(true, false);
    private final HStoreMessenger messenger;

    /** ProtoServer EventLoop **/
    private final NIOEventLoop protoEventLoop = new NIOEventLoop();

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

    /** PartitionId -> Dtxn.ExecutionEngine */
    private final Dtxn.Coordinator coordinators[];
    private final Dtxn.Partition engine_channels[];
    private final NIOEventLoop engineEventLoop = new NIOEventLoop();

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
    private final EventObservable startWorkload_observable = new EventObservable();
    
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
    private final Dtxn.FragmentResponse cached_FragmentResponse;
    
    /** All of the partitions in the cluster */
    private final Collection<Integer> all_partitions;

    /** List of local partitions at this HStoreSite */
    private final List<Integer> local_partitions = new ArrayList<Integer>();
    private final int num_local_partitions;

    
    /** Request counter **/
    private final AtomicInteger server_timestamp = new AtomicInteger(0); 
    
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
    private final ConcurrentHashMap<Long, LocalTransactionState> inflight_txns = new ConcurrentHashMap<Long, LocalTransactionState>();
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
    public HStoreSite(Site catalog_site, Map<Integer, ExecutionSite> executors, PartitionEstimator p_estimator) throws Exception {
        assert(catalog_site != null);
        assert(p_estimator != null);
        
        // General Stuff
        this.hstore_conf = HStoreConf.singleton();
        this.catalog_site = catalog_site;
        this.site_id = this.catalog_site.getId();
        this.catalog_db = CatalogUtil.getDatabase(this.catalog_site);
        this.all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        this.p_estimator = p_estimator;
        this.hasher = this.p_estimator.getHasher();
        this.thresholds = new EstimationThresholds(); // default values

        final int num_partitions = this.all_partitions.size();
        this.executors = new ExecutionSite[num_partitions];
        this.executor_threads = new Thread[num_partitions];
        this.engine_channels = new Dtxn.Partition[num_partitions];
        this.coordinators = new Dtxn.Coordinator[num_partitions];
        this.txnid_managers = new TransactionIdManager[num_partitions];
        this.inflight_txns_ctr = new AtomicInteger[num_partitions];
        this.incoming_throttle = new boolean[num_partitions];
        this.incoming_throttle_time = new ProfileMeasurement[num_partitions];
        this.incoming_queue_max = new int[num_partitions];
        this.incoming_queue_release = new int[num_partitions];
        
        for (int partition : executors.keySet()) {
            this.executors[partition] = executors.get(partition);
            this.txnid_managers[partition] = new TransactionIdManager(partition);
            this.local_partitions.add(partition);
            this.inflight_txns_ctr[partition] = new AtomicInteger(0); 
            this.incoming_throttle[partition] = false;
            this.incoming_throttle_time[partition] = new ProfileMeasurement("incoming-" + partition);
            this.incoming_queue_max[partition] = hstore_conf.site.txn_incoming_queue_max_per_partition;
            this.incoming_queue_release[partition] = Math.max((int)(this.incoming_queue_max[partition] * hstore_conf.site.txn_incoming_queue_release_factor), 1);
            
            if (hstore_conf.site.status_show_executor_info) {
                this.incoming_throttle_time[partition].resetOnEvent(this.startWorkload_observable);
                // this.redirect_throttle_time.resetOnEvent(this.startWorkload_observable);
            }
            
        } // FOR
        this.num_local_partitions = this.local_partitions.size();
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
        this.messenger = new HStoreMessenger(this);
        this.helper_pool = Executors.newScheduledThreadPool(1);
        
        // Static Object Pools
        if (POOL_FORWARDTXN_REQUEST == null) {
            POOL_FORWARDTXN_REQUEST = new StackObjectPool(new ForwardTxnRequestCallback.Factory(hstore_conf.site.pool_profiling),
                                                          hstore_conf.site.pool_forwardtxnrequests_idle);
        }
        if (POOL_FORWARDTXN_RESPONSE == null) {
            POOL_FORWARDTXN_RESPONSE = new StackObjectPool(new ForwardTxnResponseCallback.Factory(hstore_conf.site.pool_profiling),
                                                           hstore_conf.site.pool_forwardtxnresponses_idle);
        }
        
        // Reusable Cached Messages
        ClientResponseImpl cresponse = new ClientResponseImpl(-1, ClientResponse.REJECTED, EMPTY_RESULT, "", -1);
        this.cached_ClientResponse = ByteBuffer.wrap(FastSerializer.serialize(cresponse));
        this.cached_FragmentResponse = Dtxn.FragmentResponse.newBuilder().setStatus(Status.OK)
                                                                   .setOutput(ByteString.EMPTY)
                                                                   .build();
        
        
        // NewOrder Hack
        if (hstore_conf.site.exec_neworder_cheat) {
            this.tpcc_inspector = new NewOrderInspector(this);
        } else {
            this.tpcc_inspector = null;
        }
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
        return (Collections.unmodifiableCollection(this.processors));
    }
    
    public int getExecutorCount() {
        return (this.local_partitions.size());
    }
    
    public HStoreMessenger getMessenger() {
        return (this.messenger);
    }
    
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    
    /**
     * Return the estimation thresholds used for this HStoreSite 
     * @return
     */
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
     * @return
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
    
    protected int getInflightTxnCount() {
        return (this.inflight_txns.size());
    }
    
    protected int getQueuedResponseCount() {
        return (this.ready_responses.size());
    }
    
    /**
     * Return all of the transactions at this HStoreSite
     * @return
     */
    protected Collection<Entry<Long, LocalTransactionState>> getAllTransactions() {
        return this.inflight_txns.entrySet();
    }
    
    /**
     * Relative marker used 
     * @return
     */
    public int getNextServerTimestamp() {
        return (this.server_timestamp.getAndIncrement());
    }
    
    /**
     * 
     * @param suffix
     * @param partition
     * @return
     */
    public final String getThreadName(String suffix, Integer partition) {
        return (HStoreSite.getThreadName(this.site_id, suffix, partition));
    }
    

    /**
     * Returns a nicely formatted thread name
     * @param suffix
     * @return
     */
    public final String getThreadName(String suffix) {
        return (HStoreSite.getThreadName(this.site_id, suffix, null));
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
        
        // Tell the LoggerUtil thread to register with our HStoreThreadManager
        if (hstore_conf.site.cpu_affinity)
            LoggerUtil.registerThread(this.threadManager);
        
        // First we need to tell the HStoreMessenger to start-up and initialize its connections
        if (d) LOG.debug("Starting HStoreMessenger for " + this.getSiteName());
        this.messenger.start();

        // Create all of our parameter manglers
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.param_manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (d) LOG.debug(String.format("Created ParameterManglers for %d procedures", this.param_manglers.size()));

        // Start Status Monitor
        if (hstore_conf.site.status_interval > 0) {
            if (d) LOG.debug("Starting HStoreSiteStatus monitor thread");
            this.status_monitor = new HStoreSiteStatus(this, hstore_conf);
            Thread t = new Thread(this.status_monitor);
            t.setPriority(Thread.MIN_PRIORITY);
            t.setDaemon(true);
            t.start();
        }
        
        // Start the ExecutionSitePostProcessor
        if (hstore_conf.site.exec_postprocessing_thread) {
            for (ExecutionSitePostProcessor espp : this.processors) {
                Thread t = new Thread(espp);
                t.setDaemon(true);
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

            Thread t = new Thread(executor);
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY); // Probably does nothing...
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
        Collection<Integer> ports = CatalogUtil.getExecutionSitePorts(catalog_site);
        LOG.info(String.format("%s [site=%s, ports=%s, #partitions=%d]",
                               HStoreSite.SITE_READY_MSG,
                               this.getSiteName(),
                               ports,
                               this.getExecutorCount()));
        this.ready = true;
        this.ready_observable.notifyObservers();
    }
    
    /**
     * Returns true if this HStoreSite is ready
     * @return
     */
    public boolean isReady() {
        return (this.ready);
    }
    
    /**
     * Get the Observable handle for this HStoreSite that can alert others when the party is
     * getting started
     * @return
     */
    public EventObservable getReadyObservable() {
        return (this.ready_observable);
    }

    /**
     * Returns true if this HStoreSite is throttling incoming transactions
     */
    public boolean isIncomingThrottled(int partition) {
        return (this.incoming_throttle[partition]);
    }
    public int getIncomingQueueMax(int partition) {
        return (this.incoming_queue_max[partition]);
    }
    public int getIncomingQueueRelease(int partition) {
        return (this.incoming_queue_release[partition]);
    }
    public Histogram<Integer> getIncomingPartitionHistogram() {
        return (this.incoming_partitions);
    }
    public Histogram<String> getIncomingListenerHistogram() {
        return (this.incoming_listeners);
    }
    
    public ProfileMeasurement getEmptyQueueTime() {
        return (this.idle_time);
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
                if (d) LOG.debug(String.format("Disabling INCOMING throttling because txn #%d finished [queue_size=%d, release_threshold=%d]",
                                               txn_id, queue_size, this.incoming_queue_release));
            }
        }
        return (this.incoming_throttle[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // HSTORESTITE SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    public static void crash() {
        if (SHUTDOWN_HANDLE != null) {
            SHUTDOWN_HANDLE.messenger.shutdownCluster();
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
        this.messenger.prepareShutdown();
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
//        this.voltListener.close();
        if (t) LOG.trace("Telling Dtxn.Engine event loop to exit");
        this.engineEventLoop.exitLoop();
        
//        if (d) 
            LOG.info("Completed shutdown process at " + this.getSiteName());
    }
    
//    private String dumpTransaction(LocalTransactionState ts) {
//        final Object args[] = ts.getInvocation().getParams().toArray();
//        Procedure catalog_proc = ts.getProcedure();
//        Object cast_args[] = this.param_manglers.get(catalog_proc).convert(args);
//        
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < cast_args.length; i++) {
//            sb.append("[" + i + "] ");
//            if (catalog_proc.getParameters().get(i).getIsarray()) {
//                sb.append(Arrays.toString((Object[])cast_args[i]));
//            } else {
//                sb.append(cast_args[i]);
//            }
//            sb.append("\n");
//        } // FOR
//        sb.append(ts.toString());
//        return (sb.toString());
//    }
    
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
    public void addShutdownObservable(Observer observer) {
        this.shutdown_observable.addObserver(observer);
    }

    // ----------------------------------------------------------------------------
    // EXECUTION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void procedureInvocation(byte[] serializedRequest, RpcCallback<byte[]> done) {
        long timestamp = (hstore_conf.site.txn_profiling ? ProfileMeasurement.getTime() : -1);
        
        ByteBuffer buffer = ByteBuffer.wrap(serializedRequest);
        boolean sysproc = StoredProcedureInvocation.isSysProc(buffer);
        int base_partition = StoredProcedureInvocation.getBasePartition(buffer);
        
        if (hstore_conf.site.exec_profiling) {
            if (base_partition != -1)
                this.incoming_partitions.put(base_partition);
            this.incoming_listeners.put(Thread.currentThread().getName());
        }
        
        // The serializedRequest is a ProcedureInvocation object
        StoredProcedureInvocation request = null;
        FastDeserializer fds = new FastDeserializer(serializedRequest);
        try {
            request = fds.readObject(StoredProcedureInvocation.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        if (hstore_conf.site.status_show_txn_info) TxnCounter.RECEIVED.inc(request.getProcName());
        if (request == null) {
            throw new RuntimeException("Failed to get ProcedureInvocation object from request bytes");
        }

        // Extract the stuff we need to figure out whether this guy belongs at our site
        request.buildParameterSet();
        assert(request.getParams() != null) : "The parameters object is null for new txn from client #" + request.getClientHandle();
        final Object args[] = request.getParams().toArray(); 
        final Procedure catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(request.getProcName());
        if (catalog_proc == null) throw new RuntimeException("Unknown procedure '" + request.getProcName() + "'");
        if (d) LOG.debug(String.format("Received new stored procedure invocation request for %s [handle=%d, bytes=%d]", catalog_proc.getName(), request.getClientHandle(), serializedRequest.length));
        
        // First figure out where this sucker needs to go
        // If it's a sysproc, then it doesn't need to go to a specific partition
        if (sysproc) {
            // HACK: Check if we should shutdown. This allows us to kill things even if the
            // DTXN coordinator is stuck.
            if (catalog_proc.getName().equalsIgnoreCase("@Shutdown")) {
                ClientResponseImpl cresponse = new ClientResponseImpl(1, ClientResponse.SUCCESS, new VoltTable[0], "");
                FastSerializer out = new FastSerializer();
                try {
                    out.writeObject(cresponse);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                done.run(out.getBytes());
                // Non-blocking....
                this.messenger.shutdownCluster(new Exception("Shutdown command received at " + this.getSiteName()), false);
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
        
        if (d) LOG.debug(String.format("%s Invocation [handle=%d, partition=%d]",
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
            ForwardTxnRequestCallback callback = null;
            try {
                callback = (ForwardTxnRequestCallback)HStoreSite.POOL_FORWARDTXN_REQUEST.borrowObject();
                callback.init(done);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get ForwardTxnRequestCallback", ex);
            }
            
            // Mark this request as having been redirected
            assert(request.hasBasePartition() == false) : "Trying to redirect " + request.getProcName() + " transaction more than once!";
            StoredProcedureInvocation.markRawBytesAsRedirected(base_partition, serializedRequest);
            
            this.messenger.forwardTransaction(serializedRequest, callback, base_partition);
            if (hstore_conf.site.status_show_txn_info) TxnCounter.REDIRECTED.inc(catalog_proc);
            return;
        }
        
        // Check whether we're throttled and should just reject this transaction
        //  (1) It's not a sysproc
        //  (2) It's a new txn request and we're throttled on incoming requests
        //  (2) It's a redirect request and we're throttled on redirects
        if (sysproc == false && this.incoming_throttle[base_partition]) {
            String procName = StoredProcedureInvocation.getProcedureName(buffer);
            if (hstore_conf.site.status_show_txn_info) TxnCounter.RECEIVED.inc(procName);
            int request_ctr = this.getNextServerTimestamp();
            long clientHandle = StoredProcedureInvocation.getClientHandle(buffer);
            
            if (d)
                LOG.debug(String.format("Throttling is enabled. Rejecting transaction and asking client to wait [clientHandle=%d, requestCtr=%d]",
                                        clientHandle, request_ctr));
            
            synchronized (this.cached_ClientResponse) {
                ClientResponseImpl.setServerTimestamp(this.cached_ClientResponse, request_ctr);
                ClientResponseImpl.setThrottleFlag(this.cached_ClientResponse, this.incoming_throttle[base_partition]);
                ClientResponseImpl.setClientHandle(this.cached_ClientResponse, clientHandle);
                done.run(this.cached_ClientResponse.array());
            } // SYNCH
            if (hstore_conf.site.status_show_txn_info) TxnCounter.REJECTED.inc(procName);
            return;
        }
        
        
        // Grab a new LocalTransactionState object from the target base partition's ExecutionSite object pool
        // This will be the handle that is used all throughout this txn's lifespan to keep track of what it does
        long txn_id = id_generator.getNextUniqueTransactionId();
        LocalTransactionState ts = null;
        try {
            ts = (LocalTransactionState)this.executors[base_partition].localTxnPool.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for txn #" + txn_id);
            throw new RuntimeException(ex);
        }
        
        // Disable transaction profiling for sysprocs
        if (hstore_conf.site.txn_profiling && sysproc) ts.profiler.disableProfiling();
        
        // -------------------------------
        // TRANSACTION EXECUTION PROPERTIES
        // -------------------------------
        
        boolean predict_singlePartitioned = false;
        boolean predict_abortable = (hstore_conf.site.exec_no_undo_logging_all == false);
        boolean predict_readOnly = catalog_proc.getReadonly();
        TransactionEstimator.State t_state = null; 
        
        // Sysprocs are always multi-partitioned
        if (sysproc) {
            if (t) LOG.trace(String.format("%s is a sysproc, so it has to be multi-partitioned", ts));
            predict_singlePartitioned = false;
            
        // Force all transactions to be single-partitioned
        } else if (hstore_conf.site.exec_force_singlepartitioned) {
            if (t) LOG.trace("The \"Always Single-Partitioned\" flag is true. Marking as single-partitioned!");
            predict_singlePartitioned = true;
            
        // Assume we're executing TPC-C neworder. Manually examine the input parameters and figure
        // out what partitions it's going to need to touch
        } else if (hstore_conf.site.exec_neworder_cheat && catalog_proc.getName().equalsIgnoreCase("neworder")) {
            if (t) LOG.trace(String.format("%s - Executing neworder argument hack for VLDB paper", ts));
            predict_singlePartitioned = this.tpcc_inspector.initializeTransaction(ts, args);
            
        // Otherwise, we'll try to estimate what the transaction will do (if we can)
        } else {
            if (d) LOG.debug(String.format("Using TransactionEstimator to check whether %s is single-partition", ts));
            
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
                    if (d) LOG.debug(String.format("No TransactionEstimator.State was returned for %s. Executing as multi-partitioned", TransactionState.formatTxnName(catalog_proc, txn_id))); 
                    predict_singlePartitioned = false;
                    
                // We have a TransactionEstimator.State, so let's see what it says...
                } else {
                    if (t) LOG.trace("\n" + StringUtil.box(t_state.toString()));
                    MarkovEstimate m_estimate = t_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a MarkovEstimate for some reason...
                    if (m_estimate == null) {
                        if (d) LOG.debug(String.format("No MarkovEstimate was found for %s. Executing as multi-partitioned", TransactionState.formatTxnName(catalog_proc, txn_id)));
                        predict_singlePartitioned = false;
                        
                    // Invalid MarkovEstimate. Stick with defaults
                    } else if (m_estimate.isValid() == false) {
                        if (d) LOG.warn(String.format("Invalid MarkovEstimate for %s. Marking as not read-only and multi-partitioned.\n%s",
                                TransactionState.formatTxnName(catalog_proc, txn_id), m_estimate));
                        predict_singlePartitioned = false;
                        predict_readOnly = catalog_proc.getReadonly();
                        predict_abortable = true;
                        
                    // Use MarkovEstimate to determine things
                    } else {
                        if (d) {
                            LOG.debug(String.format("Using MarkovEstimate for %s to determine if single-partitioned", TransactionState.formatTxnName(catalog_proc, txn_id)));
                            LOG.debug(String.format("%s MarkovEstimate:\n%s", TransactionState.formatTxnName(catalog_proc, txn_id), m_estimate));
                        }
                        predict_singlePartitioned = m_estimate.isSinglePartition(this.thresholds);
                        predict_readOnly = m_estimate.isReadOnlyAllPartitions(this.thresholds);
                        predict_abortable = (predict_singlePartitioned == false || m_estimate.isAbortable(this.thresholds)); // || predict_readOnly == false
//                        if (catalog_proc.getName().startsWith("payment") && predict_abortable == true) {
//                            LOG.info(catalog_proc.getName() + " -> predict_singlePartition = " + predict_singlePartitioned);
//                            LOG.info(catalog_proc.getName() + " -> predict_readonly        = " + predict_readOnly);
//                            LOG.info(catalog_proc.getName() + " -> isAbortable             =  " + m_estimate.isAbortable(this.thresholds));
//                            LOG.info("MARKOV ESTIMATE:\n" + m_estimate);
//                            MarkovGraph markov = t_state.getMarkovGraph();
//                            GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(t_state.getInitialPath()));
//                            gv.highlightPath(markov.getPath(t_state.getActualPath()), "blue");
//                            System.err.println("WROTE MARKOVGRAPH: " + gv.writeToTempFile(catalog_proc));
//                            this.messenger.shutdownCluster(new RuntimeException("BUSTED!"), false);
//                        }
                    }
                }
            } catch (Throwable ex) {
                if (t_state != null) {
                    MarkovGraph markov = t_state.getMarkovGraph();
                    GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(t_state.getInitialPath()));
                    gv.highlightPath(markov.getPath(t_state.getActualPath()), "blue");
                    System.err.println("WROTE MARKOVGRAPH: " + gv.writeToTempFile(catalog_proc));
                }
                LOG.error(String.format("Failed calculate estimate for %s request", TransactionState.formatTxnName(catalog_proc, txn_id)), ex);
                predict_singlePartitioned = false;
                predict_readOnly = false;
                predict_abortable = true;
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopInitEstimation();
            }
        }
        
        // If this is the first non-sysproc transaction that we've seen, then
        // we will notify anybody that is waiting for this event. This is used to clear
        // out any counters or profiling information that got recorded when we were loading data
        if (this.startWorkload == false && sysproc == false) {
            synchronized (this) {
                if (this.startWorkload == false) {
                    if (d) LOG.debug(String.format("First non-sysproc transaction request recieved. Notifying %d observers [proc=%s]",
                                                   this.startWorkload_observable.countObservers(), catalog_proc.getName()));
                    this.startWorkload = true;
                    this.startWorkload_observable.notifyObservers();
                }
            } // SYNCH
        }

        ts.init(txn_id, request.getClientHandle(), base_partition,
                        predict_singlePartitioned, predict_readOnly, predict_abortable,
                        t_state, catalog_proc, request, done);
        if (hstore_conf.site.txn_profiling) ts.profiler.startTransaction(timestamp);
        if (d) {
            LOG.debug(String.format("Executing %s on partition %d [singlePartition=%s, readOnly=%s, abortable=%s, handle=%d]",
                                    ts, base_partition,
                                    predict_singlePartitioned, predict_readOnly, predict_abortable,
                                    request.getClientHandle()));
        }
        if (this.inflight_txns_ctr[base_partition].getAndIncrement() == 0 && hstore_conf.site.status_show_executor_info) {
            ProfileMeasurement.stop(true, idle_time);
        }
        this.initializeInvocation(ts);
    }

    /**
     * 
     * @param ts
     */
    private void initializeInvocation(LocalTransactionState ts) {
        long txn_id = ts.getTransactionId();
        Long orig_txn_id = ts.getOriginalTransactionId();
        int base_partition = ts.getBasePartition();
        boolean single_partitioned = ts.isPredictSinglePartition();
                
        // For some odd reason we sometimes get duplicate transaction ids from the VoltDB id generator
        // So we'll just double check to make sure that it's unique, and if not, we'll just ask for a new one
        LocalTransactionState dupe = this.inflight_txns.put(txn_id, ts);
        if (dupe != null) {
            // HACK!
            this.inflight_txns.put(txn_id, dupe);
            long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
            if (new_txn_id == txn_id) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("ORIG TRANSACTION:\n" + dupe);
                LOG.fatal("NEW TRANSACTION:\n" + ts);
                this.messenger.shutdownCluster(new Exception(msg), true);
            }
            LOG.warn(String.format("Had to fix duplicate txn ids: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            ts.setTransactionId(txn_id);
            this.inflight_txns.put(txn_id, ts);
        }
        
        // We have to wrap the StoredProcedureInvocation object into an InitiateTaskMessage so that it can be put
        // into the ExecutionSite's execution queue
        InitiateTaskMessage wrapper = new InitiateTaskMessage(txn_id, base_partition, base_partition, ts.isPredictReadOnly(), ts.invocation);
        
        // -------------------------------
        // FAST MODE: Skip the Dtxn.Coordinator
        // -------------------------------
        if (hstore_conf.site.exec_avoid_coordinator && single_partitioned) {
            ts.ignore_dtxn = true;
            ts.init_wrapper = wrapper;
            SinglePartitionTxnCallback init_callback = new SinglePartitionTxnCallback(this, ts, base_partition, ts.client_callback);
            
            // Always execute this mofo right away and let each ExecutionSite figure out what it needs to do
            this.executeTransaction(ts, ts.init_wrapper, init_callback);        
            
            if (d) LOG.debug(String.format("Fast path single-partition execution for %s on partition %d [handle=%d]",
                                           ts, base_partition, ts.getClientHandle()));
            
        // -------------------------------
        // CANADIAN MODE: Single-Partition
        // -------------------------------
        } else if (single_partitioned) {
            // Construct a message that goes directly to the Dtxn.Engine for the destination partition
            if (d) LOG.debug(String.format("Passing single-partition %s to Dtxn.Engine for partition %d [handle=%d]",
                                           ts, base_partition, ts.getClientHandle()));

            Dtxn.DtxnPartitionFragment.Builder requestBuilder = Dtxn.DtxnPartitionFragment.newBuilder();
            requestBuilder.setTransactionId(txn_id);
            requestBuilder.setCommit(Dtxn.DtxnPartitionFragment.CommitState.LOCAL_COMMIT);
            requestBuilder.setPayload(HStoreSite.encodeTxnId(txn_id));
            requestBuilder.setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array()));
            
            RpcCallback<Dtxn.FragmentResponse> callback = new SinglePartitionTxnCallback(this, ts, base_partition, ts.client_callback);
            if (hstore_conf.site.txn_profiling) ts.profiler.startCoordinatorBlocked();
            this.engine_channels[base_partition].execute(ts.rpc_request_init, requestBuilder.build(), callback);

            if (t) LOG.trace("Using SinglePartitionTxnCallback for " + ts);
            
        // -------------------------------    
        // CANADIAN MODE: Multi-Partition
        // -------------------------------
        } else {
            // Construct the message for the Dtxn.Coordinator
            if (d) LOG.debug(String.format("Passing multi-partition %s to Dtxn.Coordinator for partition %d [handle=%d]",
                                           ts, base_partition, ts.getClientHandle()));
            
            // Since we know that this txn came over from the Dtxn.Coordinator, we'll throw it in
            // our set of coordinator txns. This way we can prevent ourselves from executing
            // single-partition txns straight at the ExecutionSite
//            if (d && dtxn_txns.isEmpty()) LOG.debug(String.format("Enabling CANADIAN mode [txn=#%d]", txn_id));
//            dtxn_txns.add(txn_id);
            
            Dtxn.CoordinatorFragment.Builder requestBuilder = Dtxn.CoordinatorFragment.newBuilder();
            
            // Note that we pass the fake txn id to the Dtxn.Coordinator. 
            requestBuilder.setTransactionId(txn_id);
            
            // Whether this transaction is single-partitioned or not
            requestBuilder.setLastFragment(single_partitioned);
            
            Set<Integer> done_partitions = ts.getDonePartitions();
            
            // TransactionEstimator
            // If we know we're single-partitioned, then we *don't* want to tell the Dtxn.Coordinator
            // that we're done at any partitions because it will throw an error
            // Instead, if we're not single-partitioned then that's that only time that 
            // we Tell the Dtxn.Coordinator that we are finished with partitions if we have an estimate
            TransactionEstimator.State s = ts.getEstimatorState(); 
            if (orig_txn_id == null && s != null && s.getInitialEstimate() != null) {
                MarkovEstimate est = s.getInitialEstimate();
                assert(est != null);
                Set<Integer> touched_partitions = est.getTouchedPartitions(this.thresholds);
                for (Integer p : this.all_partitions) {
                    // Make sure that we don't try to mark ourselves as being done at our own partition
                    if (touched_partitions.contains(p) == false && p.intValue() != base_partition) done_partitions.add(p);
                } // FOR
            }
            
            for (Integer p : done_partitions) {
                requestBuilder.addDonePartition(p.intValue());
            } // FOR
            assert(done_partitions.size() != this.all_partitions.size()) : "Trying to mark " + ts + " as done at EVERY partition!";
            if (d && requestBuilder.getDonePartitionCount() > 0) {
                LOG.debug(String.format("Marked %s as done at %d partitions: %s", ts, requestBuilder.getDonePartitionCount(), requestBuilder.getDonePartitionList()));
            }

            // NOTE: Evan betrayed our love so we can't use his txn ids because they are meaningless to us
            // So we're going to pack in our txn id in the payload. Any message they we get from Evan
            // will have this payload so that we can figure out what the hell is going on...
            requestBuilder.setPayload(HStoreSite.encodeTxnId(txn_id));
    
            // Pack the StoredProcedureInvocation into a Dtxn.PartitionFragment
            requestBuilder.addFragment(Dtxn.CoordinatorFragment.PartitionFragment.newBuilder()
                    .setPartitionId(base_partition)
                    .setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array())));
            
            // This latch prevents us from making additional requests to the Dtxn.Coordinator until
            // we get hear back about our our initialization request
            if (t) LOG.trace("Using InitiateCallback for " + ts);
            RpcCallback<Dtxn.CoordinatorResponse> callback = new InitiateCallback(this, txn_id, ts.init_latch);
            
            Dtxn.CoordinatorFragment dtxn_request = requestBuilder.build();
            if (hstore_conf.site.txn_profiling) ts.profiler.startCoordinatorBlocked();
            this.coordinators[base_partition].execute(new ProtoRpcController(), dtxn_request, callback); // txn_info.rpc_request_init
            
            if (d) LOG.debug(String.format("Sent Dtxn.CoordinatorFragment for %s [bytes=%d]", ts, dtxn_request.getSerializedSize()));
            
        }
        
        // Look at the number of inflight transactions and see whether we should block and wait for the 
        // queue to drain for a bit
        int queue_size = this.inflight_txns.size(); //  - this.ready_responses.size();
        if (this.incoming_throttle[base_partition] == false && queue_size > this.incoming_queue_max[base_partition]) {
            if (d) LOG.debug(String.format("INCOMING overloaded because of %s. Waiting for queue to drain [size=%d, trigger=%d]",
                                           ts, queue_size, this.incoming_queue_release));
            this.incoming_throttle[base_partition] = true;
            if (hstore_conf.site.status_show_executor_info) 
                ProfileMeasurement.start(true, this.incoming_throttle_time[base_partition]);
            
            // HACK: Randomly discard some distributed TransactionStates because we can't
            // tell the Dtxn.Coordinator to prune its queue.
            if (hstore_conf.site.txn_enable_queue_pruning && rand.nextBoolean() == true) {
                int ctr = 0;
                for (Long dtxn_id : this.inflight_txns.keySet()) {
                    LocalTransactionState _ts = this.inflight_txns.get(dtxn_id);
                    if (_ts == null) continue;
                    if (_ts.isPredictSinglePartition() == false && _ts.hasStarted() == false && rand.nextInt(10) == 0) {
                        _ts.markAsRejected();
                        ctr++;
                    }
                } // FOR
                if (d && ctr > 0) LOG.debug("Pruned " + ctr + " queued distributed transactions to try to free up the queue");
            }
        }
//        if (this.redirect_throttle[base_partition] == false && queue_size > this.redirect_queue_max) {
//            if (d) LOG.debug(String.format("REDIRECT overloaded because of %s. Waiting for queue to drain [size=%d, trigger=%d]",
//                                           ts, queue_size, this.redirect_queue_release));
//            this.redirect_throttle[base_partition] = true;
//            if (hstore_conf.site.status_show_executor_info) redirect_throttle_time.start();
//        }
    }
    
    /**
     * Execute some work on a particular ExecutionSite
     */
    @Override
    public void execute(RpcController controller, Dtxn.Fragment request, RpcCallback<Dtxn.FragmentResponse> done) {
        // Decode the procedure request
        TransactionInfoBaseMessage msg = null;
        try {
            msg = (TransactionInfoBaseMessage)VoltMessage.createMessageFromBuffer(request.getWork().asReadOnlyByteBuffer(), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        long txn_id = msg.getTxnId();
        int base_partition = msg.getDestinationPartitionId();
        
        if (d) {
            LOG.debug(String.format("Got %s message for txn #%d [partition=%d]", msg.getClass().getSimpleName(), txn_id, base_partition));
            if (t) LOG.trace("CONTENTS:\n" + msg);
        }
        
        // If we have an InitiateTaskMessage, then this call is for starting a new txn
        // at this site. If this is a multi-partitioned transaction, then we need to send
        // back a placeholder response through the Dtxn.Coordinator so that we are allowed to
        // execute queries on remote partitions later.
        // Note that we maintain the callback to the client so that we know how to send back
        // our ClientResponse once the txn is finished.
        if (msg instanceof InitiateTaskMessage) {
            LocalTransactionState ts = this.inflight_txns.get(txn_id);
            assert(ts != null) : String.format("Missing TransactionState for txn #%d at site %d", txn_id, this.site_id);
            if (hstore_conf.site.txn_profiling) ts.profiler.stopCoordinatorBlocked();
            
            // HACK: Reject the transaction
            if (hstore_conf.site.txn_enable_queue_pruning && ts.isRejected()) {
                this.rejectTransaction(ts, done);
                return;
            }
            
            // Inject the StoredProcedureInvocation because we're not going to send it over the wire
            InitiateTaskMessage task = (InitiateTaskMessage)msg;
            assert(task.getStoredProcedureInvocation() == null);
            task.setStoredProcedureInvocation(ts.invocation);
            this.executeTransaction(ts, task, done);
        
        // This is work from a transaction executing at another node
        // Any other message can just be sent along to the ExecutionSite without sending
        // back anything right away. The ExecutionSite will use our callback handle
        // to send back whatever response it needs to on its own.
        } else if (msg instanceof FragmentTaskMessage) {
            if (t) LOG.trace("Executing remote FragmentTaskMessage for txn #" + txn_id);
            this.executors[base_partition].doWork((FragmentTaskMessage)msg, done);
        } else {
            assert(false) : "Unexpected message type: " + msg.getClass().getSimpleName();
        }
    }

    /**
     * 
     * @param ts
     * @param done
     */
    private void rejectTransaction(LocalTransactionState ts, RpcCallback<Dtxn.FragmentResponse> done) {
        // Send back the initial response to the Dtxn.Coordinator
        done.run(cached_FragmentResponse);
        
        // We then need to tell the coordinator that we committed
        ByteString payload = null;
        synchronized (this.cached_ClientResponse) {
            ClientResponseImpl.setThrottleFlag(this.cached_ClientResponse, this.incoming_throttle[ts.getBasePartition()]);
            ClientResponseImpl.setClientHandle(this.cached_ClientResponse, ts.getClientHandle());
            this.cached_ClientResponse.rewind();
            payload = ByteString.copyFrom(this.cached_ClientResponse);
        } // SYNCH
        
        Dtxn.FinishRequest request = Dtxn.FinishRequest.newBuilder().setTransactionId(ts.getTransactionId())
                                                                    .setCommit(false)
                                                                    .setPayload(payload)
                                                                    .build();
        ClientResponseFinalCallback callback = new ClientResponseFinalCallback(this,
                                                                               ts.getTransactionId(),
                                                                               ts.getBasePartition(),
                                                                               payload.toByteArray(),
                                                                               Dtxn.FragmentResponse.Status.OK,
                                                                               ts.client_callback);
        this.requestFinish(ts, request, callback);
    }
    
    /**
     * 
     * @param executor
     * @param ts
     * @param task
     * @param done
     */
    private void executeTransaction(LocalTransactionState ts, InitiateTaskMessage task, RpcCallback<Dtxn.FragmentResponse> done) {
        long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
        boolean single_partitioned = ts.isPredictSinglePartition();
        assert(ts.client_callback != null) : "Missing original RpcCallback for " + ts;
        RpcCallback<Dtxn.FragmentResponse> callback = null;
        
        ExecutionSite executor = this.executors[base_partition];
        assert(executor != null) : "No ExecutionSite exists for partition #" + base_partition + " at HStoreSite " + this.site_id;
  
        // If we're single-partitioned, then we don't want to send back a callback now.
        // The response from the ExecutionSite should be the only response that we send back to the Dtxn.Coordinator
        // This limits the number of network roundtrips that we have to do...
        if (ts.ignore_dtxn == false && single_partitioned == false) { //  || (txn_info.sysproc == false && hstore_conf.site.ignore_dtxn == false)) {
            // We need to send back a response before we actually start executing to avoid a race condition    
            if (d) LOG.debug(String.format("Sending back Dtxn.FragmentResponse to the InitiateTaskMessage for %s", ts));    
            done.run(cached_FragmentResponse);
            callback = new MultiPartitionTxnCallback(this, ts, ts.client_callback);
        } else {
            if (ts.init_latch != null) ts.init_latch.countDown();
            callback = done;
        }
        ts.setCoordinatorCallback(callback);
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startQueue();
        executor.doWork(task, callback, ts);
        
        if (hstore_conf.site.status_show_txn_info) {
            assert(ts.getProcedure() != null) : "Null Procedure for txn #" + txn_id;
            TxnCounter.EXECUTED.inc(ts.getProcedure());
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
    public void misprediction(LocalTransactionState orig_ts, RpcCallback<byte[]> orig_callback) {
        if (d) LOG.debug(orig_ts + " was mispredicted! Going to clean-up our mess and re-execute");
        int base_partition = orig_ts.getBasePartition();
        StoredProcedureInvocation spi = orig_ts.invocation;
        assert(spi != null) : "Missing StoredProcedureInvocation for " + orig_ts;
        
        final Histogram<Integer> touched = orig_ts.getTouchedPartitions();
        
        // Figure out whether this transaction should be redirected based on what partitions it
        // tried to touch before it was aborted 
        if (hstore_conf.site.exec_db2_redirects) {
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
                    this.messenger.shutdownCluster(ex, false);
                    return;
                }
                assert(serializedRequest != null);
                
                ForwardTxnRequestCallback callback;
                try {
                    callback = (ForwardTxnRequestCallback)HStoreSite.POOL_FORWARDTXN_REQUEST.borrowObject();
                    callback.init(orig_callback);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to get ForwardTxnRequestCallback", ex);   
                }
                this.messenger.forwardTransaction(serializedRequest, callback, redirect_partition);
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
        LocalTransactionState new_ts = null;
        try {
            ExecutionSite executor = this.executors[base_partition];
            new_ts = (LocalTransactionState)executor.localTxnPool.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for mispredicted " + orig_ts);
            throw new RuntimeException(ex);
        }
        
        // Restart the new transaction
        if (hstore_conf.site.txn_profiling) new_ts.profiler.startTransaction(ProfileMeasurement.getTime());
        boolean predict_singlePartitioned = (orig_ts.getOriginalTransactionId() == null && touched.getValueCount() == 1);
        boolean predict_readOnly = orig_ts.getProcedure().getReadonly(); // FIXME
        boolean predict_abortable = true; // FIXME
        new_ts.init(new_txn_id, base_partition, orig_ts, predict_singlePartitioned, predict_readOnly, predict_abortable);
        Set<Integer> new_done = new_ts.getDonePartitions();
        new_done.addAll(this.all_partitions);
        new_done.removeAll(touched.values());
        
        if (d) {
            LOG.debug(String.format("Re-executing mispredicted %s as new %s-partition %s on partition %d",
                                    orig_ts, (predict_singlePartitioned ? "single" : "multi"), new_ts, base_partition));
            if (t) LOG.trace(String.format("%s Mispredicted partitions\n%s", new_ts, orig_ts.getTouchedPartitions()));
        }
        
        this.initializeInvocation(new_ts);
    }
    
    /**
     * This will block until the the initialization latch is released by the InitiateCallback
     * @param txn_id
     */
    private void initializationBlock(LocalTransactionState ts) {
        if (ts.init_latch != null && ts.init_latch.getCount() > 0) {
            if (d) LOG.debug(String.format("Waiting for Dtxn.Coordinator to process our initialization response for %s", ts));
            if (hstore_conf.site.txn_profiling) ts.profiler.startCoordinatorBlocked();
            try {
                ts.init_latch.await();
            } catch (Exception ex) {
                if (this.isShuttingDown() == false) LOG.fatal("Unexpected error when waiting for latch on " + ts, ex);
                this.shutdown();
            }
            if (hstore_conf.site.txn_profiling) ts.profiler.stopCoordinatorBlocked();
            if (d) LOG.debug("Got the all clear message for " + ts);
        }
        return;
    }

    /**
     * Request some work to be executed on partitions through the Dtxn.Coordinator
     * @param txn_id
     * @param fragment
     * @param callback
     */
    public void requestWork(LocalTransactionState ts, Dtxn.CoordinatorFragment fragment, RpcCallback<Dtxn.CoordinatorResponse> callback) {
        this.initializationBlock(ts);
        if (d) LOG.debug(String.format("Asking the Dtxn.Coordinator to execute fragment for %s [bytes=%d, last=%s]",
                                       ts, fragment.getSerializedSize(), fragment.getLastFragment()));
        ts.rpc_request_work.reset();
        this.coordinators[ts.getBasePartition()].execute(ts.rpc_request_work, fragment, callback);
    }

    public EventObservable getWorkloadObservable() {
        return (this.startWorkload_observable);
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
    public void queueClientResponse(ExecutionSite es, LocalTransactionState ts, ClientResponseImpl cr) {
        assert(hstore_conf.site.exec_postprocessing_thread);
        if (d) LOG.debug(String.format("Adding ClientResponse for %s from partition %d to processing queue [status=%s, size=%d]",
                                       ts, es.getPartitionId(), cr.getStatusName(), this.ready_responses.size()));
//        this.queue_size.incrementAndGet();
        this.ready_responses.add(new Object[]{es, ts, cr});
    }
    
    /**
     * Request that the Dtxn.Coordinator finish our transaction
     * @param ts
     * @param request
     * @param callback
     */
    public void requestFinish(LocalTransactionState ts, Dtxn.FinishRequest request, RpcCallback<Dtxn.FinishResponse> callback) {
        this.initializationBlock(ts);
        if (d) LOG.debug(String.format("Telling the Dtxn.Coordinator to finish %s [commit=%s, error=%s]", ts, request.getCommit(), ts.getPendingErrorMessage()));
        if (hstore_conf.site.txn_profiling) ts.profiler.startCoordinatorBlocked();
        this.coordinators[ts.getBasePartition()].finish(ts.rpc_request_finish, request, callback);
    }
    
    /**
     * 
     */
    @Override
    public void finish(RpcController controller, Dtxn.FinishRequest request, RpcCallback<Dtxn.FinishResponse> done) {
        // The payload will have our stored txn id. We can't use the FinishRequest's txn id because that is
        // going to be internal to Evan's stuff and is not guarenteed to be unique for this HStoreSiteNode
        assert(request.hasPayload()) : "Got Dtxn.FinishRequest without a payload. Can't determine txn id!";
        Long txn_id = HStoreSite.decodeTxnId(request.getPayload());
        assert(txn_id != null) : "Null txn id in Dtxn.FinishRequest payload";
        boolean commit = request.getCommit();
        
        if (d) LOG.debug(String.format("Got Dtxn.FinishRequest for txn #%d [commit=%s]", txn_id, commit));
        
        // This will be null for non-local multi-partition transactions
        LocalTransactionState ts = this.inflight_txns.get(txn_id);
        // ??? assert(ts != null) : String.format("Missing TransactionState for txn #%d at site %d", txn_id, this.site_id);

        // We only need to call commit/abort if this wasn't a single-partition transaction
        if (ts == null || ts.isPredictSinglePartition() == false) {
//            if (hstore_conf.site.txn_profiling && ts != null) ts.profiler.stopCoordinatorBlocked();
            LOG.debug(String.format("Calling finishWork for txn #%d on %d local partitions", txn_id, this.local_partitions.size()));
            for (Integer p : this.local_partitions) {
                this.executors[p].finishWork(txn_id, commit);
            } // FOR
        }
        
        // Send back a FinishResponse to let them know we're cool with everything...
        if (done != null) {
            Dtxn.FinishResponse.Builder builder = Dtxn.FinishResponse.newBuilder();
            done.run(builder.build());
            if (t) LOG.trace("Sent back Dtxn.FinishResponse for txn #" + txn_id);
        } 
    }
    
    /**
     * Notify this HStoreSite that the given transaction is done with the set of partitions
     * This will cause all the transactions that are blocked on this transaction to be released immediately and queued 
     * @param txn_id
     * @param partitions
     */
    public void doneAtPartitions(long txn_id, Collection<Integer> partitions) {
        assert(hstore_conf.site.exec_speculative_execution);
        
        int spec_cnt = 0;
        for (int p : partitions) {
            if (this.txnid_managers[p] == null) continue;
            
            // We'll let multiple tell us to speculatively execute, but we only let them go when hte latest
            // one finishes. We should really have multiple queues of speculatively execute txns, but for now
            // this is fine
            
//            assert(this.speculative_txn[p] == NULL_SPECULATIVE_EXEC_ID ||
//                   this.speculative_txn[p] == txn_id) : String.format("Trying to enable speculative execution twice at partition %d [current=#%d, new=#%d]", p, this.speculative_txn[p], txn_id); 
                
            // Make sure that we tell the ExecutionSite first before we allow txns to get fired off
            boolean ret = this.executors[p].enableSpeculativeExecution(txn_id, false);
            if (d && ret) {
                spec_cnt++;
                if (d) LOG.debug(String.format("Partition %d - Speculative Execution!", p));
            }
        } // FOR
        if (d) LOG.debug(String.format("Enabled speculative execution at %d partitions because of waiting for txn #%d", spec_cnt, txn_id));
    }


    /**
     * Perform final cleanup and book keeping for a completed txn
     * @param txn_id
     */
    public void completeTransaction(final long txn_id, final Dtxn.FragmentResponse.Status status) {
        if (d) LOG.debug("Cleaning up internal info for Txn #" + txn_id);
        LocalTransactionState ts = this.inflight_txns.remove(txn_id);
        assert(ts != null) : String.format("Missing TransactionState for txn #%d at site %d", txn_id, this.site_id);
        final int base_partition = ts.getBasePartition();
        final Procedure catalog_proc = ts.getProcedure();
        
        // If this partition is completely idle, then we will increase the size of its upper limit
        if (this.inflight_txns_ctr[base_partition].decrementAndGet() == 0) {
            if (this.startWorkload) {
                this.incoming_queue_max[base_partition] += 100; // TODO
                this.incoming_queue_release[base_partition] = Math.max((int)(this.incoming_queue_max[base_partition] * hstore_conf.site.txn_incoming_queue_release_factor), 1); 
            }
            if (hstore_conf.site.status_show_executor_info) ProfileMeasurement.start(true, idle_time);
        }
        
        // Update Transaction profiles
        // We have to calculate the profile information *before* we call ExecutionSite.cleanup!
        // XXX: Should we include totals for mispredicted txns?
        if (hstore_conf.site.txn_profiling && this.status_monitor != null &&
            ts.profiler.isDisabled() == false && status != Dtxn.FragmentResponse.Status.ABORT_MISPREDICT) {
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
                case ABORT_MISPREDICT:
                    if (t) LOG.trace("Telling the TransactionEstimator to IGNORE " + ts);
                    if (t_estimator != null) t_estimator.mispredict(txn_id);
                    if (hstore_conf.site.status_show_txn_info) {
                        (ts.isSpeculative() ? TxnCounter.RESTARTED : TxnCounter.MISPREDICTED).inc(catalog_proc);
                    }
                    break;
                case ABORT_USER:
                case ABORT_DEADLOCK:
                    if (t) LOG.trace("Telling the TransactionEstimator to ABORT " + ts);
                    if (t_estimator != null) t_estimator.abort(txn_id);
                    if (hstore_conf.site.status_show_txn_info) TxnCounter.ABORTED.inc(catalog_proc);
                    break;
                default:
                    assert(false) : String.format("Unexpected status %s for %s", status, ts);
            } // SWITCH
        } catch (Throwable ex) {
            LOG.error(String.format("Unexpected error when cleaning up %s transaction %s", status, ts), ex);
            // Pass...
        }
        
        // Then update transaction profiling counters
        if (hstore_conf.site.status_show_txn_info) {
            if (ts.isSpeculative()) TxnCounter.SPECULATIVE.inc(catalog_proc);
            if (ts.isExecNoUndoBuffer()) TxnCounter.NO_UNDO.inc(catalog_proc);
            if (ts.sysproc) {
                TxnCounter.SYSPROCS.inc(catalog_proc);
            } else if (status != Dtxn.FragmentResponse.Status.ABORT_MISPREDICT) {
                (ts.isPredictSinglePartition() ? TxnCounter.SINGLE_PARTITION : TxnCounter.MULTI_PARTITION).inc(catalog_proc);
            }
        }
        HStoreSite.CACHE_ENCODED_TXNIDS.remove(txn_id);
        ts.setHStoreSite_Finished(true);
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
        final int num_partitions = catalog_site.getPartitions().size();
        final String site_host = catalog_site.getHost().getIpaddr();
        
        // ----------------------------------------------------------------------------
        // (1) ProtoServer Thread (one per HStoreSite)
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Launching ProtoServer [site=%d, port=%d]", hstore_site.getSiteId(), catalog_site.getDtxn_port()));
        final CountDownLatch execLatch = new CountDownLatch(1);
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(hstore_site.getThreadName("proto"));
                if (hstore_site.getHStoreConf().site.cpu_affinity)
                    hstore_site.getThreadManager().registerProcessingThread();
                
                ProtoServer execServer = new ProtoServer(hstore_site.protoEventLoop);
                execServer.register(hstore_site);
                execServer.bind(catalog_site.getDtxn_port());
                execLatch.countDown();
                
                boolean should_shutdown = false;
                Throwable error = null;
                try {
                    hstore_site.protoEventLoop.setExitOnSigInt(true);
                    hstore_site.ready_latch.countDown();
                    hstore_site.protoEventLoop.run();
                } catch (Throwable ex) {
                    if (hstore_site.isShuttingDown() == false) LOG.fatal("ProtoServer thread failed", ex);
                    error = ex;
                    should_shutdown = true;
                }
                if (hstore_site.isShuttingDown() == false) {
                    LOG.warn(String.format("ProtoServer thread is stopping! [error=%s, should_shutdown=%s, state=%s]",
                                           (error != null ? error.getMessage() : null), should_shutdown, hstore_site.shutdown_state));
                    hstore_site.prepareShutdown();
                    if (should_shutdown) hstore_site.messenger.shutdownCluster(error);
                }
            };
        });
        
        // ----------------------------------------------------------------------------
        // (2) DTXN Engine Threads (one per partition)
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Launching DTXN Engines for %d partitions", num_partitions));
        final CountDownLatch engineLatch = new CountDownLatch(hstore_site.local_partitions.size());
        for (final Partition catalog_part : catalog_site.getPartitions()) {
            // TODO: There should be a single thread that forks all the processes and then joins on them
            runnables.add(new Runnable() {
                public void run() {
                    final Thread self = Thread.currentThread();
                    self.setName(hstore_site.getThreadName("eng", catalog_part.getId()));
                    if (hstore_site.getHStoreConf().site.cpu_affinity)
                        hstore_site.getThreadManager().registerProcessingThread();
                    
                    int partition = catalog_part.getId();
                    int port = catalog_site.getDtxn_port();
                    
                    // This needs to wait for the ProtoServer to start first
                    if (execLatch.getCount() > 0) {
                        if (d) LOG.debug("Waiting for ProtoServer to finish start up for Partition #" + partition);
                        try {
                            execLatch.await();
                        } catch (InterruptedException ex) {
                            LOG.error("Unexpected interuption while waiting for Partition #" + partition, ex);
                            hstore_site.messenger.shutdownCluster(ex);
                        }
                    }
                    
                    if (d) LOG.debug("Forking off ProtoDtxnEngine for Partition #" + partition + " [outbound_port=" + port + "]");
                    String[] command = new String[]{
                        dtxnengine_path,                // protodtxnengine
                        site_host + ":" + port,         // host:port (ProtoServer)
                        hstore_conf_path,               // hstore.conf
                        Integer.toString(partition),    // partition #
                        "0"                             // ??
                    };
                    engineLatch.countDown();
                    hstore_site.ready_latch.countDown();
                    ThreadUtil.fork(command, hstore_site.shutdown_observable,
                                    String.format("[%s] ", hstore_site.getThreadName("protodtxnengine", partition)), true);
                    if (hstore_site.isShuttingDown() == false) { 
                        String msg = "ProtoDtxnEngine for Partition #" + partition + " is stopping!"; 
                        LOG.error(msg);
                        hstore_site.prepareShutdown();
                        hstore_site.messenger.shutdownCluster(new Exception(msg));
                    }
                }
            });
        } // FOR (partition)

        // ----------------------------------------------------------------------------
        // (3) Engine EventLoop Thread (one per HStoreSite)
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Launching Engine EventLoop [site=%d]", hstore_site.getSiteId()));
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(hstore_site.getThreadName("eng"));
                if (hstore_site.getHStoreConf().site.cpu_affinity)
                    hstore_site.getThreadManager().registerProcessingThread();
                
                // Wait for all of our engines to start first
                try {
                    engineLatch.await();
                } catch (Exception ex) {
                    LOG.error("Unexpected interuption while waiting for engines to start", ex);
                    hstore_site.messenger.shutdownCluster(ex);
                }
                
                // Connect directly to all of our local engines
                final InetSocketAddress destinations[] = new InetSocketAddress[hstore_site.local_partitions.size()];
                ProtoRpcChannel[] channels = null;
                for (int i = 0; i < destinations.length; i++) {
                    int partition = hstore_site.local_partitions.get(i).intValue();
                    destinations[i] = CatalogUtil.getPartitionAddressById(hstore_site.catalog_db, partition, true);
                    assert(destinations[i] != null) : "Failed to socket address for partition " + partition;
                } // FOR
                if (d) LOG.debug(String.format("Connecting directly to %d local Dtxn.Engines for CANADIAN mode support!", destinations.length));
                if (t) LOG.trace("Dtxn.Engines: " + Arrays.toString(destinations));
                try {
                    channels = ProtoRpcChannel.connectParallel(hstore_site.engineEventLoop, destinations, 15000);
                } catch (RuntimeException ex) {
                    LOG.fatal("Failed to connect to local Dtxn.Engines", ex);
                    hstore_site.messenger.shutdownCluster(ex, true); // Blocking
                }
                assert(channels != null);
                assert(channels.length == destinations.length);
                for (int i = 0; i < channels.length; i++) {
                    int partition = hstore_site.local_partitions.get(i).intValue();
                    hstore_site.engine_channels[partition] = Dtxn.Partition.newStub(channels[i]);
                    if (t) LOG.trace("Creating direct Dtxn.Engine connection for partition " + partition);
                } // FOR
                if (d) LOG.debug("Established connections to all Dtxn.Engines");
                
                boolean should_shutdown = false;
                Throwable error = null;
                try {
                    hstore_site.engineEventLoop.setExitOnSigInt(true);
                    hstore_site.ready_latch.countDown();
                    hstore_site.engineEventLoop.run();
                } catch (Throwable ex) {
                    if (hstore_site.isShuttingDown() == false &&
                            ex != null &&
                            ex.getMessage() != null &&
                            ex.getMessage().contains("Connection closed") == false
                        ) {
                        LOG.fatal("Engine EventLoop thread failed", ex);
                        error = ex;
                        should_shutdown = true;
                    }
                }
                if (hstore_site.isShuttingDown() == false) {
                    LOG.warn(String.format("Engine EventLoop thread is stopping! [error=%s, should_shutdown=%s, state=%s]",
                                           (error != null ? error.getMessage() : null), should_shutdown, hstore_site.shutdown_state));
                    hstore_site.prepareShutdown();
                    if (should_shutdown) hstore_site.messenger.shutdownCluster(error);
                }
            };
        });
        
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
                    self.setName(hstore_site.getThreadName("listen", id));
                    if (hstore_site.getHStoreConf().site.cpu_affinity)
                        hstore_site.getThreadManager().registerProcessingThread();
                    
                    // First connect to the coordinator and generate a handle
                    if (d) LOG.debug(String.format("Creating connection to coordinator at %s:%d",
                                                   coordinatorHost, coordinatorPort));
                    InetSocketAddress[] addresses = {
                            new InetSocketAddress(coordinatorHost, coordinatorPort),
                    };
                    ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(hstore_site.procEventLoops[id], addresses);
                    Dtxn.Coordinator stub = Dtxn.Coordinator.newStub(channels[0]);
                    hstore_site.coordinators[catalog_part.getId()] = stub;
                    
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
                        hstore_site.messenger.shutdownCluster(error);
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
                self.setName(hstore_site.getThreadName("setup"));
                if (hstore_site.getHStoreConf().site.cpu_affinity)
                    hstore_site.getThreadManager().registerProcessingThread();
                
                // Always invoke HStoreSite.start() right away, since it doesn't depend on any
                // of the stuff being setup yet
                hstore_site.init();
                
                // But then wait for all of the threads to be finished with their initializations
                // before we tell the world that we're ready!
                if (d) LOG.debug(String.format("Waiting for %d threads to complete initialization tasks", hstore_site.ready_latch.getCount()));
                try {
                    hstore_site.ready_latch.await();
                } catch (Exception ex) {
                    LOG.error("Unexpected interuption while waiting for engines to start", ex);
                    hstore_site.messenger.shutdownCluster(ex);
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
        if (d) LOG.info("HStoreConf Parameters:\n" + HStoreConf.singleton().toString(true, true));

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
