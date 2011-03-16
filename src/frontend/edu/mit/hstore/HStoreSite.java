package edu.mit.hstore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.ExecutionSiteHelper;
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
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoServer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraphsContainer;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.Workload;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.FragmentResponse.Status;
import edu.mit.hstore.callbacks.ClientResponseFinalCallback;
import edu.mit.hstore.callbacks.ForwardTxnRequestCallback;
import edu.mit.hstore.callbacks.InitiateCallback;
import edu.mit.hstore.callbacks.MultiPartitionTxnCallback;
import edu.mit.hstore.callbacks.NoDtxnCallback;
import edu.mit.hstore.callbacks.SinglePartitionTxnCallback;
import edu.mit.hstore.dtxn.LocalTransactionState;

/**
 * 
 * @author pavlo
 */
public class HStoreSite extends Dtxn.ExecutionEngine implements VoltProcedureListener.Handler {
    private static final Logger LOG = Logger.getLogger(HStoreSite.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();
    
    public static final String DTXN_COORDINATOR = "protodtxncoordinator";
    public static final String DTXN_ENGINE = "protodtxnengine";
    public static final String SITE_READY_MSG = "Site is ready for action";

    public enum TxnCounter {
        /** The number of txns that we executed locally */
        EXECUTED,
        /** Of the locally executed transactions, how many were single-partitioned */
        SINGLE_PARTITION,
        /** Of the locally executed transactions, how many were multi-partitioned */
        MULTI_PARTITION,
        /** The number of sysprocs that we executed */
        SYSPROCS,
        /** Of the locally executed transactions, how many were abort */
        ABORTED,
        /** The number of tranactions that were completed (committed or aborted) */
        COMPLETED,
        /** The number of transactions that were mispredicted (and thus re-executed) */
        MISPREDICTED,
        /** The number of tranasction requests that have arrived at this site */
        RECEIVED,
        /** Of the the receieved transactions, the number that we had to send somewhere else */
        REDIRECTED,
        ;
        
        private final Histogram h = new Histogram();
        private final String name;
        private TxnCounter() {
            this.name = StringUtil.title(this.name()).replace("_", "-");
        }
        @Override
        public String toString() {
            return (this.name);
        }
        public Histogram getHistogram() {
            return (this.h);
        }
        public int get() {
            return ((int)this.h.getSampleCount());
        }
        public int inc(Procedure catalog_proc) {
            this.h.put(catalog_proc.getName());
            return (this.get());
        }
        public double ratio() {
            int total = -1;
            switch (this) {
                case SINGLE_PARTITION:
                case MULTI_PARTITION:
                    total = SINGLE_PARTITION.get() + MULTI_PARTITION.get();
                    break;
                case SYSPROCS:
                case ABORTED:
                case MISPREDICTED:
                case EXECUTED:
                    total = EXECUTED.get() - SYSPROCS.get();
                    break;
                case REDIRECTED:
                case RECEIVED:
                    total = RECEIVED.get();
                    break;
                default:
                    assert(false) : this;
            }
            return (this.get() / (double)total);
        }
    }
    
    
    private static final double PRELOAD_SCALE_FACTOR = Double.valueOf(System.getProperty("hstore.preload", "1.0")); 
    public static double getPreloadScaleFactor() {
        return (PRELOAD_SCALE_FACTOR);
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * This is the thing that we will actually use to generate txn ids used by our H-Store specific code
     */
    private final Map<Integer, TransactionIdManager> txnid_managers = new HashMap<Integer, TransactionIdManager>();
    
    private final DBBPool buffer_pool = new DBBPool(true, false);
    private final Map<Integer, Thread> executor_threads = new HashMap<Integer, Thread>();
    private final HStoreMessenger messenger;
    
    // Dtxn Stuff
    private Dtxn.Coordinator coordinator;
    private final NIOEventLoop coordinatorEventLoop = new NIOEventLoop();

    private boolean ready = false;
    private final EventObservable ready_observable = new EventObservable();
    private boolean shutdown = false;
    private final EventObservable shutdown_observable = new EventObservable();
    
    protected final Site catalog_site;
    protected final List<Integer> all_partitions;
    protected final int site_id;
    protected final Map<Integer, ExecutionSite> executors;
    protected final Database catalog_db;
    protected final PartitionEstimator p_estimator;

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
    protected final Map<Long, LocalTransactionState> inflight_txns = new ConcurrentHashMap<Long, LocalTransactionState>();

    /**
     * This is a mapping for the transactions that were restarted because of a misprediction from the second
     * transaction id back to the second transaction id.
     * Restart Txn# -> Original Txn#
     */
    private final Map<Long, Long> restarted_txns = new ConcurrentHashMap<Long, Long>();

    /**
     * Helper Thread Stuff
     */
    private final ScheduledExecutorService helper_pool;
    
    
    // ----------------------------------------------------------------------------
    // HSTORESITE STATUS THREAD
    // ----------------------------------------------------------------------------
    
    /**
     * Status Monitor
     */
    private Thread status_monitor;
    
    /**
     * Convenience method to dump out status of this HStoreSite
     * @return
     */
    public String statusSnapshot() {
        return new HStoreSiteStatus(this, 0, false).snapshot(true, false, false);
    }
    
    // ----------------------------------------------------------------------------
    // HSTORESTITE SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
    /**
     * Shutdown Hook Thread
     */
    protected class ShutdownHookThread implements Runnable {
        @Override
        public void run() {
            // Dump out our status
            int num_inflight = inflight_txns.size();
            if (num_inflight > 0) {
                System.err.println("Shutdown [" + num_inflight + " txns inflight]");
            }
        }
    } // END CLASS

    /**
     * Perform shutdown operations for this HStoreCoordinatorNode
     */
    public synchronized void shutdown() {
        final boolean t = trace.get();
        final boolean d = debug.get();
        
        if (this.shutdown) {
            if (d) LOG.debug("Already told to shutdown... Ignoring");
            return;
        }
        this.shutdown = true;
//        if (d)
            LOG.info("Shutting down everything at Site #" + this.site_id);

//        for (LocalTransactionState ts : this.inflight_txns.values()) {
//            LOG.info(String.format("INFLIGHT TXN #%d\n%s", ts.getTransactionId(), this.dumpTransaction(ts)));
//        }
            
        // Tell anybody that wants to know that we're going down
        if (t) LOG.trace("Notifying " + this.shutdown_observable.countObservers() + " observers that we're shutting down");
        this.shutdown_observable.notifyObservers();
        
        // Tell our local boys to go down too
        for (ExecutionSite executor : this.executors.values()) {
            if (t) LOG.trace("Telling the ExecutionSite for Partition #" + executor.getPartitionId() + " to shutdown");
            executor.shutdown();
        } // FOR
        
        // Stop the monitor thread
        if (this.status_monitor != null) {
            if (t) LOG.trace("Telling StatusMonitorThread to stop");
            this.status_monitor.interrupt();
        }
        
        // Tell all of our event loops to stop
        if (t) LOG.trace("Telling Dtxn.Coordinator event loop to exit");
        this.coordinatorEventLoop.exitLoop();
        
        if (d) LOG.debug("Completed shutdown process at Site #" + this.site_id);
    }
    
    private String dumpTransaction(LocalTransactionState ts) {
        final Object args[] = ts.getInvocation().getParams().toArray();
        Procedure catalog_proc = ts.getProcedure();
        Object cast_args[] = this.param_manglers.get(catalog_proc).convert(args);
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cast_args.length; i++) {
            sb.append("[" + i + "] ");
            if (catalog_proc.getParameters().get(i).getIsarray()) {
                sb.append(Arrays.toString((Object[])cast_args[i]));
            } else {
                sb.append(cast_args[i]);
            }
            sb.append("\n");
        } // FOR
        sb.append(ts.toString());
        return (sb.toString());
    }
    
    /**
     * Returns true if HStoreSite is in the process of shutting down
     * @return
     */
    public boolean isShuttingDown() {
        return (this.shutdown);
    }
    
    /**
     * Get the Oberservable handle for this HStoreSite that can alert others when the party is ending
     * @return
     */
    public void addShutdownObservable(Observer observer) {
        this.shutdown_observable.addObserver(observer);
    }

    // ----------------------------------------------------------------------------
    // INITIALIZATION STUFF
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param coordinator
     * @param p_estimator
     */
    public HStoreSite(Site catalog_site, Map<Integer, ExecutionSite> executors, PartitionEstimator p_estimator) {
        // General Stuff
        this.hstore_conf = HStoreConf.singleton();
        this.catalog_site = catalog_site;
        this.site_id = this.catalog_site.getId();
        this.catalog_db = CatalogUtil.getDatabase(this.catalog_site);
        this.all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        this.p_estimator = p_estimator;
        this.thresholds = new EstimationThresholds(); // default values
        this.executors = Collections.unmodifiableMap(new HashMap<Integer, ExecutionSite>(executors));
        this.messenger = new HStoreMessenger(this);
        
        for (Integer partition : this.executors.keySet()) {
            this.txnid_managers.put(partition, new TransactionIdManager(partition.intValue()));
        }
        
        this.helper_pool = Executors.newScheduledThreadPool(1);
        
        assert(this.catalog_db != null);
        assert(this.p_estimator != null);
    }

    /**
     * SetupThread
     */
    protected class SetupThread implements Runnable {
        public void run() {
            // Create all of our parameter manglers
            for (Procedure catalog_proc : HStoreSite.this.catalog_db.getProcedures()) {
                HStoreSite.this.param_manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
            } // FOR
            
            try {
                // Load up everything the QueryPlanUtil
                QueryPlanUtil.preload(HStoreSite.this.catalog_db);
                
                // Then load up everything in the PartitionEstimator
                HStoreSite.this.p_estimator.preload();
                
            } catch (Exception ex) {
                LOG.fatal("Failed to prepare HStoreSite", ex);
                System.exit(1);
            }
            
            // Schedule the ExecutionSiteHelper
            ExecutionSiteHelper esh = new ExecutionSiteHelper(HStoreSite.this.executors.values(), hstore_conf.helper_txn_per_round, hstore_conf.helper_txn_expire, hstore_conf.enable_profiling);
            HStoreSite.this.helper_pool.scheduleAtFixedRate(esh, 2000, hstore_conf.helper_interval, TimeUnit.MILLISECONDS);
            
            // Start Monitor Thread
            if (HStoreSite.this.status_monitor != null) HStoreSite.this.status_monitor.start(); 
        }
    }

    /**
     * Initializes all the pieces that we need to start this HStore site up
     */
    public void start() {
        if (d) LOG.debug("Initializing HStoreSite...");

        // First we need to tell the HStoreMessenger to start-up and initialize its connections
        if (d) LOG.debug("Starting HStoreMessenger for Site #" + this.site_id);
        this.messenger.start();

        // Preparation Thread
        new Thread(new SetupThread()) {
            {
                this.setName(HStoreSite.this.getThreadName("prep"));
                this.setDaemon(true);
            }
        }.start();
        
        // Then we need to start all of the ExecutionSites in threads
        if (d) LOG.debug("Starting ExecutionSite threads for " + this.executors.size() + " partitions on Site #" + this.site_id);
        for (Entry<Integer, ExecutionSite> e : this.executors.entrySet()) {
            ExecutionSite executor = e.getValue();
            executor.setHStoreCoordinatorSite(this);
            executor.setHStoreMessenger(this.messenger);

            Thread t = new Thread(executor);
            t.setDaemon(true);
            this.executor_threads.put(e.getKey(), t);
            t.start();
        } // FOR
        
        // Add in our shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHookThread()));
    }
    
    /**
     * Mark this HStoreSite as ready for action!
     */
    private synchronized void ready() {
        if (this.ready) {
            LOG.warn("Already told that we were ready... Ignoring");
            return;
        }
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
     * Enable the HStoreCoordinator's status monitor
     * @param interval
     */
    public void enableStatusMonitor(int interval, boolean kill_when_hanging) {
        if (interval > 0) {
            this.status_monitor = new Thread(new HStoreSiteStatus(this, interval, kill_when_hanging));
            this.status_monitor.setPriority(Thread.MIN_PRIORITY);
            this.status_monitor.setDaemon(true);
        }
    }
    
    public Map<Integer, ExecutionSite> getExecutors() {
        return executors;
    }
    
    public int getExecutorCount() {
        return (this.executors.size());
    }
    
    protected void setDtxnCoordinator(Dtxn.Coordinator coordinator) {
        this.coordinator = coordinator;
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
    }
    
    /**
     * Return the Site catalog object for this HStoreCoordinatorNode
     * @return
     */
    public Site getSite() {
        return (this.catalog_site);
    }
    
    public int getSiteId() {
        return (this.site_id);
    }

    public int getInflightTxnCount() {
        return (this.inflight_txns.size());
    }
    
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    /**
     * 
     * @param suffix
     * @param partition
     * @return
     */
    public final String getThreadName(String suffix, Integer partition) {
        if (suffix == null) suffix = "";
        if (suffix.isEmpty() == false) {
            suffix = "-" + suffix;
            if (partition != null) suffix = String.format("-%03d%s", partition.intValue(), suffix);
        } else if (partition != null) {
            suffix = String.format("%03d", partition.intValue());
        }
        return (String.format("H%03d%s", this.site_id, suffix));
    }

    /**
     * Returns a nicely formatted thread name
     * @param suffix
     * @return
     */
    public final String getThreadName(String suffix) {
        return (this.getThreadName(suffix, null));
    }
    
    private static final Map<Long, ByteString> cache_encoded = new ConcurrentHashMap<Long, ByteString>();
    public static ByteString encodeTxnId(long txn_id) {
        ByteString bs = cache_encoded.get(txn_id);
        if (bs == null) {
            bs = ByteString.copyFrom(Long.toString(txn_id).getBytes());
            cache_encoded.put(txn_id, bs);
        }
        return (bs);
    }
    public static long decodeTxnId(ByteString bs) {
        return (Long.valueOf(bs.toStringUtf8()));
    }

    // ----------------------------------------------------------------------------
    // EXECUTION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void procedureInvocation(byte[] serializedRequest, RpcCallback<byte[]> done) {
        long timestamp = (hstore_conf.enable_profiling ? ProfileMeasurement.getTime() : -1);
        
        // The serializedRequest is a ProcedureInvocation object
        StoredProcedureInvocation request = null;
        FastDeserializer fds = new FastDeserializer(serializedRequest);
        try {
            request = fds.readObject(StoredProcedureInvocation.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        if (request == null) {
            throw new RuntimeException("Failed to get ProcedureInvocation object from request bytes");
        }

        // Extract the stuff we need to figure out whether this guy belongs at our site
        request.buildParameterSet();
        assert(request.getParams() != null) : "The parameters object is null for new txn from client #" + request.getClientHandle();
        final Object args[] = request.getParams().toArray(); 
        final Procedure catalog_proc = this.catalog_db.getProcedures().get(request.getProcName());
        final boolean sysproc = catalog_proc.getSystemproc();
        if (this.status_monitor != null) TxnCounter.RECEIVED.inc(catalog_proc);
        
        if (catalog_proc == null) throw new RuntimeException("Unknown procedure '" + request.getProcName() + "'");
        if (d) LOG.debug(String.format("Received new stored procedure invocation request for %s [bytes=%d]", catalog_proc.getName(), serializedRequest.length));
        
        // First figure out where this sucker needs to go
        Integer dest_partition = null;
        
        // If it's a sysproc, then it doesn't need to go to a specific partition
        if (sysproc) {
            // Just pick the first one for now
            dest_partition = CollectionUtil.getFirst(this.executors.keySet());
            
            // HACK: Check if we should shutdown. This allows us to kill things even if the
            // DTXN coordinator is stuck.
            if (catalog_proc.getName().equals("@Shutdown")) {
                ClientResponseImpl cresponse = new ClientResponseImpl(1, ClientResponse.SUCCESS, new VoltTable[0], "");
                FastSerializer out = new FastSerializer();
                try {
                    out.writeObject(cresponse);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                done.run(out.getBytes());
                // Non-blocking....
                this.messenger.shutdownCluster(false, new Exception("Shutdown command received at site #" + this.site_id));
                return;
            }
        // Otherwise we use the PartitionEstimator to know where it is going
        } else if (hstore_conf.force_localexecution == false) {
            try {
                dest_partition = this.p_estimator.getBasePartition(catalog_proc, args, false);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // If we don't have a partition to send this transaction to, then we will just pick
        // one our partitions at random. This can happen if we're forcing txns to execute locally
        // or if there are no input parameters <-- this should be in the paper!!!
        if (dest_partition == null) {
            if (t && hstore_conf.force_localexecution) LOG.debug("Forcing all transaction requests to execute locally");
            dest_partition = CollectionUtil.getRandomValue(this.executors.keySet());
        }
        
        // assert(dest_partition >= 0);
        if (t) {
            LOG.trace("Client Handle = " + request.getClientHandle());
            LOG.trace("Destination Partition = " + dest_partition);
        }
        
        // If the dest_partition isn't local, then we need to ship it off to the right location
        if (hstore_conf.force_localexecution == false && this.executors.containsKey(dest_partition) == false) {
            if (d) LOG.debug("StoredProcedureInvocation request for " +  catalog_proc + " needs to be forwarded to Partition #" + dest_partition);
            
            // Make a wrapper for the original callback so that when the result comes back frm the remote partition
            // we will just forward it back to the client. How sweet is that??
            ForwardTxnRequestCallback callback = new ForwardTxnRequestCallback(done);
            this.messenger.forwardTransaction(serializedRequest, callback, dest_partition);
            if (this.status_monitor != null) TxnCounter.REDIRECTED.inc(catalog_proc);
            return;
        }
        
        // IMPORTANT: We have two txn ids here. We have the real one that we're going to use internally
        // and the fake one that we pass to Evan. We don't care about the fake one and will always ignore the
        // txn ids found in any Dtxn.Coordinator messages. 
        long txn_id = this.txnid_managers.get(dest_partition).getNextUniqueTransactionId();
                
        // Grab the TransactionEstimator for the destination partition and figure out whether
        // this mofo is likely to be single-partition or not. Anything that we can't estimate
        // will just have to be multi-partitioned. This includes sysprocs
        ExecutionSite executor = this.executors.get(dest_partition);
        TransactionEstimator t_estimator = executor.getTransactionEstimator();
        
        Boolean single_partition = null;
        TransactionEstimator.State estimator_state = null; 

        LocalTransactionState local_ts = null;
        try {
            local_ts = (LocalTransactionState)executor.localTxnPool.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for txn #" + txn_id);
            throw new RuntimeException(ex);
        }

        // Sysprocs are always multi-partitioned
        if (sysproc) {
            if (t) LOG.trace(String.format("Txn # is a sysproc, so it has to be multi-partitioned", txn_id));
            single_partition = false;
            
        // Force all transactions to be single-partitioned
        } else if (hstore_conf.force_singlepartitioned) {
            if (t) LOG.trace("The \"Always Single-Partitioned\" flag is true. Marking as single-partitioned!");
            single_partition = true;
            
        // Assume we're executing TPC-C neworder. Manually examine the input parameters and figure
        // out what partitions it's going to need to touch
        } else if (hstore_conf.force_neworder_hack) {
            if (t) LOG.trace("Executing neworder argument hack for VLDB paper");
            AbstractHasher hasher = this.p_estimator.getHasher();
            single_partition = true;
            short w_id = Short.parseShort(args[0].toString()); // HACK
            int w_id_partition = hasher.hash(w_id);
            short inner[] = (short[])args[5];
            
            Set<Integer> done_partitions = local_ts.getDonePartitions();
            done_partitions.addAll(this.all_partitions);
            
            short last_w_id = w_id;
            int last_partition = w_id_partition;
            if (hstore_conf.force_neworder_hack_done) done_partitions.remove(w_id_partition);
            for (short s_w_id : inner) {
                if (s_w_id != last_w_id) {
                    last_partition = hasher.hash(s_w_id);
                    last_w_id = s_w_id;
                }
                if (w_id_partition != last_partition) {
                    single_partition = false;
                    if (hstore_conf.force_neworder_hack_done) {
                        done_partitions.remove(last_partition);
                    } else break;
                }
            } // FOR
            if (t) LOG.trace(String.format("SinglePartitioned=%s, W_ID=%d, S_W_IDS=%s", single_partition, w_id, Arrays.toString(inner)));
            
        // Otherwise, we'll try to estimate what the transaction will do (if we can)
        } else {
            if (d) LOG.debug(String.format("Using TransactionEstimator to check whether %s txn #%d is single-partition", catalog_proc.getName(), txn_id));
            
            try {
                // HACK: Convert the array parameters to object arrays...
                Object cast_args[] = this.param_manglers.get(catalog_proc).convert(args);
                
                if (hstore_conf.enable_profiling) local_ts.est_time.start();
                estimator_state = t_estimator.startTransaction(txn_id, dest_partition, catalog_proc, cast_args);
                if (estimator_state == null) {
                    if (d) LOG.debug(String.format("No TransactionEstimator.State was returned for txn #%d. Executing as multi-partitioned", txn_id)); 
                    single_partition = false;
                } else {
                    if (t) LOG.trace("\n" + StringUtil.box(estimator_state.toString()));
                    MarkovEstimate estimate = estimator_state.getInitialEstimate();
                    if (estimate == null) {
                        if (d) LOG.debug(String.format("No TransactionEstimator.Estimate was found for txn #%d. Executing as multi-partitioned", txn_id));
                        single_partition = false;
                    } else {
                        if (d) LOG.debug(String.format("Using TransactionEstimator.Estimate for txn #%d to determine if single-partitioned", txn_id));
                        if (d) LOG.debug("MarkovEstimate:\n" + estimate);
                        single_partition = estimate.isSinglePartition(this.thresholds);     
                    }
                }
                if (hstore_conf.enable_profiling) local_ts.est_time.stop();
            } catch (RuntimeException ex) {
                LOG.fatal("Failed calculate estimate for txn #" + txn_id, ex);
                throw ex;
            } catch (AssertionError ex) {
                LOG.fatal("Failed calculate estimate for txn #" + txn_id, ex);
                throw ex;
            } catch (Exception ex) {
                LOG.fatal("Failed calculate estimate for txn #" + txn_id, ex);
                throw new RuntimeException(ex);
            }
        }
        assert (single_partition != null);
        if (d) LOG.debug(String.format("Executing %s txn #%d on partition %d [single-partitioned=%s]", catalog_proc.getName(), txn_id, dest_partition, single_partition));

        local_ts.init(txn_id, request.getClientHandle(), dest_partition.intValue(), catalog_proc, request, done);
        local_ts.setPredictSinglePartitioned(single_partition);
        local_ts.setEstimatorState(estimator_state);        
        if (hstore_conf.enable_profiling) {
            local_ts.total_time.start(timestamp);
            local_ts.init_time.start(timestamp);
        }
        this.initializeInvocation(local_ts);
    }
    
    /**
     * 
     * @param txn_id
     * @param dtxn_txn_id
     * @param dest_partition
     * @param single_partition
     * @param client_handle
     * @param request
     * @param done
     */
    private void initializeInvocation(LocalTransactionState txn_info) {
        long txn_id = txn_info.getTransactionId();
        Long orig_txn_id = txn_info.getOriginalTransactionId();
        int base_partition = txn_info.getSourcePartition();
        boolean single_partitioned = (orig_txn_id == null && txn_info.isPredictSinglePartition());
                
        LocalTransactionState orig_ts = this.inflight_txns.put(txn_id, txn_info);
        if (orig_ts != null) {
            // HACK!
            this.inflight_txns.put(txn_id, orig_ts);
            long new_txn_id = this.txnid_managers.get(base_partition).getNextUniqueTransactionId();
            if (new_txn_id == txn_id) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("ORIG TRANSACTION:\n" + orig_ts);
                LOG.fatal("NEW TRANSACTION:\n" + txn_info);
                this.messenger.shutdownCluster(true, new Exception(msg));
            }
            LOG.warn(String.format("Had to fix duplicate txn id: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            txn_info.setTransactionId(txn_id);
            this.inflight_txns.put(txn_id, txn_info);
        }
        
        if (d) {
            LOG.debug(String.format("Passing %s to Dtxn.Coordinator as %s-partition txn #%d for partition %d",
                                    txn_info.invocation.getProcName(), (single_partitioned ? "single" : "multi"), txn_id, base_partition));
        }

        
        if (this.status_monitor != null) {
            if (txn_info.sysproc) {
                TxnCounter.SYSPROCS.inc(txn_info.getProcedure());
            } else {
                (single_partitioned ? TxnCounter.SINGLE_PARTITION : TxnCounter.MULTI_PARTITION).inc(txn_info.getProcedure());
            }
        }

        InitiateTaskMessage wrapper = new InitiateTaskMessage(txn_id, base_partition, base_partition, txn_info.invocation);

        // Skip the Dtxn.Coordinator
        if (hstore_conf.ignore_dtxn && txn_info.sysproc == false) {
//            if (d) 
                LOG.debug("Ignoring the Dtxn.Coordinator for txn #" + txn_id);
            Dtxn.Fragment.Builder requestBuilder = Dtxn.Fragment.newBuilder()
                                                                .setTransactionId(txn_id)
                                                                .setUndoable(true)
                                                                .setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array()));
            RpcCallback<Dtxn.FragmentResponse> callback = new NoDtxnCallback(this, txn_id, base_partition, txn_info.client_callback); 
            this.execute(null, requestBuilder.build(), callback);

        // Construct the message for the Dtxn.Coordinator
        } else {
            Dtxn.CoordinatorFragment.Builder requestBuilder = Dtxn.CoordinatorFragment.newBuilder();
            
            // Note that we pass the fake txn id to the Dtxn.Coordinator. 
            requestBuilder.setTransactionId(txn_id);
            
            // Whether this transaction is single-partitioned or not
            requestBuilder.setLastFragment(single_partitioned);
            
            Set<Integer> done_partitions = txn_info.getDonePartitions();
            
            // NewOrder Hack
            if (hstore_conf.force_neworder_hack_done && single_partitioned == false) {
                if (d) LOG.debug(String.format("NewOrder Hack: Setting %d partitions as done for txn #%d", done_partitions.size(), txn_id));
                for (Integer p : done_partitions) {
                    requestBuilder.addDonePartition(p.intValue());
                } // FOR
                
            // TransactionEstimator
            } else {
                // If we know we're single-partitioned, then we *don't* want to tell the Dtxn.Coordinator
                // that we're done at any partitions because it will throw an error
                // Instead, if we're not single-partitioned then that's that only time that 
                // we Tell the Dtxn.Coordinator that we are finished with partitions if we have an estimate
                TransactionEstimator.State estimator_state = txn_info.getEstimatorState(); 
                if (single_partitioned == false && orig_txn_id == null && estimator_state != null && estimator_state.getInitialEstimate() != null) {
                    // TODO: How do we want to come up with estimates per partition?
                    Set<Integer> touched_partitions = estimator_state.getEstimatedPartitions();
                    for (Integer p : this.all_partitions) {
                        if (touched_partitions.contains(p) == false) {
                            requestBuilder.addDonePartition(p.intValue());
                            done_partitions.add(p);
                        }
                    } // FOR
                    if (d && requestBuilder.getDonePartitionCount() > 0) {
                        LOG.debug(String.format("Marked txn #%d as done at %d partitions: %s", txn_id, requestBuilder.getDonePartitionCount(), requestBuilder.getDonePartitionList()));
                    }
                }
            }
            
            // NOTE: Evan betrayed our love so we can't use his txn ids because they are meaningless to us
            // So we're going to pack in our txn id in the payload. Any message they we get from Evan
            // will have this payload so that we can figure out what the hell is going on...
            requestBuilder.setPayload(HStoreSite.encodeTxnId(txn_id));
    
            // Pack the StoredProcedureInvocation into a Dtxn.PartitionFragment
            requestBuilder.addFragment(Dtxn.CoordinatorFragment.PartitionFragment.newBuilder()
                    .setPartitionId(base_partition)
                    .setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array())));
            
            RpcCallback<Dtxn.CoordinatorResponse> callback = null;
            if (single_partitioned) {
                if (t) LOG.trace("Using SinglePartitionTxnCallback for txn #" + txn_id);
                
                ExecutionSite executor = this.executors.get(base_partition);
                TransactionEstimator t_estimator = executor.getTransactionEstimator();
                callback = new SinglePartitionTxnCallback(this, txn_id, base_partition, t_estimator, txn_info.client_callback);

            // This latch prevents us from making additional requests to the Dtxn.Coordinator until
            // we get hear back about our our initialization request
            } else {
                if (t) LOG.trace("Using InitiateCallback for txn #" + txn_id);
                callback = new InitiateCallback(this, txn_id, txn_info.init_latch);
            }
            Dtxn.CoordinatorFragment dtxn_request = requestBuilder.build();
            if (hstore_conf.enable_profiling) ProfileMeasurement.swap(txn_info.init_time, txn_info.coord_time);
            this.coordinator.execute(txn_info.rpc_request_init, dtxn_request, callback);
            if (d) LOG.debug(String.format("Sent Dtxn.CoordinatorFragment for txn #%d [bytes=%d]", txn_id, dtxn_request.getSerializedSize()));
        }
    }
    

    /**
     * Execute some work on a particular ExecutionSite
     */
    @Override
    public void execute(RpcController controller, Dtxn.Fragment request, RpcCallback<Dtxn.FragmentResponse> done) {
        long timestamp = (hstore_conf.enable_profiling ? ProfileMeasurement.getTime() : -1);
        
        // Decode the procedure request
        TransactionInfoBaseMessage msg = null;
        try {
            msg = (TransactionInfoBaseMessage)VoltMessage.createMessageFromBuffer(request.getWork().asReadOnlyByteBuffer(), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        long txn_id = msg.getTxnId();
        int partition = msg.getDestinationPartitionId();
        Boolean single_partitioned = null;
        
        if (d) {
            LOG.debug(String.format("Got %s message for txn #%d [partition=%d]", msg.getClass().getSimpleName(), txn_id, partition));
            if (t) LOG.trace("CONTENTS:\n" + msg);
        }
        
        ExecutionSite executor = this.executors.get(partition);
        if (executor == null) {
            throw new RuntimeException("No ExecutionSite exists for Partition #" + partition + " at this site???");
        }
        
        // Two things can now happen based on what type of message we were given:
        //
        //  (1) If we have an InitiateTaskMessage, then this call is for starting a new txn
        //      at this site. If this is a multi-partitioned transaction, then we need to send
        //      back a placeholder response through the Dtxn.Coordinator so that we are allowed to
        //      execute queries on remote partitions later.
        //      Note that we maintain the callback to the client so that we know how to send back
        //      our ClientResponse once the txn is finished.
        //
        //  (2) Any other message can just be sent along to the ExecutionSite without sending
        //      back anything right away. The ExecutionSite will use our callback handle
        //      to send back whatever response it needs to on its own.
        //
        RpcCallback<Dtxn.FragmentResponse> callback = null;
        if (msg instanceof InitiateTaskMessage) {
            InitiateTaskMessage task = (InitiateTaskMessage)msg;
            LocalTransactionState txn_info = this.inflight_txns.get(txn_id);
            assert(txn_info != null) : "Missing TransactionInfo for txn #" + txn_id;
            if (hstore_conf.enable_profiling) txn_info.coord_time.stop(timestamp);
            
            single_partitioned = txn_info.isPredictSinglePartition();
            assert(txn_info.client_callback != null) : "Missing original RpcCallback for txn #" + txn_id;
      
            // SCORE!
            assert(task.getStoredProcedureInvocation() == null);
            task.setStoredProcedureInvocation(txn_info.invocation);
            
            // If we're single-partitioned, then we don't want to send back a callback now.
            // The response from the ExecutionSite should be the only response that we send back to the Dtxn.Coordinator
            // This limits the number of network roundtrips that we have to do...
            if (single_partitioned == false) { //  || (txn_info.sysproc == false && hstore_conf.ignore_dtxn == false)) {
                // We need to send back a response before we actually start executing to avoid a race condition    
                if (d) LOG.debug("Sending back Dtxn.FragmentResponse for multi-partitioned InitiateTaskMessage message on txn #" + txn_id);    
                Dtxn.FragmentResponse response = Dtxn.FragmentResponse.newBuilder()
                                                     .setStatus(Status.OK)
                                                     .setOutput(ByteString.EMPTY)
                                                     .build();
                done.run(response);
                callback = new MultiPartitionTxnCallback(this, txn_id, partition, executor.getTransactionEstimator(), txn_info.client_callback);
            } else {
                txn_info.init_latch.countDown();
                callback = done;
            }
            txn_info.setCoordinatorCallback(callback);
            
            if (hstore_conf.enable_profiling) txn_info.queue_time.start();
            executor.doWork(msg, callback, txn_info);
            
            if (this.status_monitor != null) {
                assert(txn_info.getProcedure() != null) : "Null Procedure for txn #" + txn_id;
                TxnCounter.EXECUTED.inc(txn_info.getProcedure());
            }
        } else {
            if (t) LOG.trace("Executing fragment for already started txn #" + txn_id);
            executor.doWork(msg, done);
        }
        
    }

    /**
     * 
     * @param txn_id
     */
    public void misprediction(long txn_id, RpcCallback<byte[]> orig_callback) {
        LocalTransactionState orig_ts = this.inflight_txns.get(txn_id);
        // XXX assert(txn_info.orig_txn_id == null) : "Trying to restart a mispredicted transaction more than once!";
        int base_partition = orig_ts.getSourcePartition();
        StoredProcedureInvocation spi = orig_ts.invocation;
        assert(spi != null) : "Missing StoredProcedureInvocation for txn #" + txn_id;
        
        if (d) LOG.debug("Txn #" + txn_id + " was mispredicted! Going to clean-up our mess and re-execute");
        
        long new_txn_id = this.txnid_managers.get(base_partition).getNextUniqueTransactionId();
        this.restarted_txns.put(new_txn_id, txn_id);
        
        if (d) LOG.debug(String.format("Re-executing mispredicted txn #%d as new txn #%d", txn_id, new_txn_id));
        LocalTransactionState local_ts = null;
        try {
            ExecutionSite executor = this.executors.get(base_partition);
            local_ts = (LocalTransactionState)executor.localTxnPool.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for txn #" + txn_id);
            throw new RuntimeException(ex);
        }
        if (hstore_conf.enable_profiling) ProfileMeasurement.start(local_ts.total_time, local_ts.init_time);
        local_ts.init(new_txn_id, orig_ts);
        local_ts.setPredictSinglePartitioned(false);
        
        this.initializeInvocation(local_ts);
        
        if (this.status_monitor != null) TxnCounter.MISPREDICTED.inc(orig_ts.getProcedure());
    }
    
    /**
     * This will block until the the initialization latch is released by the InitiateCallback
     * @param txn_id
     */
    private void initializationBlock(LocalTransactionState txn_info) {
        CountDownLatch latch = txn_info.init_latch;
        if (latch.getCount() > 0) {
            final boolean d = debug.get();
            if (d) LOG.debug("Waiting for Dtxn.Coordinator to process our initialization response because Evan eats babies!!");
            if (hstore_conf.enable_profiling) txn_info.blocked_time.start();
            try {
                latch.await();
            } catch (Exception ex) {
                if (this.shutdown == false) LOG.fatal("Unexpected error when waiting for latch on txn #" + txn_info.getTransactionId(), ex);
                this.shutdown();
            }
            if (hstore_conf.enable_profiling) txn_info.blocked_time.stop();
            if (d) LOG.debug("Got the all clear message for txn #" + txn_info.getTransactionId());
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
        if (d) LOG.debug(String.format("Asking the Dtxn.Coordinator to execute fragment for txn #%d [bytes=%d, last=%s]", ts.getTransactionId(), fragment.getSerializedSize(), fragment.getLastFragment()));
        ts.rpc_request_work.reset();
        this.coordinator.execute(ts.rpc_request_work, fragment, callback);
    }

    /**
     * Request that the Dtxn.Coordinator finish out transaction
     * @param txn_id
     * @param request
     * @param callback
     */
    public void requestFinish(long txn_id, Dtxn.FinishRequest request, RpcCallback<Dtxn.FinishResponse> callback) {
        LocalTransactionState local_ts = this.inflight_txns.get(txn_id); 
        if (this.status_monitor != null && request.getCommit() == false) TxnCounter.ABORTED.inc(local_ts.getProcedure());
        this.initializationBlock(local_ts);
        if (local_ts.sysproc == false && hstore_conf.enable_profiling) ProfileMeasurement.stop(local_ts.finish_time, local_ts.total_time);
        if (d) LOG.debug(String.format("Telling the Dtxn.Coordinator to finish txn #%d [commit=%s]", txn_id, request.getCommit()));
        this.coordinator.finish(local_ts.rpc_request_finish, request, callback);
    }

    // ----------------------------------------------------------------------------
    // TRANSACTION FINISH/CLEANUP METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    @Override
    public void finish(RpcController controller, Dtxn.FinishRequest request, RpcCallback<Dtxn.FinishResponse> done) {
        // The payload will have our stored txn id. We can't use the FinishRequest's txn id because that is
        // going to be internal to Evan's stuff and is not guarenteed to be unique for this HStoreCoordinatorNode
        if (request.hasPayload() == false) {
            throw new RuntimeException("Got Dtxn.FinishRequest without a payload. Can't determine txn id!");
        }
        Long txn_id = HStoreSite.decodeTxnId(request.getPayload());
        assert(txn_id != null) : "Null txn id in Dtxn.FinishRequest payload";
        if (d) LOG.debug("Got Dtxn.FinishRequest for txn #" + txn_id + " [commit=" + request.getCommit() + "]");
        
        // We only need to call commit/abort if this wasn't a single-partition transaction
        LocalTransactionState local_ts = this.inflight_txns.get(txn_id);
        if (local_ts == null || local_ts.isPredictSinglePartition() == false) {
            // Tell our node to either commit or abort the txn in the FinishRequest
            // FIXME: Dtxn.FinishRequest needs to tell us what partition to tell to commit/abort
            boolean commit = request.getCommit(); 
            for (Entry<Integer, ExecutionSite> e : this.executors.entrySet()) {
                if (commit) {
                    e.getValue().commitWork(txn_id.longValue());
                } else {
                    e.getValue().abortWork(txn_id.longValue());
                }
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
     * Perform final cleanup and book keeping for a completed txn
     * @param txn_id
     */
    public void completeTransaction(long txn_id) {
        if (d) LOG.debug("Cleaning up internal info for Txn #" + txn_id);
        LocalTransactionState txn_info = this.inflight_txns.remove(txn_id);
        assert(txn_info != null) : "No LocalTransactionState for txn #" + txn_id;
        txn_info.setHStoreSiteDone(true);
        cache_encoded.remove(txn_id);
        if (this.status_monitor != null) TxnCounter.COMPLETED.inc(txn_info.getProcedure());
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
        List<Runnable> threads = new ArrayList<Runnable>();
        final Site catalog_site = hstore_site.getSite();
        final int num_partitions = catalog_site.getPartitions().size();
        final String site_host = catalog_site.getHost().getIpaddr();
        
        // ----------------------------------------------------------------------------
        // (1) ProtoServer Thread (one per site)
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Starting HStoreSite [site=%d]", hstore_site.getSiteId()));
        hstore_site.start();
        
        // ----------------------------------------------------------------------------
        // (1) ProtoServer Thread (one per site)
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Launching ProtoServer [site=%d, port=%d]", hstore_site.getSiteId(), catalog_site.getDtxn_port()));
        final NIOEventLoop execEventLoop = new NIOEventLoop();
        final CountDownLatch execLatch = new CountDownLatch(1);
        execEventLoop.setExitOnSigInt(true);
        threads.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(hstore_site.getThreadName("proto"));
                
                ProtoServer execServer = new ProtoServer(execEventLoop);
                execServer.register(hstore_site);
                execServer.bind(catalog_site.getDtxn_port());
                execLatch.countDown();
                
                boolean should_shutdown = false;
                Exception error = null;
                try {
                    execEventLoop.run();
                } catch (AssertionError ex) {
                    LOG.fatal("ProtoServer thread failed", ex);
                    error = new Exception(ex);
                    should_shutdown = true;
                } catch (Exception ex) {
                    LOG.fatal("ProtoServer thread failed", ex);
                    error = ex;
                    should_shutdown = true;
                }
                if (hstore_site.shutdown == false) {
                    LOG.warn(String.format("ProtoServer thread is stopping! [error=%s, should_shutdown=%s, hstore_shutdown=%s]",
                                           (error != null ? error.getMessage() : null), should_shutdown, hstore_site.shutdown));
                    if (should_shutdown) hstore_site.messenger.shutdownCluster(error);
                }
            };
        });
        
        // ----------------------------------------------------------------------------
        // (2) DTXN Engine Threads (one per partition)
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Launching DTXN Engines for %d partitions", num_partitions));
        for (final Partition catalog_part : catalog_site.getPartitions()) {
            // TODO: There should be a single thread that forks all the processes and then joins on them
            threads.add(new Runnable() {
                public void run() {
                    final Thread self = Thread.currentThread();
                    self.setName(hstore_site.getThreadName("eng", catalog_part.getId()));
                    
                    int partition = catalog_part.getId();
                    if (execLatch.getCount() > 0) {
                        if (d) LOG.debug("Waiting for ProtoServer to finish start up for Partition #" + partition);
                        try {
                            execLatch.await();
                        } catch (InterruptedException ex) {
                            LOG.error(ex);
                            return;
                        }
                    }
                    
                    int port = catalog_site.getDtxn_port();
                    if (d) LOG.debug("Forking off ProtoDtxnEngine for Partition #" + partition + " [outbound_port=" + port + "]");
                    String[] command = new String[]{
                        dtxnengine_path,                // protodtxnengine
                        site_host + ":" + port,         // host:port (ProtoServer)
                        hstore_conf_path,               // hstore.conf
                        Integer.toString(partition),    // partition #
                        "0"                             // ??
                    };
                    ThreadUtil.fork(command, hstore_site.shutdown_observable,
                                    String.format("[%s] ", hstore_site.getThreadName("protodtxnengine", partition)), true);
                    if (hstore_site.shutdown == false) { 
                        String msg = "ProtoDtxnEngine for Partition #" + partition + " is stopping!"; 
                        LOG.error(msg);
                        hstore_site.messenger.shutdownCluster(new Exception(msg));
                    }
                }
            });
        } // FOR (partition)
        
        // ----------------------------------------------------------------------------
        // (3) Procedure Request Listener Thread
        // ----------------------------------------------------------------------------
        threads.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(hstore_site.getThreadName("coord"));
                
                if (d) LOG.debug("Creating connection to coordinator at " + coordinatorHost + ":" + coordinatorPort + " [site=" + hstore_site.getSiteId() + "]");
                
                hstore_site.coordinatorEventLoop.setExitOnSigInt(true);
                InetSocketAddress[] addresses = {
                        new InetSocketAddress(coordinatorHost, coordinatorPort),
                };
                ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(hstore_site.coordinatorEventLoop, addresses);
                Dtxn.Coordinator stub = Dtxn.Coordinator.newStub(channels[0]);
                hstore_site.setDtxnCoordinator(stub);
                VoltProcedureListener voltListener = new VoltProcedureListener(hstore_site.coordinatorEventLoop, hstore_site);
                voltListener.bind(catalog_site.getProc_port());
                LOG.info(String.format("%s [site=%d, port=%d, #partitions=%d]",
                                       HStoreSite.SITE_READY_MSG,
                                       hstore_site.getSiteId(),
                                       catalog_site.getProc_port(),
                                       hstore_site.getExecutorCount()));
                hstore_site.ready();
                
                boolean should_shutdown = false;
                Exception error = null;
                try {
                    hstore_site.coordinatorEventLoop.run();
                } catch (AssertionError ex) {
                    LOG.fatal("Dtxn.Coordinator thread failed", ex);
                    error = new Exception(ex);
                    should_shutdown = true;
                } catch (Exception ex) {
                    if (hstore_site.shutdown == false &&
                        ex != null &&
                        ex.getMessage() != null &&
                        ex.getMessage().contains("Connection closed") == false
                    ) {
                        LOG.fatal("Dtxn.Coordinator thread stopped", ex);
                        error = ex;
                        should_shutdown = true;
                    }
                }
                if (hstore_site.shutdown == false) {
                    LOG.warn(String.format("Dtxn.Coordinator thread is stopping! [error=%s, should_shutdown=%s, hstore_shutdown=%s]",
                                           (error != null ? error.getMessage() : null), should_shutdown, hstore_site.shutdown));
                    if (should_shutdown) hstore_site.messenger.shutdownCluster(error);
                }
            };
        });
        
        // Blocks!
        ThreadUtil.runNewPool(threads);
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
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG,
                     ArgumentsParser.PARAM_COORDINATOR_HOST,
                     ArgumentsParser.PARAM_COORDINATOR_PORT,
                     ArgumentsParser.PARAM_NODE_SITE,
                     ArgumentsParser.PARAM_DTXN_CONF,
                     ArgumentsParser.PARAM_DTXN_ENGINE
        );
        HStoreConf.init(args);

        // HStoreNode Stuff
        final int site_id = args.getIntParam(ArgumentsParser.PARAM_NODE_SITE);
        Thread t = Thread.currentThread();
        t.setName(String.format("H%03d-main", site_id));

        // HStoreCoordinator Stuff
        final String coordinatorHost = args.getParam(ArgumentsParser.PARAM_COORDINATOR_HOST);
        final int coordinatorPort = args.getIntParam(ArgumentsParser.PARAM_COORDINATOR_PORT);

        // For every partition in our local site, we want to setup a new ExecutionSite
        // Thankfully I had enough sense to have PartitionEstimator take in the local partition
        // as a parameter, so we can share a single instance across all ExecutionSites
        Site catalog_site = CatalogUtil.getSiteFromId(args.catalog_db, site_id);
        if (catalog_site == null) {
            LOG.fatal("Invalid site #" + site_id);
            System.exit(1);
        }
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();

        // ----------------------------------------------------------------------------
        // MarkovGraphs
        // ----------------------------------------------------------------------------
        Map<Integer, MarkovGraphsContainer> markovs = null;
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            File path = new File(args.getParam(ArgumentsParser.PARAM_MARKOV));
            if (path.exists()) {
                markovs = MarkovUtil.loadIds(args.catalog_db, path.getAbsolutePath(), CatalogUtil.getLocalPartitionIds(catalog_site));
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
            
            // For each HStoreCoordinatorNode, we need to make sure that the trace ids start at our offset
            // This will allow us to merge multiple traces together for a benchmark single-run
            long start_id = (site_id + 1) * 100000l;
            AbstractTraceElement.setStartingId(start_id);
            
            LOG.info("Enabled workload logging '" + tracePath + "' with trace id offset " + start_id);
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
                assert(local_markovs != null) : "Failed to MarkovGraphsContainer that we need for partition #" + local_partition;
            }

            // Initialize TransactionEstimator stuff
            // Load the Markov models if we were given an input path and pass them to t_estimator
            // HACK: For now we have to create a TransactionEstimator for all partitions, since
            // it is written under the assumption that it was going to be running at just a single partition
            // I'm not proud of this...
            // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
            // stick them into the HStoreCoordinator
            LOG.debug("Creating Estimator for Site #" + site_id);
            TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, args.param_correlations, local_markovs);

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

        // Status Monitor
        if (args.hasParam(ArgumentsParser.PARAM_NODE_STATUS_INTERVAL)) {
            int interval = args.getIntParam(ArgumentsParser.PARAM_NODE_STATUS_INTERVAL);
            boolean kill_when_hanging = false;
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_STATUS_INTERVAL_KILL)) {
                kill_when_hanging = args.getBooleanParam(ArgumentsParser.PARAM_NODE_STATUS_INTERVAL_KILL);
            }
            if (interval > 0) {
                LOG.info(String.format("Enabling StatusMonitorThread [interval=%d, kill=%s]", interval, kill_when_hanging));
                site.enableStatusMonitor(interval, kill_when_hanging);
            }
        }
        
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
