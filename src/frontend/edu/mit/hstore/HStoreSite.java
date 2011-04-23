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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import edu.mit.hstore.callbacks.ForwardTxnRequestCallback;
import edu.mit.hstore.callbacks.InitiateCallback;
import edu.mit.hstore.callbacks.MultiPartitionTxnCallback;
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
    
    public static final long NULL_SPECULATIVE_EXEC_ID = -1;
    
    private static final double PRELOAD_SCALE_FACTOR = Double.valueOf(System.getProperty("hstore.preload", "1.0")); 
    public static double getPreloadScaleFactor() {
        return (PRELOAD_SCALE_FACTOR);
    }
    
    private static final Map<Long, ByteString> CACHE_ENCODED_TXNIDS = new ConcurrentHashMap<Long, ByteString>();
    public static ByteString encodeTxnId(long txn_id) {
        ByteString bs = CACHE_ENCODED_TXNIDS.get(txn_id);
        if (bs == null) {
            bs = ByteString.copyFrom(Long.toString(txn_id).getBytes());
            CACHE_ENCODED_TXNIDS.put(txn_id, bs);
        }
        return (bs);
    }
    public static long decodeTxnId(ByteString bs) {
        return (Long.valueOf(bs.toStringUtf8()));
    }

    // ----------------------------------------------------------------------------
    // TRANSACTION COUNTERS
    // ----------------------------------------------------------------------------
    
    public enum TxnCounter {
        /** The number of txns that we executed locally */
        EXECUTED,
        /** Fast single-partition execution */
        FAST,
        /** Speculative Execution **/
        SPECULATIVE,
        /** No undo buffers! Naked transactions! */
        NO_UNDO_BUFFER,
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
        /** The number of transaction requests that have arrived at this site */
        RECEIVED,
        /** Of the the received transactions, the number that we had to send somewhere else */
        REDIRECTED,
        ;
        
        private final Histogram<String> h = new Histogram<String>();
        private final String name;
        private TxnCounter() {
            this.name = StringUtil.title(this.name()).replace("_", "-");
        }
        @Override
        public String toString() {
            return (this.name);
        }
        public Histogram<String> getHistogram() {
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
                case SPECULATIVE:
                    total = FAST.get();
                    break;
                case NO_UNDO_BUFFER:
                    total = EXECUTED.get();
                    break;
                case FAST:
                    total = SINGLE_PARTITION.get();
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

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * This is the thing that we will actually use to generate txn ids used by our H-Store specific code
     */
    private final TransactionIdManager txnid_managers[];
    
    private final DBBPool buffer_pool = new DBBPool(true, false);
    private final Map<Integer, Thread> executor_threads = new HashMap<Integer, Thread>();
    private final HStoreMessenger messenger;

    /** ProtoServer EventLoop **/
    private final NIOEventLoop protoEventLoop = new NIOEventLoop();
    
    private VoltProcedureListener voltListener;
    private boolean throttle = false;
    
    // Dtxn Stuff
    private Dtxn.Coordinator coordinator;
    private final NIOEventLoop coordinatorEventLoop = new NIOEventLoop();
    
    /**
     * PartitionId -> Dtxn.ExecutionEngine
     */
    private final Dtxn.Partition engine_channels[];
    private final NIOEventLoop engineEventLoop = new NIOEventLoop();

    /**
     * PartitionId -> The TxnId that caused Speculative Execution to get enabled
     * When we get the response for these txns, we know we can commit/abort the ExecutionSite's queues
     */
    private final long speculative_txn[];
    
    /**
     * Sets of TransactionIds that have been sent to either the Dtxn.Coordinator
     * or the Dtxn.Engine. We can use the fast mode for each partition until this is empty
     * PartitionId -> Set<TransactionId> 
     */
    private final Set<Long> canadian_txns[]; 
    

    private CountDownLatch ready_latch;
    private boolean ready = false;
    private final EventObservable ready_observable = new EventObservable();
    private boolean shutdown = false;
    private final EventObservable shutdown_observable = new EventObservable();
    
    protected final Site catalog_site;
    protected final int site_id;
    protected final Map<Integer, ExecutionSite> executors;
    protected final Database catalog_db;
    protected final PartitionEstimator p_estimator;
    protected final AbstractHasher hasher;

    /** All of the partitions in the cluster */
    protected final List<Integer> all_partitions;
    
    /** List of local partitions at this HStoreSite */
    private final List<Integer> local_partitions = new ArrayList<Integer>();
    
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
     * Helper Thread Stuff
     */
    private final ScheduledExecutorService helper_pool;
    
    /**
     * NewOrder Hack
     * W_ID String -> W_ID Short
     */
    private final Map<String, Short> neworder_hack_w_id;
    /**
     * NewOrder Hack
     * W_ID Short -> PartitionId
     */
    private final Map<Short, Integer> neworder_hack_hashes;
    
    /**
     * Status Monitor
     */
    private Thread status_monitor;
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param coordinator
     * @param p_estimator
     */
    @SuppressWarnings("unchecked")
    public HStoreSite(Site catalog_site, Map<Integer, ExecutionSite> executors, PartitionEstimator p_estimator) {
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
        this.executors = Collections.unmodifiableMap(new HashMap<Integer, ExecutionSite>(executors));
        this.engine_channels = new Dtxn.Partition[num_partitions];
        this.txnid_managers = new TransactionIdManager[num_partitions];
        this.speculative_txn = new long[num_partitions];
        this.canadian_txns = (Set<Long>[])new Set<?>[num_partitions];
        for (int partition : this.executors.keySet()) {
            this.txnid_managers[partition] = new TransactionIdManager(partition);
            this.canadian_txns[partition] = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
            this.speculative_txn[partition] = NULL_SPECULATIVE_EXEC_ID;
            this.local_partitions.add(partition);
        } // FOR
        
        this.messenger = new HStoreMessenger(this);
        this.helper_pool = Executors.newScheduledThreadPool(1);
        
        // NewOrder Hack
        if (hstore_conf.force_neworder_hack) {
            this.neworder_hack_hashes = new HashMap<Short, Integer>();
            this.neworder_hack_w_id = new HashMap<String, Short>();
            for (Short w_id = 0; w_id < 256; w_id++) {
                this.neworder_hack_w_id.put(w_id.toString(), w_id);
                this.neworder_hack_hashes.put(w_id, hasher.hash(w_id.intValue()));
            } // FOR
        } else {
            this.neworder_hack_hashes = null;
            this.neworder_hack_w_id = null;
        }
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

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
    
    /**
     * Convenience method to dump out status of this HStoreSite
     * @return
     */
    public String statusSnapshot() {
        return new HStoreSiteStatus(this, 0, false).snapshot(true, false, false);
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
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION STUFF
    // ----------------------------------------------------------------------------

    /**
     * Initializes all the pieces that we need to start this HStore site up
     */
    public void init() {
        if (d) LOG.debug("Initializing HStoreSite...");

        // First we need to tell the HStoreMessenger to start-up and initialize its connections
        if (d) LOG.debug("Starting HStoreMessenger for Site #" + this.site_id);
        this.messenger.start();

        // Create all of our parameter manglers
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.param_manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (d) LOG.debug(String.format("Created ParameterManglers for %d procedures", this.param_manglers.size()));
        
        // Then we need to start all of the ExecutionSites in threads
        if (d) LOG.debug("Starting ExecutionSite threads for " + this.executors.size() + " partitions on Site #" + this.site_id);
        for (Entry<Integer, ExecutionSite> e : this.executors.entrySet()) {
            ExecutionSite executor = e.getValue();
            executor.setHStoreSite(this);
            executor.setHStoreMessenger(this.messenger);

            Thread t = new Thread(executor);
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY); // Probably does nothing...
            this.executor_threads.put(e.getKey(), t);
            t.start();
        } // FOR
        
        // Schedule the ExecutionSiteHelper
        // This must come after the ExecutionSites are initialized
        if (d) LOG.debug(String.format("Scheduling ExecutionSiteHelper to run every %.1f seconds", hstore_conf.helper_interval / 1000f));
        ExecutionSiteHelper helper = new ExecutionSiteHelper(this.executors.values(),
                                                             hstore_conf.helper_txn_per_round,
                                                             hstore_conf.helper_txn_expire,
                                                             hstore_conf.enable_profiling);
        this.helper_pool.scheduleAtFixedRate(helper, hstore_conf.helper_initial_delay,
                                                     hstore_conf.helper_interval,
                                                     TimeUnit.MILLISECONDS);
        
        // Start Monitor Thread
        if (this.status_monitor != null) {
            if (d) LOG.debug("Starting HStoreSiteStatus monitor thread");
            this.status_monitor.start(); 
        }
        
        if (d) LOG.debug("Preloading cached objects");
        try {
            // Load up everything the QueryPlanUtil
            QueryPlanUtil.preload(this.catalog_db);
            
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
    }
    
    /**
     * Mark this HStoreSite as ready for action!
     */
    private synchronized void ready() {
        if (this.ready) {
            LOG.warn("Already told that we were ready... Ignoring");
            return;
        }
        LOG.info(String.format("%s [site=%d, port=%d, #partitions=%d]",
                               HStoreSite.SITE_READY_MSG,
                               this.site_id,
                               this.catalog_site.getProc_port(),
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
     * Returns true if this HStoreSite is throttling transactions
     * @return
     */
    public boolean isThrottlingEnabled() {
        return (this.throttle);
    }
    
    // ----------------------------------------------------------------------------
    // HSTORESTITE SHUTDOWN STUFF
    // ----------------------------------------------------------------------------
    
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

    /**
     * Perform shutdown operations for this HStoreCoordinatorNode
     * This should only be called by HStoreMessenger 
     */
    public synchronized void shutdown() {
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
        
        // Stop the helper
        this.helper_pool.shutdown();
        
        // Tell all of our event loops to stop
        if (t) LOG.trace("Telling Dtxn.Coordinator event loop to exit");
        this.coordinatorEventLoop.exitLoop();
        if (t) LOG.trace("Telling Dtxn.Engine event loop to exit");
        this.engineEventLoop.exitLoop();
        
        if (d) LOG.debug("Completed shutdown process at Site #" + this.site_id);
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
        if (d) LOG.debug(String.format("Received new stored procedure invocation request for %s [handle=%d, bytes=%d]", catalog_proc.getName(), request.getClientHandle(), serializedRequest.length));
        
        // First figure out where this sucker needs to go
        Integer dest_partition = null;
        
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
                this.messenger.shutdownCluster(false, new Exception("Shutdown command received at site #" + this.site_id));
                return;
            }
        // DB2-style Transaction Redirection
        } else if (hstore_conf.enable_db2_redirects) {
            if (request.hasBasePartition()) {
                if (d) LOG.debug(String.format("Using embedded base partition from %s request", request.getProcName()));
                dest_partition = request.getBasePartition();    
            } else if (d) {
                LOG.debug(String.format("There is no embedded base partition for %s request", request.getProcName()));
            }
            
            
        // Otherwise we use the PartitionEstimator to figure out where this thing needs to go
        } else if (hstore_conf.force_localexecution == false) {
            if (d) LOG.debug(String.format("Using PartitionEstimator for %s request", request.getProcName()));
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
            if (t) LOG.trace(String.format("Selecting a random local partition to execute %s request [force_local=%s]", request.getProcName(), hstore_conf.force_localexecution));
            dest_partition = CollectionUtil.getRandomValue(this.local_partitions);
        }
        
        if (d) LOG.debug(String.format("%s Invocation [handle=%d, partition=%d]", request.getProcName(), request.getClientHandle(), dest_partition));
        
        // If the dest_partition isn't local, then we need to ship it off to the right location
        TransactionIdManager id_generator = this.txnid_managers[dest_partition.intValue()]; 
        if (id_generator == null) {
            if (d) LOG.debug(String.format("StoredProcedureInvocation request for %s needs to be forwarded to partition #%d", request.getProcName(), dest_partition));
            
            // Make a wrapper for the original callback so that when the result comes back frm the remote partition
            // we will just forward it back to the client. How sweet is that??
            ForwardTxnRequestCallback callback = new ForwardTxnRequestCallback(done);
            
            // Mark this request as having been redirected
            assert(request.hasBasePartition() == false) : "Trying to redirect " + request.getProcName() + " transaction more than once!";
            StoredProcedureInvocation.markRawBytesAsRedirected(dest_partition.intValue(), serializedRequest);
            
            this.messenger.forwardTransaction(serializedRequest, callback, dest_partition);
            if (this.status_monitor != null) TxnCounter.REDIRECTED.inc(catalog_proc);
            return;
        }
        
        // IMPORTANT: We have two txn ids here. We have the real one that we're going to use internally
        // and the fake one that we pass to Evan. We don't care about the fake one and will always ignore the
        // txn ids found in any Dtxn.Coordinator messages. 
        long txn_id = id_generator.getNextUniqueTransactionId();
                
        // Grab the TransactionEstimator for the destination partition and figure out whether
        // this mofo is likely to be single-partition or not. Anything that we can't estimate
        // will just have to be multi-partitioned. This includes sysprocs
        ExecutionSite executor = this.executors.get(dest_partition);
        TransactionEstimator t_estimator = executor.getTransactionEstimator();
        
        boolean single_partition = false;
        boolean predict_can_abort = true;
        boolean predict_readonly = false;
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
            single_partition = true;
            Short w_id = this.neworder_hack_w_id.get(args[0].toString());
            assert(w_id != null);
            Integer w_id_partition = this.neworder_hack_hashes.get(w_id);
            assert(w_id_partition != null);
            short inner[] = (short[])args[5];
            
            Set<Integer> done_partitions = local_ts.getDonePartitions();
            if (hstore_conf.force_neworder_hack_done) done_partitions.addAll(this.all_partitions);
            
            short last_w_id = w_id.shortValue();
            Integer last_partition = w_id_partition;
            if (hstore_conf.force_neworder_hack_done) done_partitions.remove(w_id_partition);
            for (short s_w_id : inner) {
                if (s_w_id != last_w_id) {
                    last_partition = this.neworder_hack_hashes.get(s_w_id);
                    last_w_id = s_w_id;
                }
                if (w_id_partition.equals(last_partition) == false) {
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
                if (d) LOG.debug(String.format("Txn #%d Parameters:\n%s", txn_id, this.param_manglers.get(catalog_proc).toString(cast_args)));
                
                if (hstore_conf.enable_profiling) local_ts.est_time.start();
                estimator_state = t_estimator.startTransaction(txn_id, dest_partition, catalog_proc, cast_args);
                
                // If there is no TransactinEstimator.State, then there is nothing we can do
                // It has to be executed as multi-partitioned
                if (estimator_state == null) {
                    if (d) LOG.debug(String.format("No TransactionEstimator.State was returned for txn #%d. Executing as multi-partitioned", txn_id)); 
                    single_partition = false;
                    
                // We have a TransactionEstimator.State, so let's see what it says...
                } else {
                    if (t) LOG.trace("\n" + StringUtil.box(estimator_state.toString()));
                    MarkovEstimate estimate = estimator_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a MarkovEstimate for some reason...
                    if (estimate == null) {
                        if (d) LOG.debug(String.format("No TransactionEstimator.Estimate was found for txn #%d. Executing as multi-partitioned", txn_id));
                        single_partition = false;
                        
                    // Check whether the probability that we're single-partitioned is above our threshold
                    } else {
                        if (d) LOG.debug(String.format("Using TransactionEstimator.Estimate for txn #%d to determine if single-partitioned", txn_id));
                        
                        assert(estimate.isValid()) : String.format("Invalid MarkovEstimate for txn #%d\n%s", txn_id, estimate);
                        if (d) LOG.debug("MarkovEstimate:\n" + estimate);
                        single_partition = estimate.isSinglePartition(this.thresholds);
                        predict_readonly = catalog_proc.getReadonly(); // FIXME
                        
                        // can_abort = estimate.isUserAbort(this.thresholds);
                        if (this.status_monitor != null && predict_can_abort == false) TxnCounter.NO_UNDO_BUFFER.inc(catalog_proc);
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
        if (d) LOG.debug(String.format("Executing %s txn #%d on partition %d [single-partitioned=%s]", catalog_proc.getName(), txn_id, dest_partition, single_partition));

        local_ts.init(txn_id, request.getClientHandle(), dest_partition.intValue(),
                      single_partition, predict_can_abort, estimator_state,
                      catalog_proc, request, done);
        local_ts.setPredictReadOnly(predict_readonly);
        if (hstore_conf.enable_profiling) {
            local_ts.total_time.start(timestamp);
            local_ts.init_time.start(timestamp);
        }
        this.initializeInvocation(local_ts);
        
        // Look at the number of inflight transactions and see whether we should block and wait for the 
        // queue to drain for a bit
        if (this.throttle == false && this.inflight_txns.size() > hstore_conf.txn_queue_max) {
            if (d) LOG.debug(String.format("HStoreSite is overloaded. Waiting for queue to drain [size=%d, trigger=%d]", this.inflight_txns.size(), hstore_conf.txn_queue_release));
            this.throttle = true;
            this.voltListener.setThrottleFlag(true);
        }
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
        int base_partition = txn_info.getBasePartition();
        boolean single_partitioned = txn_info.isPredictSinglePartition();
        boolean read_only = txn_info.isPredictReadOnly();
                
        LocalTransactionState dupe = this.inflight_txns.put(txn_id, txn_info);
        if (dupe != null) {
            // HACK!
            this.inflight_txns.put(txn_id, dupe);
            long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
            if (new_txn_id == txn_id) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("ORIG TRANSACTION:\n" + dupe);
                LOG.fatal("NEW TRANSACTION:\n" + txn_info);
                this.messenger.shutdownCluster(true, new Exception(msg));
            }
            LOG.warn(String.format("Had to fix duplicate txn id: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            txn_info.setTransactionId(txn_id);
            this.inflight_txns.put(txn_id, txn_info);
        }
        
        if (this.status_monitor != null) {
            if (txn_info.sysproc) {
                TxnCounter.SYSPROCS.inc(txn_info.getProcedure());
            } else {
                (single_partitioned ? TxnCounter.SINGLE_PARTITION : TxnCounter.MULTI_PARTITION).inc(txn_info.getProcedure());
            }
        }

        InitiateTaskMessage wrapper = new InitiateTaskMessage(txn_id, base_partition, base_partition, txn_info.invocation);
        Set<Long> dtxn_txns = this.canadian_txns[base_partition];
        assert(dtxn_txns != null) : "Missing multi-partition txn id set at partition " + base_partition;
        boolean spec_exec = (this.speculative_txn[base_partition] != NULL_SPECULATIVE_EXEC_ID);
        
        // -------------------------------
        // FAST MODE: Skip the Dtxn.Coordinator
        // -------------------------------
        if (hstore_conf.ignore_dtxn && single_partitioned && (dtxn_txns.isEmpty() || spec_exec || read_only)) {
            txn_info.ignore_dtxn = true;
            if (d) LOG.debug(String.format("Fast path execution for %s txn #%d on partition %d [speculative=%s, handle=%d]",
                                           txn_info.getProcedureName(), txn_id, base_partition, spec_exec, txn_info.getClientHandle()));
            RpcCallback<Dtxn.FragmentResponse> callback = new SinglePartitionTxnCallback(this, txn_id, base_partition, txn_info.client_callback);
            if (hstore_conf.enable_profiling) ProfileMeasurement.swap(txn_info.init_time, txn_info.coord_time);
            this.executeTransaction(txn_info, wrapper, callback);
            if (this.status_monitor != null) {
                TxnCounter.FAST.inc(txn_info.getProcedure());
                if (spec_exec) TxnCounter.SPECULATIVE.inc(txn_info.getProcedure());
            }

        // -------------------------------
        // CANADIAN MODE: Single-Partition
        // -------------------------------
        } else if (single_partitioned) {
            // Construct a message that goes directly to the Dtxn.Engine for the destination partition
            if (d) LOG.debug(String.format("Passing %s to Dtxn.Engine as single-partition txn #%d for partition %d [handle=%d]",
                                           txn_info.getProcedureName(), txn_id, base_partition, txn_info.getClientHandle()));

            Dtxn.DtxnPartitionFragment.Builder requestBuilder = Dtxn.DtxnPartitionFragment.newBuilder();
            requestBuilder.setTransactionId(txn_id);
            requestBuilder.setCommit(Dtxn.DtxnPartitionFragment.CommitState.LOCAL_COMMIT);
            requestBuilder.setPayload(HStoreSite.encodeTxnId(txn_id));
            requestBuilder.setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array()));
            
            RpcCallback<Dtxn.FragmentResponse> callback = new SinglePartitionTxnCallback(this, txn_id, base_partition, txn_info.client_callback);
            this.engine_channels[base_partition].execute(txn_info.rpc_request_init, requestBuilder.build(), callback);

            if (t) LOG.trace("Using SinglePartitionTxnCallback for txn #" + txn_id);
            
        // -------------------------------    
        // CANADIAN MODE: Multi-Partition
        // -------------------------------
        } else {
            // Construct the message for the Dtxn.Coordinator
            if (d) LOG.debug(String.format("Passing %s to Dtxn.Coordinator as multi-partition txn #%d for partition %d [handle=%d]",
                                           txn_info.getProcedureName(), txn_id, base_partition, txn_info.getClientHandle()));
            
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
            
            Set<Integer> done_partitions = txn_info.getDonePartitions();
            
            // TransactionEstimator
            // If we know we're single-partitioned, then we *don't* want to tell the Dtxn.Coordinator
            // that we're done at any partitions because it will throw an error
            // Instead, if we're not single-partitioned then that's that only time that 
            // we Tell the Dtxn.Coordinator that we are finished with partitions if we have an estimate
            TransactionEstimator.State estimator_state = txn_info.getEstimatorState(); 
            if (orig_txn_id == null && estimator_state != null && estimator_state.getInitialEstimate() != null) {
                // TODO: How do we want to come up with estimates per partition?
                Set<Integer> touched_partitions = estimator_state.getEstimatedPartitions();
                for (Integer p : this.all_partitions) {
                    // Make sure that we don't try to mark ourselves as being done at our own partition
                    if (touched_partitions.contains(p) == false && p.intValue() != base_partition) done_partitions.add(p);
                } // FOR
            }
            
            for (Integer p : done_partitions) {
                requestBuilder.addDonePartition(p.intValue());
            } // FOR
            assert(done_partitions.size() != this.all_partitions.size()) : "Trying to mark txn #" + txn_id + " as done at EVERY partition!";
            if (d && requestBuilder.getDonePartitionCount() > 0) {
                LOG.debug(String.format("Marked txn #%d as done at %d partitions: %s", txn_id, requestBuilder.getDonePartitionCount(), requestBuilder.getDonePartitionList()));
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
            if (t) LOG.trace("Using InitiateCallback for txn #" + txn_id);
            RpcCallback<Dtxn.CoordinatorResponse> callback = new InitiateCallback(this, txn_id, txn_info.init_latch);
            
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
        
        if (d) {
            LOG.debug(String.format("Got %s message for txn #%d [partition=%d]", msg.getClass().getSimpleName(), txn_id, partition));
            if (t) LOG.trace("CONTENTS:\n" + msg);
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
        
        Set<Long> multip_txns = this.canadian_txns[partition];
        assert(multip_txns != null) : "Missing multi-partition txn id set at partition " + partition;
        if (d && multip_txns.isEmpty()) LOG.debug(String.format("Enabling Dtxn.Coordinator CANADIAN mode [txn=#%d]", txn_id));
        multip_txns.add(txn_id);
        
        if (msg instanceof InitiateTaskMessage) {
            LocalTransactionState txn_info = this.inflight_txns.get(txn_id);
            assert(txn_info != null) : "Missing TransactionInfo for txn #" + txn_id;
            
            // Inject the StoredProcedureInvocation because we're not going to send it over the wire
            InitiateTaskMessage task = (InitiateTaskMessage)msg;
            assert(task.getStoredProcedureInvocation() == null);
            task.setStoredProcedureInvocation(txn_info.invocation);
            
            if (hstore_conf.enable_profiling) txn_info.coord_time.stop(timestamp);
            this.executeTransaction(txn_info, task, done);

        } else {
            if (t) LOG.trace("Executing remote fragment for txn #" + txn_id);
            
            this.executors.get(partition).doWork(msg, done);
        }
    }
    
    /**
     * 
     * @param executor
     * @param ts
     * @param task
     * @param done
     */
    public void executeTransaction(LocalTransactionState ts, InitiateTaskMessage task, RpcCallback<Dtxn.FragmentResponse> done) {
        long txn_id = ts.getTransactionId();
        int base_partition = ts.getBasePartition();
        boolean single_partitioned = ts.isPredictSinglePartition();
        assert(ts.client_callback != null) : "Missing original RpcCallback for txn #" + txn_id;
        RpcCallback<Dtxn.FragmentResponse> callback = null;
        
        ExecutionSite executor = this.executors.get(base_partition);
        assert(executor != null) : "No ExecutionSite exists for Partition #" + base_partition + " at this site???";
  
        // If we're single-partitioned, then we don't want to send back a callback now.
        // The response from the ExecutionSite should be the only response that we send back to the Dtxn.Coordinator
        // This limits the number of network roundtrips that we have to do...
        if (ts.ignore_dtxn == false && single_partitioned == false) { //  || (txn_info.sysproc == false && hstore_conf.ignore_dtxn == false)) {
            // We need to send back a response before we actually start executing to avoid a race condition    
            if (d) LOG.debug("Sending back Dtxn.FragmentResponse for InitiateTaskMessage message on txn #" + txn_id);    
            Dtxn.FragmentResponse response = Dtxn.FragmentResponse.newBuilder()
                                                 .setStatus(Status.OK)
                                                 .setOutput(ByteString.EMPTY)
                                                 .build();
            done.run(response);
            callback = new MultiPartitionTxnCallback(this, txn_id, base_partition, ts.client_callback);
        } else {
            ts.init_latch.countDown();
            callback = done;
        }
        ts.setCoordinatorCallback(callback);
        
        if (hstore_conf.enable_profiling) ts.queue_time.start();
        executor.doWork(task, callback, ts);
        
        if (this.status_monitor != null) {
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
    public void misprediction(long txn_id, RpcCallback<byte[]> orig_callback) {
        if (d) LOG.debug("Txn #" + txn_id + " was mispredicted! Going to clean-up our mess and re-execute");
        
        LocalTransactionState orig_ts = this.inflight_txns.get(txn_id);
        // XXX assert(txn_info.orig_txn_id == null) : "Trying to restart a mispredicted transaction more than once!";
        int base_partition = orig_ts.getBasePartition();
        StoredProcedureInvocation spi = orig_ts.invocation;
        assert(spi != null) : "Missing StoredProcedureInvocation for txn #" + txn_id;
        if (this.status_monitor != null) TxnCounter.MISPREDICTED.inc(orig_ts.getProcedure());
        
        final Histogram<Integer> touched = orig_ts.getTouchedPartitions();
        
        // Figure out whether this transaction should be redirected based on what partitions it
        // tried to touch before it was aborted 
        if (hstore_conf.enable_db2_redirects) {
            Set<Integer> most_touched = touched.getMaxCountValues();
            if (d) LOG.debug(String.format("Touched partitions for mispredicted txn #%d\n%s", txn_id, touched));
            Integer redirect_partition = null;
            if (most_touched.size() == 1) {
                redirect_partition = CollectionUtil.getFirst(most_touched);
            } else if (most_touched.isEmpty() == false) {
                redirect_partition = CollectionUtil.getRandomValue(most_touched);
            } else {
                redirect_partition = CollectionUtil.getRandomValue(this.all_partitions);
            }
            
            // If the txn wants to execute on another node, then we'll send them off *only* if this txn wasn't
            // already redirected at least once. If this txn was already redirected, then it's going to just
            // execute on the same partition, but this time as a multi-partition txn that locks all partitions.
            // That's what you get for messing up!!
            if (this.local_partitions.contains(redirect_partition) == false && spi.hasBasePartition() == false) {
                if (d) LOG.debug(String.format("Redirecting mispredicted %s txn #%d to partition %d", orig_ts.getProcedureName(), txn_id, redirect_partition));
                
                spi.setBasePartition(redirect_partition.intValue());
                
                // Add all the partitions that the txn touched before it got aborted
                spi.addPartitions(touched.values());
                
                byte serializedRequest[] = null;
                try {
                    serializedRequest = FastSerializer.serialize(spi);
                } catch (IOException ex) {
                    LOG.fatal("Failed to serialize StoredProcedureInvocation to redirect txn #" + txn_id);
                    this.messenger.shutdownCluster(false, ex);
                    return;
                }
                assert(serializedRequest != null);
                
                this.messenger.forwardTransaction(serializedRequest, new ForwardTxnRequestCallback(orig_callback), redirect_partition);
                if (this.status_monitor != null) TxnCounter.REDIRECTED.inc(orig_ts.getProcedure());
                return;
                
            // Allow local redirect
            } else if (spi.hasBasePartition() == false) {    
                base_partition = redirect_partition.intValue();
                spi.setBasePartition(base_partition);
            } else {
                if (d) LOG.debug(String.format("Mispredicted %s txn #%d has already been aborted once before. Restarting as all partition txn", orig_ts.getProcedureName(), txn_id));
                touched.putAll(this.local_partitions);
            }
        }

        long new_txn_id = this.txnid_managers[base_partition].getNextUniqueTransactionId();
        LocalTransactionState new_ts = null;
        try {
            ExecutionSite executor = this.executors.get(base_partition);
            new_ts = (LocalTransactionState)executor.localTxnPool.borrowObject();
        } catch (Exception ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for txn #" + txn_id);
            throw new RuntimeException(ex);
        }
        
        // Restart the new transaction
        if (hstore_conf.enable_profiling) ProfileMeasurement.start(new_ts.total_time, new_ts.init_time);
        boolean single_partitioned = (orig_ts.getOriginalTransactionId() == null && touched.getValueCount() == 1);
        
        if (d) LOG.debug(String.format("Re-executing mispredicted txn #%d as new %s-partition txn #%d on partition %d",
                                       txn_id, (single_partitioned ? "single" : "multi"), new_txn_id, base_partition));
        new_ts.init(new_txn_id, base_partition, orig_ts);
        new_ts.setPredictSinglePartitioned(single_partitioned);
        Set<Integer> new_done = new_ts.getDonePartitions();
        new_done.addAll(this.all_partitions);
        new_done.removeAll(touched.values());
        if (t) LOG.trace(String.format("Txn #%d Mispredicted partitions %s", txn_id, orig_ts.getTouchedPartitions()));
        
        this.initializeInvocation(new_ts);
    }
    
    /**
     * This will block until the the initialization latch is released by the InitiateCallback
     * @param txn_id
     */
    private void initializationBlock(LocalTransactionState ts) {
        CountDownLatch latch = ts.init_latch;
        if (latch.getCount() > 0) {
            final boolean d = debug.get();
            if (d) LOG.debug("Waiting for Dtxn.Coordinator to process our initialization response because Evan eats babies!!");
            if (hstore_conf.enable_profiling) ts.blocked_time.start();
            try {
                latch.await();
            } catch (Exception ex) {
                if (this.shutdown == false) LOG.fatal("Unexpected error when waiting for latch on txn #" + ts.getTransactionId(), ex);
                this.shutdown();
            }
            if (hstore_conf.enable_profiling) ts.blocked_time.stop();
            if (d) LOG.debug("Got the all clear message for txn #" + ts.getTransactionId());
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
        LocalTransactionState ts = this.inflight_txns.get(txn_id); 
        this.initializationBlock(ts);
        if (ts.sysproc == false && hstore_conf.enable_profiling) ProfileMeasurement.stop(ts.finish_time, ts.total_time);
        if (d) LOG.debug(String.format("Telling the Dtxn.Coordinator to finish txn #%d [commit=%s]", txn_id, request.getCommit()));
        this.coordinator.finish(ts.rpc_request_finish, request, callback);
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
        assert(request.hasPayload()) : "Got Dtxn.FinishRequest without a payload. Can't determine txn id!";
        Long txn_id = HStoreSite.decodeTxnId(request.getPayload());
        assert(txn_id != null) : "Null txn id in Dtxn.FinishRequest payload";
        boolean commit = request.getCommit();
        
        if (d) LOG.debug("Got Dtxn.FinishRequest for txn #" + txn_id + " [commit=" + commit + "]");
        
        // This will be null for non-local transactions
        LocalTransactionState ts = this.inflight_txns.get(txn_id);

        // We only need to call commit/abort if this wasn't a single-partition transaction
        boolean invoke_executor = (ts == null || ts.isPredictSinglePartition() == false); 
        // Tell each partition to either commit or abort the txn in the FinishRequest
        for (Integer p : this.local_partitions) {
            if (invoke_executor) {
                ExecutionSite executor = this.executors.get(p);
                if (commit) {
                    executor.commitWork(txn_id);
                } else {
                    executor.abortWork(txn_id);
                }
            
                // Check whether this is the response that the speculatively executed txns
                // have been waiting for
                if (hstore_conf.enable_speculative_execution && this.speculative_txn[p.intValue()] == txn_id.longValue()) {
                    if (d) LOG.debug(String.format("Turning off speculative execution mode at partition %d because txn #%d is finished", p, txn_id));
                    
                    // We can always commit our boys no matter what if we know that the txn was read-only 
                    // at the given partition
                    Boolean readonly = executor.isReadOnly(txn_id);
                    executor.drainQueueResponses(readonly != null && readonly == true ? true : commit);
                    this.speculative_txn[p.intValue()] = NULL_SPECULATIVE_EXEC_ID;
                }
            }
            this.removeCanadianTransaction(txn_id, p.intValue());
        } // FOR
        
        // Send back a FinishResponse to let them know we're cool with everything...
        if (done != null) {
            Dtxn.FinishResponse.Builder builder = Dtxn.FinishResponse.newBuilder();
            done.run(builder.build());
            if (t) LOG.trace("Sent back Dtxn.FinishResponse for txn #" + txn_id);
        } 
    }

    /**
     * Remove a transaction that was executed by the Canadian DTXN system
     * @param txn_id
     * @param p
     */
    private void removeCanadianTransaction(long txn_id, int p) {
        boolean removed = this.canadian_txns[p].remove(txn_id);
        if (d) {
            if (removed && t) LOG.trace(String.format("Removed txn #%d from the multi-partition txn set for Partition %d", txn_id, p));
            Set<Long> multip_txns = this.canadian_txns[p];
            if (removed && multip_txns.isEmpty()) LOG.debug(String.format("Disabling Dtxn.Coordinator CANADIAN mode [txn=#%d]", txn_id));
            else if (multip_txns.isEmpty() == false) LOG.debug(String.format("Partition %d In-Flight Dtxn.Coordinator Txns: %s", p, multip_txns.toString()));
        }
    }

    /**
     * Perform final cleanup and book keeping for a completed txn
     * @param txn_id
     */
    public void completeTransaction(final long txn_id, final Dtxn.FragmentResponse.Status status) {
        if (d) LOG.debug("Cleaning up internal info for Txn #" + txn_id);
        LocalTransactionState ts = this.inflight_txns.remove(txn_id);
        assert(ts != null) : "No LocalTransactionState for txn #" + txn_id;

        if (this.throttle && this.inflight_txns.size() < hstore_conf.txn_queue_release) {
            this.throttle = false;
            this.voltListener.setThrottleFlag(false);
            if (d) LOG.debug(String.format("Disabling throttling because txn #%d finished", txn_id));
        }
        
        int base_partition = ts.getBasePartition();
        boolean removed = this.canadian_txns[base_partition].remove(txn_id);
        if (d) {
            if (removed && t) LOG.trace(String.format("Removed txn #%d from the multi-partition txn set for Partition %d", txn_id, base_partition));
            Set<Long> dtxn_txns = this.canadian_txns[base_partition];
            if (removed && dtxn_txns.isEmpty()) LOG.debug(String.format("Disabling Dtxn.Coordinator CANADIAN mode [txn=#%d]", txn_id));
            else if (dtxn_txns.isEmpty() == false) LOG.debug(String.format("Partition %d In-Flight Dtxn.Coordinator Txns: %s", base_partition, dtxn_txns.toString()));
        }
        
        // Then clean-up any extra information that we may have for the txn
        if (ts.getEstimatorState() != null) {
            TransactionEstimator t_estimator = this.executors.get(base_partition).getTransactionEstimator();
            assert(t_estimator != null);
            switch (status) {
                case OK:
                    if (t) LOG.trace("Telling the TransactionEstimator to COMMIT txn #" + txn_id);
                    t_estimator.commit(txn_id);
                    break;
                case ABORT_MISPREDICT:
                    if (t) LOG.trace("Telling the TransactionEstimator to IGNORE txn #" + txn_id);
                    t_estimator.ignore(txn_id);
                    break;
                case ABORT_USER:
                case ABORT_DEADLOCK:
                    if (t) LOG.trace("Telling the TransactionEstimator to ABORT txn #" + txn_id);
                    t_estimator.abort(txn_id);
                    break;
                default:
                    assert(false) : String.format("Unexpected status %s for txn #%d", status, txn_id);
            } // SWITCH
        }
        
        ts.setHStoreSiteDone(true);
        CACHE_ENCODED_TXNIDS.remove(txn_id);
        if (this.status_monitor != null) {
            TxnCounter.COMPLETED.inc(ts.getProcedure());
            if (status != Dtxn.FragmentResponse.Status.OK) TxnCounter.ABORTED.inc(ts.getProcedure());
        }
    }
    
    /**
     * Notify this HStoreSite that the given transaction is done with the set of partitions
     * This will cause speculative execution to be enabled
     * @param txn_id
     * @param partitions
     */
    public void doneAtPartitions(long txn_id, Collection<Integer> partitions) {
        assert(hstore_conf.enable_speculative_execution);

        int spec_cnt = 0;
        for (Integer p : partitions) {
            if (this.txnid_managers[p.intValue()] == null) continue;
            
            // We'll let multiple tell us to speculatively execute, but we only let them go when hte latest
            // one finishes. We should really have multiple queues of speculatively execute txns, but for now
            // this is fine
            
//            assert(this.speculative_txn[p] == NULL_SPECULATIVE_EXEC_ID ||
//                   this.speculative_txn[p] == txn_id) : String.format("Trying to enable speculative execution twice at partition %d [current=#%d, new=#%d]", p, this.speculative_txn[p], txn_id); 
            ExecutionSite executor = this.executors.get(p); 
                
            // Make sure that we tell the ExecutionSite first before we allow txns to get fired off
            if (executor.enableSpeculativeExecution(txn_id)) {
                this.speculative_txn[p.intValue()] = txn_id;
                spec_cnt++;
                if (d) LOG.debug(String.format("Partition %d - Speculative Execution!", p));
            }
        } // FOR
        if (d) LOG.debug(String.format("Enabled speculative execution at %d partitions because of waiting for txn #%d", spec_cnt, txn_id));
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
                
                ProtoServer execServer = new ProtoServer(hstore_site.protoEventLoop);
                execServer.register(hstore_site);
                execServer.bind(catalog_site.getDtxn_port());
                execLatch.countDown();
                
                boolean should_shutdown = false;
                Exception error = null;
                try {
                    hstore_site.protoEventLoop.setExitOnSigInt(true);
                    hstore_site.ready_latch.countDown();
                    hstore_site.protoEventLoop.run();
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
        final CountDownLatch engineLatch = new CountDownLatch(hstore_site.local_partitions.size());
        for (final Partition catalog_part : catalog_site.getPartitions()) {
            // TODO: There should be a single thread that forks all the processes and then joins on them
            runnables.add(new Runnable() {
                public void run() {
                    final Thread self = Thread.currentThread();
                    self.setName(hstore_site.getThreadName("eng", catalog_part.getId()));
                    
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
                    if (hstore_site.shutdown == false) { 
                        String msg = "ProtoDtxnEngine for Partition #" + partition + " is stopping!"; 
                        LOG.error(msg);
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
                try {
                    channels = ProtoRpcChannel.connectParallel(hstore_site.engineEventLoop, destinations, 15000);
                } catch (RuntimeException ex) {
                    LOG.fatal("Failed to connect to local Dtxn.Engines", ex);
                    hstore_site.messenger.shutdownCluster(true, ex); // Blocking
                }
                assert(channels.length == destinations.length);
                for (int i = 0; i < channels.length; i++) {
                    int partition = hstore_site.local_partitions.get(i).intValue();
                    hstore_site.engine_channels[partition] = Dtxn.Partition.newStub(channels[i]);
                    if (d) LOG.debug("Creating direct Dtxn.Engine connection for partition " + partition);
                } // FOR
                if (d) LOG.debug("Established connections to all Dtxn.Engines");
                
                boolean should_shutdown = false;
                Exception error = null;
                try {
                    hstore_site.engineEventLoop.setExitOnSigInt(true);
                    hstore_site.ready_latch.countDown();
                    hstore_site.engineEventLoop.run();
                } catch (AssertionError ex) {
                    LOG.fatal("Engine EventLoop thread failed", ex);
                    error = new Exception(ex);
                    should_shutdown = true;
                } catch (Exception ex) {
                    if (hstore_site.shutdown == false &&
                            ex != null &&
                            ex.getMessage() != null &&
                            ex.getMessage().contains("Connection closed") == false
                        ) {
                        LOG.fatal("Engine EventLoop thread failed", ex);
                        error = ex;
                        should_shutdown = true;
                    }
                }
                if (hstore_site.shutdown == false) {
                    LOG.warn(String.format("Engine EventLoop thread is stopping! [error=%s, should_shutdown=%s, hstore_shutdown=%s]",
                                           (error != null ? error.getMessage() : null), should_shutdown, hstore_site.shutdown));
                    if (should_shutdown) hstore_site.messenger.shutdownCluster(error);
                }
            };
        });
        
        // ----------------------------------------------------------------------------
        // (4) Procedure Request Listener Thread (one per HStoreSite)
        // ----------------------------------------------------------------------------
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(hstore_site.getThreadName("coord"));
                
                if (d) LOG.debug(String.format("Creating connection to coordinator at %s:%d [site=%d]",
                                               coordinatorHost, coordinatorPort, hstore_site.getSiteId()));
                
                InetSocketAddress[] addresses = {
                        new InetSocketAddress(coordinatorHost, coordinatorPort),
                };
                ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(hstore_site.coordinatorEventLoop, addresses);
                Dtxn.Coordinator stub = Dtxn.Coordinator.newStub(channels[0]);
                hstore_site.setDtxnCoordinator(stub);
                hstore_site.voltListener = new VoltProcedureListener(hstore_site.coordinatorEventLoop, hstore_site);
                hstore_site.voltListener.bind(catalog_site.getProc_port());
                
                boolean should_shutdown = false;
                Exception error = null;
                try {
                    hstore_site.coordinatorEventLoop.setExitOnSigInt(true);
                    hstore_site.ready_latch.countDown();
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
        
        // ----------------------------------------------------------------------------
        // (5) HStoreSite Setup Thread
        // ----------------------------------------------------------------------------
        if (d) LOG.debug(String.format("Starting HStoreSite [site=%d]", hstore_site.getSiteId()));
        hstore_site.ready_latch = new CountDownLatch(runnables.size());
        runnables.add(new Runnable() {
            public void run() {
                final Thread self = Thread.currentThread();
                self.setName(hstore_site.getThreadName("setup"));
                
                // Always invoke HStoreSite.start() right away, since it doesn't depend on any
                // of the stuff being setup yet
                hstore_site.init();
                
                // But then wait for all of the threads to be finished with their initializations
                // before we tell the world that we're ready!
                if (d) LOG.info(String.format("Waiting for %d threads to complete initialization tasks", hstore_site.ready_latch.getCount()));
                try {
                    hstore_site.ready_latch.await();
                } catch (Exception ex) {
                    LOG.error("Unexpected interuption while waiting for engines to start", ex);
                    hstore_site.messenger.shutdownCluster(ex);
                }
                hstore_site.ready();
            }
        });
        
        // Blocks!
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
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG,
                     ArgumentsParser.PARAM_COORDINATOR_HOST,
                     ArgumentsParser.PARAM_COORDINATOR_PORT,
                     ArgumentsParser.PARAM_NODE_SITE,
                     ArgumentsParser.PARAM_DTXN_CONF,
                     ArgumentsParser.PARAM_DTXN_ENGINE
        );
        HStoreConf.init(args);
        if (d) LOG.debug("HStoreConf Parameters:\n" + HStoreConf.singleton().toString());

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
                MarkovUtil.setHasher(markovs, p_estimator.getHasher());
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
